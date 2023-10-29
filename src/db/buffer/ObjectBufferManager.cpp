//
// Created by Zhihan Guo on 4/3/23.
//

#include "ObjectBufferManager.h"
#include "local/ITable.h"
#include "db/ARDB.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"

namespace arboretum {

ObjectBufferManager::ObjectBufferManager(ARDB *db) : db_(db) {
  num_slots_ = g_total_buf_sz * 1.0 / g_buf_entry_sz; // an estimate for now
  // evict_end_threshold_ is used for AllocTuple caused eviction only
  // on-demand eviction only evict till need
  LOG_INFO("Initiated Object Buffer with %zu slots", num_slots_);
}

ITuple *ObjectBufferManager::AccessTuple(SharedPtr &ptr, uint64_t expected_key) {
  // called after lock permission is granted.
  auto tuple = reinterpret_cast<ITuple *>(ptr.Get());
  if (!tuple) {
    LOG_ERROR("Error accessing tuple: invalid addr");
  }
  if (tuple->GetTID() != expected_key) {
    LOG_ERROR("Error accessing tuple: key (%u) does not match expectation (%lu)",
              tuple->GetTID(), expected_key);
  }
  tuple->buf_latch_.lock();
  Pin(tuple);
  tuple->buf_latch_.unlock();
  return tuple;
}

void ObjectBufferManager::FinishAccessTuple(SharedPtr &ptr, bool dirty) {
  auto tuple = reinterpret_cast<ITuple *>(ptr.Get());
  FinishAccessTuple(tuple, dirty);
  ptr.Free();
}

void ObjectBufferManager::FinishAccessTuple(ITuple * tuple, bool dirty) {
  assert(tuple);
  tuple->buf_latch_.lock();
  UnPin(tuple);
  if (dirty) {
    tuple->buf_state_ = SetFlag(tuple->buf_state_, BM_DIRTY);
    // giving writes more weights
    if (GetUsageCount(tuple->buf_state_) < BM_MAX_USAGE_COUNT)
      tuple->buf_state_ += BUF_USAGECOUNT_ONE;
  }
  tuple->buf_latch_.unlock();
}

ITuple * ObjectBufferManager::AllocTuple(size_t tuple_sz, OID tbl_id, OID tid) {
  auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(tbl_id, tid);
  auto buf_sz = allocated_.fetch_add(1) + other_allocated_ / g_buf_entry_sz;
  if (buf_sz > num_slots_) {
    SearchKey dummy;
    Pin(tuple);
    EvictAndLoad(dummy, tuple, false);
  } else {
    latch_.lock();
    Pin(tuple);
    ClockInsert(evict_hand_, evict_tail_, tuple);
    latch_.unlock();
  }
  return tuple;
}

ITuple **ObjectBufferManager::LoadRangeFromStorage(OID tbl_id,
                                                   SearchKey low_key,
                                                   SearchKey high_key) {
  auto cnt = high_key.ToUInt64() - low_key.ToUInt64() + 1;
  auto table = db_->GetTable(tbl_id);
  auto tuple_sz = table->GetTotalTupleSize();
  auto tuples = (ITuple **) MemoryAllocator::Alloc(sizeof(void *) * cnt);
  char ** data_ptrs = (char **) MemoryAllocator::Alloc(sizeof(void *) * cnt);
  auto buf_sz = allocated_.fetch_add(cnt) + other_allocated_ / g_buf_entry_sz;
  volatile bool is_done = true;
  if (buf_sz > num_slots_) {
    // evict a batch.
    BatchEvict(buf_sz - num_slots_, is_done);
  }
  // batch load data
  for (int i = 0; i < cnt; i++) {
    tuples[i] = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(
        tbl_id, low_key.ToUInt64() + i);
    data_ptrs[i] = tuples[i]->GetData();
    SetUsageCount(tuples[i], 2);
  }
  g_data_store->ReadRange(table->GetStorageId(), low_key, high_key, data_ptrs);
  // insert data into the buffer ring
  latch_.lock();
  for (int i = 0; i < cnt; i++) {
    ClockInsert(evict_hand_, evict_tail_, tuples[i]);
  }
  latch_.unlock();
  // block until all writes are flushed. (not necessary but limit rate)
  while(!is_done) {};
  return tuples;
}


ITuple *ObjectBufferManager::LoadFromStorage(OID tbl_id, SearchKey key) {
  // will delete tuple from index if evicted.
  auto table = db_->GetTable(tbl_id);
  auto tuple_sz = table->GetTotalTupleSize();
  auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(tbl_id, key.ToUInt64());
  auto buf_sz = allocated_.fetch_add(1) + other_allocated_ / g_buf_entry_sz;
  if (buf_sz > num_slots_) {
    EvictAndLoad(key, tuple, true);
  } else {
    std::string data;
    g_data_store->Read(table->GetStorageId(), key, data);
    // don't copy everything, only copy things you need
    tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
    // no need to hold individual latch since no one can access it now
    SetUsageCount(tuple, 2);
    // add to buffer
    latch_.lock();
    ClockInsert(evict_hand_, evict_tail_, tuple);
    latch_.unlock();
    // LOG_DEBUG("Load tuple %s from storage", storage_key.c_str());
  }
  SearchKey pkey;
  tuple->GetPrimaryKey(table->GetSchema(), pkey);
  M_ASSERT(pkey == key, "tid does not match");
  return tuple;
}

void ObjectBufferManager::BatchEvict(size_t cnt, volatile bool &is_done) {
  // partition to map
  size_t flushed = 0;
  std::unordered_map<uint64_t, std::multimap<std::string, std::string>> flush_set;
  latch_.lock();
  auto start = evict_hand_;
  auto rounds = 0;
  std::string tbl_name;
  while (true) {
    if (!evict_hand_ || allocated_ == 0) {
      LOG_ERROR("Run out of buffer, increase buffer size!");
    }
    auto table = db_->GetTable(evict_hand_->GetTableId());
    // if latched, skip; otherwise, try write lock.
    if (!IsPinned(evict_hand_->buf_state_) && evict_hand_->buf_latch_.try_lock()) {
      // already held tuple latch.
      if (GetUsageCount(evict_hand_->buf_state_) != 0) {
        evict_hand_->buf_state_ -= BUF_USAGECOUNT_ONE;
        evict_hand_->buf_latch_.unlock();
        ClockAdvance(evict_hand_, evict_tail_);
        continue;
      }
      // try to evict the tuple
      // write to storage if dirty and load expected in the same roundtrip
      tbl_name = table->GetStorageId();
      auto flush_tuple = evict_hand_;
      if (CheckFlag(flush_tuple->buf_state_, BM_DIRTY)) {
        SearchKey flush_key;
        flush_tuple->GetPrimaryKey(table->GetSchema(), flush_key);
        std::string flush_data(flush_tuple->GetData(), table->GetTupleDataSize());
        flush_set[flush_key.ToUInt64() / g_partition_sz].emplace(
            flush_key.ToString(), flush_data);
      }
      flush_tuple->buf_latch_.unlock();
      // remove evict_hand_ from buffer
      ClockRemoveHand(evict_hand_, evict_tail_, allocated_);
      if (++flushed == cnt) break;
    }
    else {
      ClockAdvance(evict_hand_, evict_tail_);
      if (evict_hand_ == start) {
        rounds++;
        if (rounds >= BM_MAX_USAGE_COUNT) {
          LOG_ERROR("All pinned! Make sure # buffer slots > # threads * # reqs per txn");
        }
      }
    }
  }
  latch_.unlock();
  if (flushed != 0) {
    int64_t num_parts = flush_set.size();
    for (auto &pair : flush_set) {
      if (--num_parts == 0) {
        g_data_store->WriteBatchAsync(tbl_name, std::to_string(pair.first),
                                      pair.second, is_done);
      } else {
        g_data_store->WriteBatch(tbl_name, std::to_string(pair.first), pair.second);
      }
    }
  }
}

void ObjectBufferManager::EvictAndLoad(SearchKey &load_key,
                                       ITuple *tuple, bool load) {
  latch_.lock();
  auto start = evict_hand_;
  auto rounds = 0;
  while(true) {
    if (!evict_hand_ || allocated_ == 0) {
      LOG_ERROR("Run out of buffer, increase buffer size!");
    }
    auto table = db_->GetTable(evict_hand_->GetTableId());
    // if latched, skip; otherwise, try write lock.
    // alloc tuple (1) -> latch -> pin -> insert -> latch (2) evict: insert. release latch
    // latch
    if (!IsPinned(evict_hand_->buf_state_) && evict_hand_->buf_latch_.try_lock()) {
      // already held tuple latch.
      if (GetUsageCount(evict_hand_->buf_state_) != 0) {
        evict_hand_->buf_state_ -= BUF_USAGECOUNT_ONE;
        evict_hand_->buf_latch_.unlock();
        ClockAdvance(evict_hand_, evict_tail_);
        continue;
      }
      // try to evict the tuple
      // write to storage if dirty and load expected in the same roundtrip
      auto tbl_name = table->GetStorageId();
      bool flush = CheckFlag(evict_hand_->buf_state_, BM_DIRTY);
      auto flush_tuple = evict_hand_;
      auto flush_size = table->GetTupleDataSize();
      SearchKey flush_key;
      flush_tuple->GetPrimaryKey(table->GetSchema(), flush_key);
      if (flush_key.ToUInt64() != flush_tuple->GetTID()) {
        LOG_ERROR("Invalid tuple to evict (tid = %u, pkey in data = %lu)",
                  flush_tuple->GetTID(), flush_key.ToUInt64());
      }
      // add tuple to buffer and remove evict_hand_ from buffer
      ClockRemoveHand(evict_hand_, evict_tail_, allocated_);
      SetUsageCount(tuple, 2);
      ClockInsert(evict_hand_, evict_tail_, tuple);
      latch_.unlock();
      std::string data;
      if (flush) {
        // need to flush, then flush and load
        if (load) {
          g_data_store->WriteAndRead(tbl_name, flush_key,
                                     flush_tuple->GetData(), flush_size,
                                     load_key, data);
          tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
        } else {
          g_data_store->Write(tbl_name, flush_key, flush_tuple->GetData(), flush_size);
        }
        flush_tuple->buf_latch_.unlock();
        // LOG_DEBUG("Thread-%u Flushed tuple %lu and load tuple %lu", Worker::GetThdId(), flush_key.ToUInt64(), load_key.ToUInt64());
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX))
          table->IndexDelete(flush_tuple); // will unlock and free
      }
      else if (load) {
        // no need to flush, just load
        flush_tuple->buf_latch_.unlock();
        g_data_store->Read(tbl_name, load_key, data);
        // TODO: think about when to pin the tuple and
        //  why loading from storage does not pin but allicating pins the page
        tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
        SearchKey pkey;
        tuple->GetPrimaryKey(table->GetSchema(), pkey);
        // LOG_DEBUG("Thread-%u Load tuple %lu", Worker::GetThdId(), load_key.ToUInt64());
      }
      else {
        flush_tuple->buf_latch_.unlock();
        // LOG_DEBUG("Evicted tuple %lu", flush_key.ToUInt64());
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX))
          table->IndexDelete(flush_tuple); // will unlock and free
      }
      return;
    } else {
       ClockAdvance(evict_hand_, evict_tail_);
       if (evict_hand_ == start) {
         rounds++;
         if (rounds >= BM_MAX_USAGE_COUNT) {
           LOG_ERROR("All pinned! Make sure # buffer slots > # threads * # reqs per txn");
         }
       }
    }
  }
}

void ObjectBufferManager::SetUsageCount(ITuple *row, size_t cnt) {
  // cleaned buf_state_
  auto usage_cnt = GetUsageCount(row->buf_state_);
  row->buf_state_ += (usage_cnt + cnt > BM_MAX_USAGE_COUNT) ?
                     (BM_MAX_USAGE_COUNT - usage_cnt) * BUF_USAGECOUNT_ONE :
                     BUF_USAGECOUNT_ONE * cnt;
}

void ObjectBufferManager::Pin(ITuple *row) {
  if (GetPinCount(row->buf_state_) >= g_num_worker_threads * 10) {
    LOG_ERROR("pin count overflow!");
  }
  row->buf_state_ += BUF_REFCOUNT_ONE;
  // update usage count (weight)
  if (GetUsageCount(row->buf_state_) < BM_MAX_USAGE_COUNT) {
    row->buf_state_ += BUF_USAGECOUNT_ONE;
  }
}

void ObjectBufferManager::UnPin(ITuple *row) {
  row->buf_state_ -= BUF_REFCOUNT_ONE;
}

} // arboretum