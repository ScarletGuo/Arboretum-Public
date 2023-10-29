//
// Created by Zhihan Guo on 3/30/23.
//

#include "ITable.h"
#include "ITuple.h"
#include "db/access/IndexBTreeObject.h"
#include "db/access/IndexBTreePage.h"
#include "db/buffer/ObjectBufferManager.h"
#include "db/buffer/PageBufferManager.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"

namespace arboretum {

ITable::ITable(std::string tbl_name, OID tbl_id, ISchema *schema)  :
    tbl_name_(std::move(tbl_name)), tbl_id_(tbl_id), schema_(schema) {
  storage_id_ = g_dataset_id;
  storage_id_ += "-" + std::to_string(tbl_id);
  if (!g_restore_from_remote) {
    g_data_store->CreateTable(storage_id_);
  }
};

void ITable::InitInsertTuple(char *data, size_t sz) {
  if (g_buf_type == NOBUF) {
    auto tid = row_cnt_.fetch_add(1);
    auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    if (g_load_to_remote_only) {
      auto pkey = tuple->GetPrimaryKey(schema_);
      g_data_store->Write(GetStorageId(), std::to_string(pkey.ToUInt64() /
      g_partition_sz), pkey.ToString(), data, sz);
    }
  } else if (g_buf_type == OBJBUF) {
    auto tid = row_cnt_.fetch_add(1);
    auto tuple = ((ObjectBufferManager *) g_buf_mgr)->AllocTuple(
        GetTotalTupleSize(), tbl_id_, tid);
    tuple->SetData(data, sz);
    InitIndexInsert(tuple, 0, 0);
    // need to unpin
    arboretum::ObjectBufferManager::FinishAccessTuple(tuple, !g_restore_from_remote);
  } else {
    auto tid = row_cnt_.fetch_add(1);
    // keep inserting in the latest page without holding latches.
    // if full, allocate a new page and finish accessing this one
    // once all done. send a message to close last one.
    auto buf_mgr = (PageBufferManager *) g_buf_mgr;
    if (!pg_cursor_ || pg_cursor_->MaxSizeOfNextItem() < GetTotalTupleSize()) {
      // alloc new page
      if (pg_cursor_) {
        buf_mgr->FinishAccessPage(pg_cursor_, true);
      }
      auto pid = pg_cnt_.fetch_add(1);
      pg_cursor_ = buf_mgr->AllocNewPage(tbl_id_, pid);
    }
    // insert into the page and indexes
    auto addr = pg_cursor_->AllocDataSpace(GetTotalTupleSize());
    auto tuple = new (addr) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    InitIndexInsert(tuple, pg_cursor_->GetPageID(),
                pg_cursor_->GetMaxOffsetNumber());
  }
  // TODO: not thread safe. for future insert request after loading, need to
  //  protect it with latches
  // insert into lock table
  lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
}

// IndexBTreePage
void ITable::InitIndexInsert(ITuple * p_tuple, OID pg_id, OID offset) {
  if (g_index_type == IndexType::REMOTE) {
    // not used
    auto key = p_tuple->GetPrimaryKey(schema_);
    g_data_store->Write(GetStorageId(), key, p_tuple->GetData(),
                        GetTupleDataSize());
  } else if (g_buf_type == BufferType::OBJBUF) {
    RC rc = RC::OK;
    for (auto & pairs : indexes_) {
      // generate search key for corresponding index
      // does not support composite key yet
      SearchKey idx_key;
      auto & index = pairs.second;
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      ((IndexBTreeObject *) index)->Insert(idx_key, p_tuple, INSERT, nullptr);
    }
  } else if (g_buf_type == BufferType::PGBUF) {
    // insert into index
    for (auto &pairs: indexes_) {
      // generate search key for corresponding index
      // NOTE: does not support composite key yet
      SearchKey idx_key;
      auto &index = pairs.second;
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      ((IndexBTreePage *) index)->Insert(idx_key, pg_id, offset);
    }
  }
}

RC ITable::InsertTuple(SearchKey& pkey, char *data, size_t sz, ITxn *txn) {
  RC rc;
  if (g_buf_type == OBJBUF) {
    // update row_cnt
    row_cnt_.fetch_add(1);
    auto tuple = ((ObjectBufferManager *) g_buf_mgr)->AllocTuple(
        GetTotalTupleSize(), tbl_id_, pkey.ToUInt64());
    tuple->SetData(data, sz);
    // insert into every index
    for (auto & pairs : indexes_) {
      auto & index = pairs.second;
      // generate search key for corresponding index; composite key not supported yet
      SearchKey idx_key;
      tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      rc = ((IndexBTreeObject *) index)->Insert(idx_key, tuple, INSERT, txn);
      if (rc == ABORT) {
        tuple->buf_state_ = ObjectBufferManager::SetFlag(tuple->buf_state_, BM_NOT_IN_INDEX);
        break;
      }
      // LOG_DEBUG("inserted key %lu", idx_key.ToUInt64());
    }
    // need to unpin
    arboretum::ObjectBufferManager::FinishAccessTuple(tuple, rc != ABORT);
  } else {
    LOG_ERROR("not supported yet");
  }
  return rc;
}

SharedPtr *
ITable::IndexRangeSearch(SearchKey low_key, SearchKey high_key, OID idx_id,
                         ITxn *txn, RC &rc, size_t &cnt) {
  // LOG_DEBUG("start range search %lu - %lu", low_key.ToUInt64(), high_key.ToUInt64());
  std::vector<std::pair<SearchKey, SearchKey>> fetch_req;
  SharedPtr * tags;
  if (g_buf_type == OBJBUF) {
    auto index = (IndexBTreeObject *) indexes_[idx_id];
    tags = index->RangeSearch(low_key, high_key, txn, fetch_req, rc, cnt);
    if (rc == ABORT) return nullptr;
    // fetch missing tuples.
    for (auto &req: fetch_req) {
      // fetch range (req.first, req.second). exclusive.
      auto low = SearchKey(req.first.ToUInt64() + 1);
      auto high = SearchKey(req.second.ToUInt64() - 1);
      auto tuples = ((ObjectBufferManager *) g_buf_mgr)->LoadRangeFromStorage(
          GetTableId(), low, high);
      auto range_sz = high.ToUInt64() - low.ToUInt64() + 1;
      // LOG_DEBUG("remote fetch req: %lu - %lu size = %lu", low.ToUInt64(), high.ToUInt64(), range_sz);
      // it will never abort since read lock is acquired during index->Search.
      for (size_t i = 0; i < range_sz; i++) {
        auto tuple = tuples[i];
        cnt++;
        auto key = SearchKey(i + low.ToUInt64());
        auto tag = &tags[key.ToUInt64() - low_key.ToUInt64()];
        // LOG_DEBUG("add remote tuple %lu to range search results", key.ToUInt64());
        rc = index->Insert(key, tuple, AccessType::READ, nullptr, tag);
        if (i < range_sz - 1) tuple->next_key_in_storage_ = false;
        // if already exist, mark tuple as no need to delete index.
        if (tag->Get() != tuple)
          tuple->buf_state_ = ObjectBufferManager::SetFlag(tuple->buf_state_, BM_NOT_IN_INDEX);
        if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
      }
      DEALLOC(tuples);
    }
    auto expected_cnt = high_key.ToUInt64() - low_key.ToUInt64() + 1;
    M_ASSERT(cnt == expected_cnt,
             "return results (cnt = %lu) from range scan does not match expectation (%lu).",
             cnt, expected_cnt);
    if (g_warmup_finished) {
      g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(cnt);
      // how many txns have more than 1 scan request
      g_stats->db_stats_[Worker::GetThdId()].incr_txn_scans_(fetch_req.size());
      if (fetch_req.size() > 1)
        g_stats->db_stats_[Worker::GetThdId()].incr_more_than_one_scan_txns_(1);
    }
  } else if (g_index_type == IndexType::REMOTE) {
    cnt = high_key.ToUInt64() - low_key.ToUInt64() + 1;
    tags = NEW_SZ(SharedPtr, cnt);
    auto addrs = (char *) MemoryAllocator::Alloc(GetTotalTupleSize() * cnt);
    char ** data_ptrs = NEW_SZ(char *, cnt);
    auto next = sizeof(ITuple);
    for (int i = 0; i < cnt; i++) {
      data_ptrs[i] = &addrs[next];
      next += GetTotalTupleSize();
    }
    g_data_store->ReadRange(GetStorageId(), low_key, high_key, data_ptrs);
    next = 0;
    for (int i = 0; i < cnt; i++) {
      auto tuple = new (&addrs[next]) ITuple(tbl_id_, low_key.ToUInt64() + i);
      next += GetTotalTupleSize();
      SearchKey key;
      // no need to set from loaded since memcpy is done in azure client.
      // tuple->SetFromLoaded(const_cast<char *>(data.c_str()), GetTupleDataSize());
      tuple->GetPrimaryKey(schema_, key);
      if (low_key.ToUInt64() + i != key.ToUInt64())
        LOG_ERROR("Loaded data does not match!");
      tags[i].Init(tuple);
    }
    DEALLOC(data_ptrs);
    if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(cnt);
  } else if (g_buf_type == PGBUF) {
    auto index = (IndexBTreePage *) indexes_[idx_id];
    tags = index->RangeSearch(low_key, high_key, txn, rc, cnt);
    if (rc == ABORT) return nullptr;
  }
  return tags;
}

SharedPtr *
ITable::IndexSearch(SearchKey key, OID idx_id, ITxn *txn, RC &rc) {
  if (g_index_type == IndexType::REMOTE) {
    rc = RC::OK;
    auto tuple_shared_ptr = NEW(SharedPtr);
    std::string data;
    g_data_store->Read(GetStorageId(), key, data);
    auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, key.ToUInt64());
    tuple->SetFromLoaded(const_cast<char *>(data.c_str()), GetTupleDataSize());
    SearchKey pkey;
    tuple->GetPrimaryKey(schema_, pkey);
    M_ASSERT(pkey == key, "Loaded data does not match!");
    tuple_shared_ptr->Init(tuple);
    return tuple_shared_ptr;
  } else if (g_buf_type == OBJBUF) {
    auto index = (IndexBTreeObject *) indexes_[idx_id];
    auto tag = index->Search(key, txn, rc);
    if (rc == RC::ERROR) {
      // TODO: currently assume data is always in storage if not found locally
      //   once supporting true delete, need to check next_key_in_storage
      // cache miss! need to load from storage
      auto tuple = ((ObjectBufferManager *)g_buf_mgr)->LoadFromStorage(tbl_id_, key);
      // it will never abort since read lock is acquired during index->Search.
      tag = NEW(SharedPtr);
      rc = index->Insert(key, tuple, AccessType::READ, nullptr, tag);
      // if already exist, mark tuple as no need to delete index.
      if (tag->Get() != tuple)
        tuple->buf_state_ = ObjectBufferManager::SetFlag(tuple->buf_state_, BM_NOT_IN_INDEX);
      if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
    }
    if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    return tag;
  } else if (g_buf_type == PGBUF) {
    auto index = (IndexBTreePage *) indexes_[idx_id];
    // TODO: currently assume data is always in storage if not found locally
    auto tags = index->Search(key);
    return tags;
  }
  return nullptr;
}

void ITable::IndexDelete(ITuple * p_tuple) {
  // delete from all indexes
  if (g_index_type == IndexType::REMOTE) {
    LOG_ERROR("should not happen");
  } else if (g_index_type == IndexType::BTREE) {
    for (auto & pairs : indexes_) {
      auto & index = pairs.second;
      // generate search key for corresponding index
      SearchKey idx_key;
      // TODO: does not support composite key yet
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      if (g_buf_type == OBJBUF)
        ((IndexBTreeObject *) index)->Delete(idx_key, p_tuple);
      else LOG_ERROR("Not impl yet.");
    }
  }
}

void ITable::FinishLoadingData() {
  if (g_buf_type == PGBUF) {
    LOG_DEBUG("Finish loading %lu data pages", pg_cnt_.load());
    auto buf_mgr = (PageBufferManager *) g_buf_mgr;
    buf_mgr->FinishAccessPage(pg_cursor_, true);
    for (auto &pairs: indexes_) {
      auto &index = pairs.second;
      ((IndexBTreePage *) index)->FlushMetaData();
    }
    buf_mgr->FlushAll();
    g_data_store->Write(GetStorageId(), "metadata", "pg_cnt_", pg_cnt_);
  }
  g_data_store->Write(GetStorageId(), "metadata", "row_cnt_", row_cnt_);
}

size_t ITable::GetPgCnt() {
  size_t cnt = 0;
  for (auto &pairs: indexes_) {
    cnt += ((IndexBTreePage *) pairs.second)->GetMetaData().num_nodes_;
  }
  return cnt + pg_cnt_;
}

} // arboretum