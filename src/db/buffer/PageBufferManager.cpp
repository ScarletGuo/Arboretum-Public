//
// Created by Zhihan Guo on 4/2/23.
//

#include "PageBufferManager.h"
#include "common/Worker.h"
#include "common/Stats.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"
#include "db/ARDB.h"

namespace arboretum {

size_t PageBufferManager::CalculateNumSlots(size_t total_sz) {
  auto num_slots = total_sz /
      (0.5 * sizeof(void *) + 0.5 * sizeof(RWLock)
      + sizeof(BucketNode) + sizeof(BufferDesc) + sizeof(std::mutex)
      + sizeof(BasePage));
  return num_slots;
}

PageBufferManager::PageBufferManager(ARDB *db) {
  LOG_INFO("Initiating page buffer manager with %zu empty slots. ",
           g_pagebuf_num_slots);
  db_ = db;
  // init buckets
  bucket_sz_ = g_pagebuf_num_slots / 2;
  bucket_hdrs_ =
      (BucketNode **) MemoryAllocator::Alloc(sizeof(void *) * bucket_sz_);
  bucket_latches_ =
      (RWLock *) MemoryAllocator::Alloc(sizeof(RWLock) * bucket_sz_);
  for (size_t i = 0; i < bucket_sz_; i++) {
    bucket_hdrs_[i] = nullptr;
    new(&bucket_latches_[i]) RWLock();
  }
  // init buffer descriptor with PAGE_BUF_SLOTS slots.
  M_ASSERT(g_pagebuf_num_slots < PAGEBUF_MAX_NUM_SLOTS,
           "number of buffer slot exceed max of %u.",
           PAGEBUF_MAX_NUM_SLOTS);
  bucket_nodes_ = (BucketNode *) MemoryAllocator::Alloc(
      sizeof(BucketNode) * g_pagebuf_num_slots);
  buf_descs_ = (BufferDesc *) MemoryAllocator::Alloc(
      sizeof(BufferDesc) * g_pagebuf_num_slots);
  desc_latches_ = (std::mutex *) MemoryAllocator::Alloc(
      sizeof(std::mutex) * g_pagebuf_num_slots);
  M_ASSERT(buf_descs_,
           "fail to allocate space, need to decrease PAGE_BUF_SLOTS; currently expected space: %lu bytes.",
           sizeof(BufferDesc) * g_pagebuf_num_slots);
  free_head_ = nullptr;
  for (int64_t i = g_pagebuf_num_slots - 1; i >= 0; i--) {
    new(&bucket_nodes_[i]) BucketNode;
    bucket_nodes_[i].buf_id = i;
    new(&desc_latches_[i]) std::mutex();
    auto desc = new(&buf_descs_[i]) BufferDesc;
    desc->buf_id = i;
    desc->free_next_ = free_head_;
    free_head_ = desc;
  }
  evict_clock_hand_ = nullptr;
  evict_clock_tail_ = nullptr;
  // init buffer pool
  pool_ = (BasePage *) MemoryAllocator::Alloc(
      sizeof(BasePage) * g_pagebuf_num_slots);
  for (size_t i = 0; i < g_pagebuf_num_slots; i++) {
    new(&pool_[i]) BasePage();
    pool_[i].header_.buf_id = i;
  }
}

PageBufferManager::~PageBufferManager() {
  MemoryAllocator::Dealloc(bucket_latches_);
  MemoryAllocator::Dealloc(bucket_nodes_);
  MemoryAllocator::Dealloc(bucket_hdrs_);
  MemoryAllocator::Dealloc(buf_descs_);
  MemoryAllocator::Dealloc(desc_latches_);
  MemoryAllocator::Dealloc(pool_);
}

ITuple *PageBufferManager::AccessTuple(ITable * table, SharedPtr &ref, uint64_t expected_key) {
  auto item_tag = ref.ReadAsPageTag();
  auto data_pg = AccessPage(table->GetTableId(), item_tag.pg_id_);
  auto tuple = reinterpret_cast<ITuple *>(data_pg->GetData(item_tag.first_));
  // use tuple's buf state to store current data page buf_id
  tuple->buf_state_ = data_pg->header_.buf_id;
  ref.InitAsGeneralPtr(tuple);
  if (tuple->GetTID() != expected_key) {
    LOG_ERROR("expecting key %lu, got key %u from page %u offset %u!",
              expected_key, tuple->GetTID(), item_tag.pg_id_, item_tag.first_);
  }
  return tuple;
}

void PageBufferManager::FinishAccessTuple(SharedPtr &ref, bool dirty) {
  auto tuple = reinterpret_cast<ITuple *>(ref.Get());
  auto data_pg = AccessPage(tuple->buf_state_);
  FinishAccessPage(data_pg, dirty);
}

BasePage *PageBufferManager::AllocNewPage(OID table_id, OID page_id) {
  auto bucket_id = CalBucketId(table_id, page_id);
  // create a new bucket node and avoid others checking the bucket node until ready.
  // find an empty slot
  auto bucket_node = AllocNewBucketNode();
  bucket_node->tag = {table_id, page_id};
  bucket_latches_[bucket_id].LockEX();
  free_desc_mutex_.lock();
  // book a buffer desc and hold the lock to prevent others accessing it before ready
  OID buf_id = FindEmptyBufDesc();
  // acquire free_desc_mutex before bucket latch to avoid deadlock.
  bucket_node->buf_id = buf_id;
  buf_descs_[buf_id].bucket_node_ = bucket_node;
  InsertIntoBucket(bucket_id, bucket_node);
  bucket_latches_[bucket_id].Unlock();
  // flush dirty page
  FlushAndLoad(buf_id, bucket_node->tag, false);
  // record idx page proportion for admitted page
  if (table_id >= (1UL << 28)) {
    idx_allocated_++;
    if (Worker::GetThdId() == 0)
      g_stats->db_stats_[Worker::GetThdId()].set_bufferd_idx_pgs_(idx_allocated_);
  }
  pool_[buf_id].header_.buf_id = buf_id;
  pool_[buf_id].header_.tag = bucket_node->tag;
  buf_descs_[buf_id].Pin(table_id >= (1UL << 28) ? 2 : 1, true);
  desc_latches_[buf_id].unlock();
  return &pool_[buf_id];
}

BasePage *
PageBufferManager::AccessPage(OID table_id, OID page_id) {
  // LOG_DEBUG("Finish access page tbl-%u pg-%u", table_id, page_id);
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    if (table_id >= (1UL << 28))
      g_stats->db_stats_[Worker::GetThdId()].incr_idx_accesses_(1);
  }
  auto bucket_id = CalBucketId(table_id, page_id);
  auto buf_id = LookUpBufferId(bucket_id, table_id, page_id);
  if (buf_id != PAGEBUF_INVALID_BUFID) {
    // found the page, pin it
    auto desc = &buf_descs_[buf_id];
    desc_latches_[buf_id].lock();
    desc->Pin(table_id >= (1UL << 28) ? 2 : 1, false);
    desc_latches_[buf_id].unlock();
    bucket_latches_[bucket_id].Unlock();
    return &pool_[buf_id];
  }
  // not found, try to load from storage
  auto bucket_node = AllocNewBucketNode();
  bucket_node->tag = {table_id, page_id};
  free_desc_mutex_.lock();
  buf_id = FindEmptyBufDesc();
  // insert into bucket while desc lock is still held
  bucket_node->buf_id = buf_id;
  buf_descs_[buf_id].bucket_node_ = bucket_node;
  InsertIntoBucket(bucket_id, bucket_node);
  bucket_latches_[bucket_id].Unlock();
  // load from storage or flush current
  FlushAndLoad(buf_id, bucket_node->tag, true);
  pool_[buf_id].header_.buf_id = buf_id;
  pool_[buf_id].header_.tag = bucket_node->tag;
  buf_descs_[buf_id].Pin(table_id >= (1UL << 28) ? 2 : 1, true);
  // record idx page proportion for admitted page
  desc_latches_[buf_id].unlock();
  // count towards a miss
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
    if (table_id >= (1UL << 28))
      g_stats->db_stats_[Worker::GetThdId()].incr_idx_misses_(1);
  }
  return &pool_[buf_id];
}

OID PageBufferManager::FindEmptyBufDesc() {
  BufferDesc *desc;
  OID buf_id;
  // protected by global latch, allocated will not change during the time
  if (allocated_ + 1 > g_pagebuf_num_slots) {
    // evict
    buf_id = FindBufDescToEvict();
  } else {
    desc = free_head_;
    free_head_ = desc->free_next_;
    buf_id = desc->buf_id;
    desc_latches_[buf_id].lock();
    desc->buf_state_ += BUF_REFCOUNT_ONE; // must pin now to avoid being evicted
    ClockInsert(evict_clock_hand_, evict_clock_tail_, desc);
    allocated_++;
    free_desc_mutex_.unlock();
  }
  return buf_id;
}

void PageBufferManager::FinishAccessPage(BasePage *page, bool dirty) {
  // LOG_DEBUG("Finish access page tbl-%u pg-%u", page->GetTableID(), page->GetPageID());
  auto buf_id = page->header_.buf_id;
  auto desc = &buf_descs_[buf_id];
  desc_latches_[buf_id].lock();
  desc->UnPin(dirty);
  desc_latches_[buf_id].unlock();
}

OID PageBufferManager::LookUpBufferId(OID bucket_id, OID table_id, OID page_id) {
  OID buf_id = PAGEBUF_INVALID_BUFID;
  bucket_latches_[bucket_id].LockEX();
  auto bucket_node = bucket_hdrs_[bucket_id];
  while (bucket_node) {
    // remove invalid node if encountered any
    if (bucket_node->buf_id == PAGEBUF_INVALID_BUFID) {
      auto next = bucket_node->next;
      if (bucket_node->prev) {
        bucket_node->prev->next = bucket_node->next;
      } else {
        bucket_hdrs_[bucket_id] = bucket_node->next;
      }
      if (bucket_node->next) {
        bucket_node->next->prev = bucket_node->prev;
      }
      MemoryAllocator::Dealloc(bucket_node);
      bucket_node = next;
      continue;
    }
    if (bucket_node->tag.first_ == table_id && bucket_node->tag.pg_id_ == page_id) {
      buf_id = bucket_node->buf_id;
      break;
    }
    bucket_node = bucket_node->next;
  }
  return buf_id;
}

PageBufferManager::BucketNode * PageBufferManager::AllocNewBucketNode() {
  auto node = (BucketNode *) MemoryAllocator::Alloc(sizeof(BucketNode));
  node->buf_id = PAGEBUF_INVALID_BUFID;
  node->next = nullptr;
  node->prev = nullptr;
  return node;
}

void PageBufferManager::InsertIntoBucket(OID bucket_id, BucketNode * node) {
  node->next = bucket_hdrs_[bucket_id];
  node->prev = nullptr;
  if (node->next)
    node->next->prev = node;
  bucket_hdrs_[bucket_id] = node;
}

OID PageBufferManager::FindBufDescToEvict() {
  auto cnt = allocated_.load();
  auto iterations = 0;
  while (evict_clock_hand_) {
    if (cnt == 0) {
      iterations++;
      cnt = allocated_.load();
      if (iterations >= BM_MAX_USAGE_COUNT) {
        LOG_ERROR("All pinned. please increase buffer size");
      }
    }
    cnt--;
    OID buf_id = evict_clock_hand_->buf_id;
    if (evict_clock_hand_->IsPinned()) {
      // cannot evict pinned
      ClockAdvance(evict_clock_hand_, evict_clock_tail_);
      continue;
    }
    // check usage count
    if (evict_clock_hand_->GetUsageCount() != 0) {
      // decrease usage count
      evict_clock_hand_->buf_state_.fetch_sub(BUF_USAGECOUNT_ONE);
      ClockAdvance(evict_clock_hand_, evict_clock_tail_);
      continue;
    }
    // book the slot so that no one else can access it
    if (!desc_latches_[buf_id].try_lock()) {
      ClockAdvance(evict_clock_hand_, evict_clock_tail_);
      continue;
    }
    // remove evicted bucket node from bucket list
    auto desc = evict_clock_hand_;
    // make sure current node will not be immediately checked by next evictor
    ClockAdvance(evict_clock_hand_, evict_clock_tail_);
    // pin current page immediately to avoid others evict it
    desc->buf_state_ += BUF_REFCOUNT_ONE;
    // avoid others accessing the old page, but the node will be removed later
    desc->bucket_node_->buf_id = PAGEBUF_INVALID_BUFID;
    // need to make sure current slot is always pinned to avoid being evicted by others
    free_desc_mutex_.unlock();
    return desc->buf_id;
  }
  LOG_ERROR("increase buffer size");
}

void PageBufferManager::FlushAndLoad(OID buf_id, PageTag &tag, bool load_from_storage) {
  auto desc = &buf_descs_[buf_id];
  auto &pg = pool_[buf_id];
  auto pg_start = reinterpret_cast<char *> (&pg);
  // record idx page proportion for evicted page
  if (pg.GetTableID() >= (1UL << 28)) {
    idx_allocated_--;
    if (Worker::GetThdId() == 0)
      g_stats->db_stats_[Worker::GetThdId()].set_bufferd_idx_pgs_(idx_allocated_);
  }
  if (load_from_storage) {
    // LOG_DEBUG("try to load tbl-%u pg-%u from storage to buf slot %u",
    // tag.first_, tag.pg_id_, buf_id);
    std::string data;
    // record idx page proportion for admitted page
    if (tag.first_ >= (1UL << 28)) {
      idx_allocated_++;
      if (Worker::GetThdId() == 0)
        g_stats->db_stats_[Worker::GetThdId()].set_bufferd_idx_pgs_(idx_allocated_);
    }
    auto load_tbl_name = ITable::GetStorageId(tag.first_);
    auto load_key = tag.GetStorageKey();
    if (desc->CheckFlag(BM_DIRTY)) {
      auto store_tbl_name = ITable::GetStorageId(pg.header_.tag.first_);
      auto store_key = pg.header_.tag.GetStorageKey();
      if (store_tbl_name == load_tbl_name) {
        g_data_store->WriteAndRead(store_tbl_name, store_key,
                                   pg_start, sizeof(BasePage),
                                   load_key, data);
      } else {
        g_data_store->Write(store_tbl_name,store_key, pg_start, sizeof(BasePage));
        g_data_store->Read(load_tbl_name, load_key, data);
      }
    } else {
      g_data_store->Read(load_tbl_name, load_key, data);
    }
    pg.SetFromLoaded(const_cast<char *>(data.c_str()), sizeof(BasePage));
    M_ASSERT(pg.header_.tag == tag, "Loaded page does not match requested info!");
  } else {
    if (desc->CheckFlag(BM_DIRTY)) {
      auto store_key = pg.header_.tag.GetStorageKey();
      g_data_store->Write(ITable::GetStorageId(pg.header_.tag.first_),
                          store_key, pg_start, sizeof(BasePage));
    }
    pg.ResetPage();
  }
}

void PageBufferManager::FlushAll() {
  auto progress = 0;
  auto portion = allocated_ / 100;
  auto starttime = GetSystemClock();
  auto cnt = allocated_.load();
  /*
  auto batch_sz = 1;
  volatile RC * status = NEW_SZ(volatile RC, batch_sz);
  for (size_t i = 0; i < batch_sz; i++) {
    status[i] = RC::OK;
  }
  */
  while (allocated_ > 0 && evict_clock_hand_) {
    if (!g_restore_from_remote && allocated_ % portion == 0 && progress <= 100) {
      LOG_DEBUG("Loading progress: %3d %%", progress);
      progress++;
    }
    auto &pg = pool_[evict_clock_hand_->buf_id];
    auto start = reinterpret_cast<char *> (&pg);
    auto key = pg.header_.tag.GetStorageKey();
    auto table_name = ITable::GetStorageId(pg.header_.tag.first_);
    g_data_store->Write(table_name, key, start, sizeof(BasePage));
    ClockRemoveHand(evict_clock_hand_, evict_clock_tail_, allocated_);
  }
  LOG_INFO("Data loading takes %.2f seconds in total. flushed %lu pages",
           (GetSystemClock() - starttime) / 1000000000.0, cnt);
}

// BufferDesc Methods
void PageBufferManager::BufferDesc::Pin(uint64_t weight, bool reset) {
  // pin cnt can be larger than # worker thread since a txn may access
  // a page multiple times to fetch different tuples.
  auto init_pin_cnt = GetPinCount();
  M_ASSERT(init_pin_cnt < BM_MAX_REF_COUNT,
           "pin count %u exceed the limit of %d", init_pin_cnt, BM_MAX_REF_COUNT);
  // usage count cannot exceed the max, protected by desc_latch
  if (reset) {
    buf_state_ = BUF_REFCOUNT_ONE + weight * BUF_USAGECOUNT_ONE;
    assert(GetPinCount() == 1);
  } else {
    auto usage = GetUsageCount();
    if (usage + weight <= BM_MAX_USAGE_COUNT) {
      buf_state_ += weight * BUF_USAGECOUNT_ONE + BUF_REFCOUNT_ONE;
    } else {
      buf_state_ += (BM_MAX_USAGE_COUNT - usage) * BUF_USAGECOUNT_ONE
          + BUF_REFCOUNT_ONE;
    }
    M_ASSERT(GetUsageCount() <= BM_MAX_USAGE_COUNT,
             "usage exceeding limit; may due to pin cnt overflow");
  }
  M_ASSERT(IsPinned(), "pin failure");
}

void PageBufferManager::BufferDesc::UnPin(bool dirty) {
  // decrement refcount and set dirty bit if needed
  if (dirty && g_warmup_finished) {
    // must hold the lock in exclusive mode
    SetFlag(BM_DIRTY);
    if (GetUsageCount() < BM_MAX_USAGE_COUNT)
      buf_state_ += BUF_USAGECOUNT_ONE;
  }
  M_ASSERT(GetPinCount() != 0, "Unpin a page with 0 pin count");
  buf_state_ -= BUF_REFCOUNT_ONE;
}


}