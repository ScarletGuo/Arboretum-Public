//
// Created by Zhihan Guo on 4/29/23.
//

#include "IndexBTreePage.h"
#include "db/buffer/PageBufferManager.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "db/ITxn.h"
#include "db/CCManager.h"

namespace arboretum {

IndexBTreePage::IndexBTreePage(OID tbl_id) : IIndex(tbl_id) {
  buf_mgr_ = (PageBufferManager *) g_buf_mgr;
  // XXX: for phantom protection purpose, use another table for index lookup
  std::string dataset_id(g_dataset_id);
  storage_id_ = "page-wl" +
      dataset_id.substr(4, dataset_id.size() - 4) +
      "-" + std::to_string(GetIndexTableId());
  if (g_restore_from_remote) {
    // NOTE: if not warmed up, load as fresh but not mark page as dirty
    // load meta data.
    std::string data;
    g_data_store->Read(storage_id_, "metadata", "metapage", data);
    memcpy(&meta_data_, data.c_str(), data.size());
    // update root buf id
    auto root_node = AccessNode(meta_data_.root_pg_id_, false);
    meta_data_.root_buf_id_ = root_node->pg_->header_.buf_id;
    // do not unpin root page to make it stay in memory
    UnLockNode(root_node);
    LOG_INFO("Load btree index root from storage with fanout = %zu and split_threshold = %zu",
             meta_data_.fanout_, meta_data_.split_bar_);
    LOG_INFO("remote storage has %u index nodes in total", meta_data_.num_nodes_.load());
  } else {
    g_data_store->CreateTable(storage_id_);
    meta_data_.fanout_ = g_idx_btree_fanout;
    meta_data_.split_bar_ = std::min((size_t)(meta_data_.fanout_ * g_idx_btree_split_ratio),
                                     meta_data_.fanout_ - 1);
    meta_data_.num_nodes_ = 1; // avoid assigning page tag = {0, 0}, which is reserved as null
    auto root_node = AllocNewNode();
    meta_data_.root_pg_id_ = root_node->pg_->GetPageID();
    meta_data_.root_buf_id_ = root_node->pg_->header_.buf_id;
    LOG_INFO("Created btree index with fanout = %zu and split_threshold = %zu. ",
             meta_data_.fanout_, meta_data_.split_bar_);
    auto root_pg = buf_mgr_->AccessPage(meta_data_.root_buf_id_);
    buf_mgr_->FinishAccessPage(root_pg, true);
  }
}

SharedPtr* IndexBTreePage::RangeSearch(SearchKey start, SearchKey end, ITxn *txn,
                                       RC &rc, size_t &cnt) {
  auto expected_cnt = end.ToUInt64() - start.ToUInt64() + 1;
  auto result = NEW_SZ(SharedPtr, expected_cnt);
  BTreePgNode * parent;
  BTreePgNode * child;
  Search(start, AccessType::READ, parent, child);
  cnt = 0;
  rc = RC::OK;
  IdxAccess idx_ac = {SCAN, tbl_id_};
  // move right if needed
  while (true) {
    for (auto i = 0; i < child->key_cnt_; i++) {
      if (start <= child->keys_[i] && child->keys_[i] <= end) {
        // try to acquire lock
        if (g_enable_phantom_protection) {
          rc = CCManager::AcquireLock(child->next_key_lock_, false);
          if (rc == ABORT) break;
          // XXX: what if page is evicted? need to separate from.
          //  currently it is not possible since the leaf is pinned.
          idx_ac.locks_.push_back(&child->next_key_lock_);
        }
        // add to access list
        result[cnt++].InitAsPageTag(child->children_[i + 1]);
//        LOG_DEBUG("found key %lu at (pg=%u, offset=%u) for range search",
//                  child->keys_[i].ToUInt64(), child->children_[i + 1].pg_id_,
//                  child->children_[i + 1].first_);
      }
    }
    if (rc == ABORT) break;
    if (child->keys_[child->key_cnt_ - 1] > end || !child->HasNext()) break;
    // coupling
    auto node = child;
    child = AccessNode(child->GetNext().pg_id_, false);
    FinishAccessNode(node, false);
  }
  FinishAccessNode(child, false);
  if (rc == RC::ABORT) {
    // release previously held index locks
    for (auto & lock : idx_ac.locks_) {
      CCManager::ReleaseLock(*lock, false);
    }
    DEALLOC(result);
    return nullptr;
  } else {
    if (cnt == 0)
      LOG_ERROR("scan result is empty!");
    if (g_enable_phantom_protection)
      txn->idx_accesses_.push_back(idx_ac);
  }
  return result;
}


SharedPtr* IndexBTreePage::Search(SearchKey key, int limit) {
  auto result = NEW_SZ(SharedPtr, limit);
  BTreePgNode * parent;
  BTreePgNode * child;
  Search(key, AccessType::READ, parent, child);
  // check if key in child
  int cnt = 0;
  for (auto i = 0; i < child->key_cnt_; i++) {
    if (child->keys_[i] == key) {
      result[cnt].InitAsPageTag(child->children_[i + 1]);
      cnt++;
      if (cnt == limit)
        break;
    }
  }
  FinishAccessNode(child, false);
  if (cnt == 0) {
    DEALLOC(result);
    LOG_ERROR("key %lu not found", key.ToUInt64());
    return nullptr;
  }
  return result;
}


void
IndexBTreePage::Search(SearchKey key, AccessType ac,
                       BTreePgNode *&parent, BTreePgNode *&child) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  auto is_update = false;
  parent = nullptr;
  child = AccessNode(meta_data_.root_pg_id_, (ac == INSERT || ac == DELETE)
      && (meta_data_.height_ <= 1));
  // In case root is out of date
  assert(IsRoot(child));
  while (true) {
    if (child->IsLeaf()) {
      if (ac != INSERT && ac != DELETE)
        if (parent) FinishAccessNode(parent, false);
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // no need to use parent which will become grandparent
    if (parent) FinishAccessNode(parent, false);
    if (ac == UPDATE) {
      // TODO: currently assuming update does not change index key.
      if (child->level_ == 0) is_update = true;
    } else if (ac == INSERT || ac == DELETE) {
      if (child->level_ <= 2) is_update = true;
    }
    parent = child;
    child = AccessNode(child->children_[offset].pg_id_, is_update);
  }
}


void IndexBTreePage::Insert(SearchKey key, OID pg_id, OID offset) {
  BTreePgNode * parent;
  BTreePgNode * child;
  Search(key, AccessType::INSERT, parent, child);
  PageTag tag = {offset, pg_id};
  // the last two layers are EX locked, see if child and parent will split
  if (!SplitSafe(child) && !SplitSafe(parent)) {
    // restart with all exclusive lock from non-split safe point.
    FinishAccessNode(child, false);
    FinishAccessNode(parent, false);
    // LOG_DEBUG("may split more than 2 levels, start pessimistic search!");
    auto stack = PessimisticSearch(key, parent, child, INSERT);
    // insert bottom-up recursively with a stack.
    RecursiveInsert(stack, key, tag, child, stack);
    FinishAccessNode(child, true);
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    RecursiveInsert(stack, key, tag, child, stack);
    FinishAccessNode(child, true);
  }
}

IndexBTreePage::Stack
IndexBTreePage::PessimisticSearch(SearchKey key, BTreePgNode *&parent,
                                  BTreePgNode *&child, AccessType ac) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  Stack stack = nullptr;
  auto is_update = true;
  parent = nullptr;
  child = AccessNode(meta_data_.root_pg_id_, is_update);
  while (true) {
    if (child->IsLeaf()) {
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // save stack if it is pessimistic and may split
    auto new_stack = NEW(StackData);
    new_stack->node_ = child;
    new_stack->parent_ = stack;
    stack = new_stack;
    // no need to use parent which will become grandparent
    // TODO: check delete safe for delete operations
    bool safe = (ac == INSERT && SplitSafe(child)) ||
        (ac == DELETE && DeleteSafe(child) );
    if (safe && parent) {
      // free entire stack recursively
      auto parent_stack = stack->parent_;
      assert(parent_stack->node_ == parent);
      while (parent_stack) {
        if (parent_stack->node_) UnLockNode(parent_stack->node_);
        auto to_delete = parent_stack;
        parent_stack = parent_stack->parent_;
        DEALLOC(to_delete);
      }
      stack->parent_ = nullptr;
    }
    parent = child;
    child = AccessNode(child->children_[offset].pg_id_, is_update);
  }
  return stack;
}

void
IndexBTreePage::RecursiveInsert(Stack stack, SearchKey key, PageTag &val,
                                BTreePgNode *child, Stack full_stack) {
  auto parent = stack ? stack->node_ : nullptr;
  if (SplitSafe(child)) {
    // insert into child and return
    child->SafeInsert(key, val);
    // delete remaining stack
    FreeStack(full_stack);
  } else {
    child->SafeInsert(key, val);
    // split child and insert new key separator into parent
    auto split_pos = child->key_cnt_ / 2;
    auto split_key = child->keys_[split_pos];
    assert(split_pos - 1 >= 0);
    assert(split_pos + 1 < child->key_cnt_);
    auto new_node = AllocNewNode();
    new_node->level_ = child->level_;
    // move from split pos to new node
    // non leaf: [1, 3, 5], [(,1), [1, 3), [3,5), [5, )], split pos = 1
    // key: [5] moved to new node (offset=2),
    // value: [(, 1], [1, 3)] moved to new node (offset = 2)
    // leaf: [1,2,3], [-, 1, 2, 3], split pos = 1, 2 is separator key
    // key: [2,3] moved to new node (offset = 1)
    auto move_start = child->IsLeaf() ? split_pos : split_pos + 1;
    auto num_ele = child->key_cnt_ - move_start;
    memmove(&new_node->keys_[0], &child->keys_[move_start],
            num_ele * sizeof(SearchKey));
    if(child->IsLeaf()) {
      memmove(&new_node->children_[1], &child->children_[split_pos + 1],
              num_ele * sizeof(SharedPtr));
      // update right link.
      new_node->SetNext(child->GetNext());
      child->SetNext(new_node->pg_tag_);
    } else {
      memmove(&new_node->children_[0], &child->children_[split_pos + 1],
              (num_ele + 1) * sizeof(SharedPtr));
    }
    child->key_cnt_ = split_pos;
    new_node->key_cnt_ = num_ele;
    if (IsRoot(child)) {
      // split root by adding a new root
      auto new_root = AllocNewNode();
      new_root->level_ = child->level_ + 1;
      new_root->children_[0] = child->pg_tag_;
      LockNode(new_root, true);
      new_root->SafeInsert(split_key, new_node->pg_tag_);
      FinishAccessNode(new_root, true);
      meta_data_.height_++;
      // this line must happen in the end to avoid others touching new root
      meta_data_.root_buf_id_ = new_root->pg_->header_.buf_id;
      meta_data_.root_pg_id_ = new_root->pg_->GetPageID();
      FreeStack(full_stack);
      return;
    }
    // insert into parent, parent may still be null even child cannot be root
    // in case it is split safe and not added!
    if (stack) {
      // TODO: double check correctness.
      stack->dirty_ = true;
      stack = stack->parent_;
      RecursiveInsert(stack, split_key, new_node->pg_tag_, parent, full_stack);
    }
  }
}


BTreePgNode *IndexBTreePage::AccessNode(OID pg_id, bool exclusive) {
  auto pg = buf_mgr_->AccessPage(GetIndexTableId(), pg_id);
  if (exclusive) {
    buf_mgr_->LockEX(pg);
  } else {
    buf_mgr_->LockSH(pg);
  }
  auto node = (BTreePgNode *) pg->GetData(0);
  // if load from storage, reset node pointers.
  if ((char *) node->keys_ != ((char *)node) + sizeof(BTreePgNode)) {
    node->keys_ =
        reinterpret_cast<SearchKey *>((char *) node + sizeof(BTreePgNode));
    node->children_ = reinterpret_cast<PageTag *>((char *) node->keys_
        + sizeof(SearchKey) * meta_data_.fanout_);
  }
  node->pg_ = pg;
  return node;
}

void IndexBTreePage::FinishAccessNode(BTreePgNode *node, bool dirty) {
  auto buf_mgr = (PageBufferManager *) g_buf_mgr;
  buf_mgr->UnLock(node->pg_);
  buf_mgr->FinishAccessPage(node->pg_, dirty);
}

BTreePgNode *IndexBTreePage::AllocNewNode() {
  auto buf_mgr = (PageBufferManager *) g_buf_mgr;
  auto pg = buf_mgr->AllocNewPage(GetIndexTableId(),
                                  meta_data_.num_nodes_.fetch_add(1));
  auto sz = sizeof(BTreePgNode) + sizeof(SearchKey) * meta_data_.fanout_
      + sizeof(PageTag) * meta_data_.fanout_;
  M_ASSERT(sz < pg->MaxSizeOfNextItem(),
           "Size of btree node exceeds page size, please decrease fanout size!");
  auto addr = pg->AllocDataSpace(sz);
  auto node = new (addr) BTreePgNode();
  node->pg_ = pg;
  node->pg_tag_ = pg->header_.tag;
  node->keys_ = reinterpret_cast<SearchKey *>((char *) addr + sizeof(BTreePgNode));
  node->children_ = reinterpret_cast<PageTag *>((char *) addr
      + sizeof(BTreePgNode) + sizeof(SearchKey) * meta_data_.fanout_);
  node->children_[0].first_ = 0;
  node->children_[0].pg_id_ = 0;
  return node;
}

void IndexBTreePage::LockNode(BTreePgNode *node, bool exclusive) {
  if (exclusive)
    buf_mgr_->LockEX(node->pg_);
  else
    buf_mgr_->LockSH(node->pg_);
}

void IndexBTreePage::UnLockNode(BTreePgNode *node) {
  buf_mgr_->UnLock(node->pg_);
}

OID IndexBTreePage::ScanBranchNode(BTreePgNode *node, SearchKey key) {
  // TODO: optimize to use binary search or interpolation search
  // if internal node, return the offset of the pointer to child
  // e.g. [1,3,5], [p0,p1,p2,p3]
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      return i;
    } else if (key == node->keys_[i]) {
      return i + 1;
    } else if (key > node->keys_[i]) {
      continue;
    }
  }
  // out of bound
  return node->key_cnt_;
}

PageTag * IndexBTreePage::ScanLeafNode(BTreePgNode *node, SearchKey key) {
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      break;
    } else if (key == node->keys_[i]) {
      return &node->children_[i + 1];
    }
  }
  return nullptr;
}

void IndexBTreePage::FreeStack(Stack full_stack) {
  auto stack = full_stack;
  while (stack) {
    if (stack->node_) FinishAccessNode(stack->node_, stack->dirty_);
    auto used_stack = stack;
    stack = stack->parent_;
    DEALLOC(used_stack);
  }
}

void IndexBTreePage::FlushMetaData() {
  LOG_INFO("All data are inserted to index, which now contains %u nodes",
           meta_data_.num_nodes_.load());
  g_data_store->Write(storage_id_, "metadata", "metapage",
                      reinterpret_cast<char *>(&meta_data_),
                      sizeof(BTreePgMetaData));
}

void IndexBTreePage::ParallelLoad(int thd, int num_thds, OID tbl_id, size_t cnt) {
  Worker::SetThdId(thd);
  auto starttime = GetSystemClock();
  int req = 0;
  for (size_t i = 1; i < cnt; i++) {
    if (i % num_thds != thd)
      continue;
    if (buf_mgr_->IsWarmedUp(cnt * 2)) {
      break;
    }
    if (req != 0 && thd == 0 && req % 10000 == 0) {
      auto latency = (GetSystemClock() - starttime) / 1000000.0;
      auto total_allocated = buf_mgr_->GetAllocated();
      LOG_DEBUG("[thd-%d] loading progress %zu / %zu, thread throughput = %.2f pages/sec, "
                "latency = %.2f ms per req",
                thd, total_allocated, g_total_buf_sz,
                total_allocated / (latency / 1000.0), latency / req);
    }
    auto pg = buf_mgr_->AccessPage(tbl_id, i);
    req++;
    buf_mgr_->FinishAccessPage(pg, false);
  }
}

void IndexBTreePage::Load(size_t pg_cnt) {
  // start multiple threads to load in parallel
  LOG_DEBUG("start loading data pages and index pages with %ld threads",
            g_num_restore_thds * 2);
  std::vector<std::thread> threads;
  auto starttime = GetSystemClock();
  // load index pages
  for (size_t i = 0; i < g_num_restore_thds; i++) {
    threads.emplace_back(IndexBTreePage::ExecuteLoad, this, i, g_num_restore_thds,
                         GetIndexTableId(), meta_data_.num_nodes_.load());
  }
  // load data pages
  for (size_t i = 0; i < g_num_restore_thds; i++) {
    threads.emplace_back(IndexBTreePage::ExecuteLoad, this, i, g_num_restore_thds,
                         tbl_id_, pg_cnt);
  }
  std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
  auto latency = (GetSystemClock() - starttime) / 1000000000.0;
  LOG_DEBUG("warm up page buffer takes %.2f seconds, throughput = %.2f pages/sec",
            latency, buf_mgr_->GetAllocated() / latency);
}

// ============

void BTreePgNode::SafeInsert(SearchKey &key, PageTag &val) {
  // find the right insert position
  OID idx;
  for (idx = 0; idx < key_cnt_; idx++) {
    if (keys_[idx] > key) {
      break;
    }
    if (level_ == 0 && keys_[idx] == key) {
      // already exist, do not insert and return the existing tuple
      LOG_DEBUG("key %lu already exist, do not insert", key.ToUInt64());
      return;
    }
  }
  if (idx != key_cnt_) {
    // need to shift
    auto shift_num = key_cnt_ - idx;
    memmove(&keys_[idx + 1], &keys_[idx],
            sizeof(SearchKey) * shift_num);
    memmove(&children_[idx + 2], &children_[idx + 1],
            sizeof(SharedPtr) * shift_num);
  }
  keys_[idx] = key;
  // init ref count = 1.
  if (IsLeaf()) {
    children_[idx + 1] = val;
  } else {
    children_[idx + 1] = val;
  }
  key_cnt_++;
}

} // namespace arboretum