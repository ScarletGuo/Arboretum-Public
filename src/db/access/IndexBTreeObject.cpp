//
// Created by Zhihan Guo on 4/3/23.
//

#include "IndexBTreeObject.h"
#include "ITuple.h"
#include "db/buffer/ObjectBufferManager.h"
#include "db/CCManager.h"
#include "db/ITxn.h"


namespace arboretum {

IndexBTreeObject::IndexBTreeObject(OID tbl_id) : IIndex(tbl_id) {
  fanout_ = g_idx_btree_fanout;
  split_bar_ = std::min((size_t)(fanout_ * g_idx_btree_split_ratio), fanout_ - 1);
  auto root_node = AllocNewNode();
  root_.Init(root_node);
  node_sz_ = sizeof(BTreeObjNode) + sizeof(SearchKey) * fanout_ +
      sizeof(SharedPtr) * (fanout_ + 1);
  LOG_INFO("Created btree index with fanout = %zu and split_threshold = %zu",
           fanout_, split_bar_);
}

#define OBJ_BTREE_RANGE_SEARCH_HELPER(lock, fetch_remote, left_bound, right_bound) { \
  M_ASSERT(right_bound.ToUInt64() > 0, "right bound should > 0");                 \
  rc = g_enable_phantom_protection ? CCManager::AcquireLock(*lock, false) : OK;   \
  if (rc != RC::OK) break;                                                        \
  if (g_enable_phantom_protection) idx_ac.locks_.push_back(lock);                 \
  if (fetch_remote) {                                                             \
    auto from = std::max(SearchKey(start.ToUInt64() - 1), left_bound);            \
    auto to = std::min(SearchKey(end.ToUInt64() + 1), right_bound);                   \
    fetch_req.emplace_back(from, to);                                             \
  }                                                                               \
}


SharedPtr* IndexBTreeObject::RangeSearch(SearchKey start, SearchKey end, ITxn *txn,
                                         std::vector<std::pair<SearchKey, SearchKey>>
                                         &fetch_req, RC &rc, size_t &cnt) {
  // RC::ERROR - key not found
  // RC::OK - key is found
  // RC::ABORT - lock conflict with insert operations.
  // TODO: here we assume pkey (discrete values) so that we can preallocate space
  auto result = NEW_SZ(SharedPtr, end.ToUInt64() - start.ToUInt64() + 1);
  BTreeObjNode * parent;
  BTreeObjNode * child;
  rc = RC::ERROR;
  IdxAccess idx_ac = {SCAN, tbl_id_};
  // traverse from the start key
  Search(start, AccessType::READ, parent, child);
  while (true) {
    // take left bound lock if needed.
    if ((child->key_cnt_ > 0 && child->keys_[0] > start)
    || (child->key_cnt_ == 0 && child->right_bound_ > start)) {
      auto right_bound = child->key_cnt_ > 0 ? child->keys_[0] : child->right_bound_;
      auto next_key_in_storage = right_bound.ToUInt64() != child->left_bound_.ToUInt64();
      // XXX: need to use child->left_bound_in_storage_ in fact.
      OBJ_BTREE_RANGE_SEARCH_HELPER(&(child->left_bound_lock_),
                                    next_key_in_storage,
                                    child->left_bound_,
                                    right_bound);
    }
    for (int i = 0; i < child->key_cnt_; i++) {
      if (end < child->keys_[i]) break;
      if (child->right_bound_ <= start) break; // move to next leaf
      if (i + 1 < child->key_cnt_ && child->keys_[i + 1] <= start) continue;
      // lock current range [child->keys_[i], child->keys_[i + 1])
      auto tuple = reinterpret_cast<ITuple *>(child->children_[i + 1].Get());
      auto right_bound = (i == child->key_cnt_ - 1) ? child->right_bound_ :
          child->keys_[i + 1];
      // XXX(zhihan): judge in next key in storage based on continuity;
      // but it actually should be tuple->next_key_in_storage_ || !child->next_.
      // however, need to debug next_key_in_storage_ as it is not accurate now.
      auto next_key_in_storage = right_bound.ToUInt64() - child->keys_[i].ToUInt64() != 1;
      OBJ_BTREE_RANGE_SEARCH_HELPER(&tuple->next_key_lock_,
                                     next_key_in_storage,
                                    child->keys_[i],
                                    right_bound);
      // add current tuple if satisfies the searching criteria
      if (start <= child->keys_[i] && child->keys_[i] <= end) {
        cnt++;
        result[child->keys_[i].ToUInt64() - start.ToUInt64()].InitFrom(child->children_[i + 1]);
      }
    }
    if (child->right_bound_ <= end) {
      // continue to next leaf.
      auto next = const_cast<BTreeObjNode *>(child->next_);
      next->latch_.LockSH();
      child->latch_.Unlock();
      child = next;
    } else break;
  }
  child->latch_.Unlock();
  if (rc == RC::ABORT) {
    // release previously held index locks
    for (auto & lock : idx_ac.locks_) {
      CCManager::ReleaseLock(*lock, false);
    }
    DEALLOC(result);
    return nullptr;
  } else if (txn) {
    M_ASSERT(cnt > 0 || !fetch_req.empty(), "if not abort, result should not be empty.");
    if (g_enable_phantom_protection)
      txn->idx_accesses_.push_back(idx_ac);
  }
  return result;
}

SharedPtr* IndexBTreeObject::Search(SearchKey key, ITxn *txn, RC &rc) {
  // RC::ERROR - key not found
  // RC::OK - key is found
  // RC::ABORT - lock conflict with insert operations.
  auto result = NEW(SharedPtr);
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::READ, parent, child);
  // check if key in child
  int i = 0;
  rc = RC::ERROR;
  IdxAccess idx_ac = {READ, tbl_id_};
  for (i = 0; i < child->key_cnt_; i++) {
    if (child->keys_[i] == key) {
      auto tuple = reinterpret_cast<ITuple *>(child->children_[i + 1].Get());
      rc = CCManager::AcquireLock(tuple->next_key_lock_, false);
      // record access info in the txn on success
      if (rc == RC::OK && txn) {
        idx_ac.locks_.push_back(&tuple->next_key_lock_);
        // must init the pointer before releasing the latch
        result->InitFrom(child->children_[i + 1]);
      }
      break;
    }
  }
  child->latch_.Unlock();
  if (rc == RC::ABORT) {
    // release previously held index locks
    for (auto & lock : idx_ac.locks_) {
      CCManager::ReleaseLock(*lock, idx_ac.ac_);
    }
    DEALLOC(result);
    return nullptr;
  } else if (txn) {
    txn->idx_accesses_.push_back(idx_ac);
  }
  return result;
}

/*
 * Three types of inserts:
 *   !create_sharedptr && txn: user index insert
 *   !create_sharedptr && !txn: system index insert during warm up
 *   create_sharedptr && !txn: user reads triggering insert
 */
RC IndexBTreeObject::Insert(SearchKey key, ITuple * tuple, AccessType ac,
                                    ITxn *txn, SharedPtr *tag) {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::INSERT,parent, child);
  RC rc;
  // the last two layers are EX locked, see if child and parent will split
  if (!SplitSafe(child) && !SplitSafe(parent)) {
    // restart with all exclusive lock from non-split safe point.
    child->latch_.Unlock();
    parent->latch_.Unlock();
    // LOG_DEBUG("may split more than 2 levels, start pessimistic search!");
    auto stack = PessimisticSearch(key, parent, child, AccessType::INSERT);
    // insert bottom-up recursively with a stack.
    rc = RecursiveInsert(stack, key, tuple, child, stack, ac, txn, tag);
    child->latch_.Unlock();
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    rc = RecursiveInsert(stack, key, tuple, child, stack, ac, txn, tag);
    child->latch_.Unlock();
  }
  return rc;
}

void
IndexBTreeObject::Search(SearchKey key, AccessType ac,
                         BTreeObjNode *&parent, BTreeObjNode *&child) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  auto is_update = false;
  parent = nullptr;
  child = AccessNode(root_, (ac == INSERT || ac == DELETE)
  && (height_ <= 1));
  // In case root is out of date
  while (!IsRoot(child)) {
    child->latch_.Unlock();
    child = AccessNode(root_, (ac == INSERT || ac == DELETE)
        && (height_ <= 1));
    // LOG_ERROR("root out of date");
  }
  while (true) {
    if (child->IsLeaf()) {
      if (ac != INSERT && ac != DELETE)
        if (parent) parent->latch_.Unlock();
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // no need to use parent which will become grandparent
    if (parent) parent->latch_.Unlock();
    if (ac == UPDATE) {
      // TODO: currently assuming update does not change index key.
      if (child->level_ == 0) is_update = true;
    } else if (ac == INSERT || ac == DELETE) {
      if (child->level_ <= 2) is_update = true;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
}

IndexBTreeObject::Stack
IndexBTreeObject::PessimisticSearch(SearchKey key, BTreeObjNode *&parent,
                                    BTreeObjNode *&child, AccessType ac) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  Stack stack = nullptr;
  auto is_update = true;
  parent = nullptr;
  child = AccessNode(root_, is_update);
  assert(height_ != 0);
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
      assert(stack->parent_->node_ == parent);
      FreeStack(stack->parent_);
      stack->parent_ = nullptr;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
  return stack;
}

RC
IndexBTreeObject::RecursiveInsert(Stack stack, SearchKey key, void * val,
                                  BTreeObjNode *child, Stack full_stack,
                                  AccessType ac, ITxn *txn, SharedPtr *tag) {
  auto parent = stack ? stack->node_ : nullptr;
  RC rc = RC::OK;
  if (SplitSafe(child)) {
    // insert into child and return
    auto ptr = child->SafeInsert(key, val, ac, txn, this);
    if (child->IsLeaf()) {
      if (!ptr) {
        rc = RC::ABORT;
      } else if (tag) {
        tag->InitFrom(*ptr);
      }
    }
    // delete remaining stack
    FreeStack(full_stack);
  } else {
    auto ptr = child->SafeInsert(key, val, ac, txn, this);
    if (child->IsLeaf() && !ptr) {
      rc = RC::ABORT;
      FreeStack(full_stack);
      return rc;
    }
    // split child and insert new key separator into parent
    auto split_pos = child->key_cnt_ / 2;
    auto split_key = child->keys_[split_pos];
    assert(split_pos - 1 >= 0);
    assert(split_pos + 1 < child->key_cnt_);
    auto new_node = AllocNewNode();
    new_node->level_ = child->level_;
    new_node->left_bound_ = split_key;
    new_node->right_bound_ = child->right_bound_;
    child->right_bound_ = split_key;
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
      new_node->left_bound_in_storage_ = false;
      new_node->next_ = child->next_;
      new_node->prev_ = child;
      child->next_ = new_node;
      auto next = const_cast<BTreeObjNode *>(new_node->next_);
      if (next) {
        next->latch_.LockEX();
        next->prev_ = new_node;
        next->latch_.Unlock();
      }
    } else {
      memmove(&new_node->children_[0], &child->children_[split_pos + 1],
              (num_ele + 1) * sizeof(SharedPtr));
    }
    child->key_cnt_ = split_pos;
    new_node->key_cnt_ = num_ele;
    if (tag && child->IsLeaf()) {
      auto ptr = ScanLeafNode(key < new_node->keys_[0] ? child : new_node, key);
      tag->InitFrom(*ptr);
    }
    if (IsRoot(child)) {
      // split root by adding a new root
      auto new_root = AllocNewNode();
      new_root->level_ = child->level_ + 1;
      new_root->children_[0].Init(child);
      new_root->latch_.LockEX();
      new_root->SafeInsert(split_key, new_node, ac, txn, this);
      new_root->latch_.Unlock(); // debug
      height_++;
      // this line must happen in the end to avoid others touching new root
      root_.Init(new_root);
      FreeStack(full_stack);
      return rc;
    }
    // insert into parent, parent may still be null even child cannot be root
    // in case it is split safe and not added!
    if (stack) {
      stack = stack->parent_;
      RecursiveInsert(stack, split_key, new_node, parent, full_stack, ac,
                      txn, tag);
    }
  }
  return rc; // only leaf node can abort so no need to check rc for recursive calls
}

OID IndexBTreeObject::ScanBranchNode(BTreeObjNode *node, SearchKey key) {
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

BTreeObjNode * IndexBTreeObject::AllocNewNode() {
  if (node_sz_ == 0) {
    node_sz_ = sizeof(BTreeObjNode) + sizeof(SearchKey) * fanout_ +
        sizeof(SharedPtr) * (fanout_ + 1);
  }
  // object buffer track the memory usage
  ((ObjectBufferManager *) g_buf_mgr)->AllocSpace(node_sz_);
  auto new_node = new (MemoryAllocator::Alloc(node_sz_)) BTreeObjNode();
  new_node->keys_ = (SearchKey *) ((char *) new_node + sizeof(BTreeObjNode));
  new_node->children_ = (SharedPtr *) ((char *) new_node->keys_ +
      sizeof(SearchKey) * fanout_);
  for (int i = 0; i < fanout_ + 1; i++) {
    new (&new_node->children_[i]) SharedPtr();
  }
  new_node->id_ = num_nodes_.fetch_add(1);
  return new_node;
}

SharedPtr* BTreeObjNode::SafeInsert(SearchKey &key, void * val, AccessType ac,
                                    ITxn *txn, IndexBTreeObject *tree) {
  M_ASSERT(latch_.IsEXLocked(), "ex lock not hold!");
  // find the right insert position
  OID idx;
  for (idx = 0; idx < key_cnt_; idx++) {
    if (keys_[idx] > key) {
      break;
    }
    if (!txn && level_ == 0 && keys_[idx] == key) {
      // if not insert due to txn,
      // do not insert on duplicated tuple and return the existing tuple
      // LOG_DEBUG("key %lu already exist, do not insert", key.ToUInt64());
      return &children_[idx + 1];
    }
  }
  // if leaf node, acquire lock and register idx access in txn
  if (IsLeaf() && txn) {
    // each tuple lock from self (inclusive) to next key
    IdxAccess idx_ac = {ac, tree->GetTableId()};
    auto & lock = idx != 0 ?
        reinterpret_cast<ITuple *>(children_[idx].Get())->next_key_lock_ :
        left_bound_lock_;
    auto rc = CCManager::AcquireLock(lock, ac != AccessType::READ && ac != SCAN);
    // record access info in the txn on success
    if (rc == RC::OK) {
      idx_ac.locks_.push_back(&lock); // TODO: check address reference correctness.
    } else {
      return nullptr; // TODO: handle abort in upper level.
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
    auto tuple = reinterpret_cast<ITuple *>(val);
    // insert at idx, then need to check keys_[idx - 1]'s ptr
    // which is at children[idx].
    tuple->next_key_in_storage_ = idx == 0 ? left_bound_in_storage_ :
        reinterpret_cast<ITuple *>(children_[idx].Get())->next_key_in_storage_;
    children_[idx + 1].Init(tuple);
  } else {
    children_[idx + 1].Init(reinterpret_cast<BTreeObjNode *>(val));
  }
  key_cnt_++;
  return &children_[idx + 1];
}

bool BTreeObjNode::SafeDelete(SearchKey &key, ITuple *tuple) {
  M_ASSERT(latch_.IsEXLocked(), "ex lock not hold!");
  bool empty = false;
  OID idx;
  if (IsLeaf()) {
    for (idx = 0; idx < key_cnt_; idx++) {
      if (key == keys_[idx] && children_[idx + 1].Get() == tuple) {
        break;
      }
    }
    if (idx >= key_cnt_) {
      LOG_ERROR("Key to delete (%lu) does not exist in leaf", key.ToUInt64());
      return false;
    }
    // must dealloc here. otherwise memmove will overwrite
    children_[idx + 1].Free();
    if (idx != 0) {
      reinterpret_cast<ITuple *>(children_[idx].Get())->next_key_in_storage_ = true;
    } else {
      left_bound_in_storage_ = true;
    }
    auto shift_num = key_cnt_ - idx - 1;
    if (shift_num > 0) {
      memmove(&keys_[idx], &keys_[idx + 1],
              sizeof(SearchKey) * shift_num);
      memmove(&children_[idx + 1], &children_[idx + 2],
              sizeof(SharedPtr) * shift_num);
    }
    key_cnt_--;
    empty = (key_cnt_ == 0);
  } else {
    for (idx = 0; idx < key_cnt_; idx++) {
      if (key < keys_[idx]) break;
    }
    // delete right key
    children_[idx].Free();
    // move keys
    auto shift_num = key_cnt_ > idx ? key_cnt_ - idx - 1 : 0;
    if (shift_num > 0) {
      memmove(&keys_[idx], &keys_[idx + 1],
              sizeof(SearchKey) * shift_num);
    }
    // move ptrs
    shift_num = key_cnt_ - idx;
    if (shift_num > 0) {
      memmove(&children_[idx], &children_[idx + 1],
              sizeof(SharedPtr) * shift_num);
    }
    if (key_cnt_ == 0) {
      empty = true;
    } else {
      key_cnt_--;
    }
  }
  return empty;
}

void IndexBTreeObject::Delete(SearchKey key, ITuple * tuple) {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::DELETE, parent, child);
  // the last two layers are EX locked, see if child and parent will split
  if (!DeleteSafe(child) && !DeleteSafe(parent)) {
    // restart with all exclusive lock from non-split safe point.
    child->latch_.Unlock();
    parent->latch_.Unlock();
    // LOG_DEBUG("may split more than 2 levels, start pessimistic search!");
    // PrintTree();
    auto stack = PessimisticSearch(key, parent, child, DELETE);
    // insert bottom-up recursively with a stack.
    RecursiveDelete(stack, key, child, tuple, stack);
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    RecursiveDelete(stack, key, child, tuple, stack);
  }
  // LOG_DEBUG("deleted key %lu tuple %p", key.ToUInt64(), tuple);
  child->latch_.Unlock();
}

void IndexBTreeObject::RecursiveDelete(IndexBTreeObject::Stack stack,
                                       SearchKey key, BTreeObjNode *child,
                                       ITuple * tuple,
                                       IndexBTreeObject::Stack full_stack) {
  auto parent = stack ? stack->node_ : nullptr;
  if (DeleteSafe(child)) {
    child->SafeDelete(key, tuple);
    FreeStack(full_stack);
  } else {
    bool empty = child->SafeDelete(key, tuple);
    // insert into parent, parent may still be null even child cannot be root
    // in case it is split safe and not added!
    if (empty) {
      // update prev and next for leafs
      if (child->IsLeaf()) {
        auto prev = const_cast<BTreeObjNode *>(child->prev_);
        if (prev) {
          while (true) {
            child->latch_.Unlock(); // avoid deadlock
            prev->latch_.Lock(true);
            child->latch_.Lock(true);
            if (child->prev_ == prev) {
              prev->next_ = child->next_;
              prev->latch_.Unlock();
              break;
            } else {
              prev->latch_.Unlock();
              prev = const_cast<BTreeObjNode *>(child->prev_);
              if (!prev) break; // someone may delete prev node.
            }
          }
        }
        auto next = const_cast<BTreeObjNode *>(child->next_);
        if (next) {
          next->latch_.Lock(true);
          child->next_->prev_ = prev;
          next->left_bound_ = child->left_bound_;
          next->latch_.Unlock();
        }
      }
      // dealloc node
      ((ObjectBufferManager *)g_buf_mgr)->DeallocSpace(node_sz_);
      if (stack) {
        stack = stack->parent_;
        RecursiveDelete(stack, key, parent, nullptr, full_stack);
      }
    } else {
      FreeStack(full_stack);
    }
  }
}

void IndexBTreeObject::PrintNode(BTreeObjNode * node) {
  std::cout << "node-" << node->id_ << " (level=" << node->level_
            << ", #keys=" << node->key_cnt_ << ")" << std::endl;
  if (!node->IsLeaf() && node->key_cnt_ == 0) {
    auto child = AccessNode(node->children_[0]);
    std::cout << "\t[ptr=node-" << child->id_ << "]" << std::endl;
    return;
  }
  for (auto i = 0; i < node->key_cnt_; i++) {
    if (node->IsLeaf()) {
      std::cout << "\t(key=" << node->keys_[i].ToString() << ", row=" <<
                node->children_[i+1].Get() << "), ";
      continue;
    } else {
      if (i == 0) {
        auto child = AccessNode(node->children_[0]);
        std::cout << "\t[ptr=node-" << child->id_;
      }
      auto child = AccessNode(node->children_[i + 1]);
      std::cout << ", key=" << node->keys_[i].ToString() << ", ptr=node-"
                << child->id_;
      if (i == node->key_cnt_ - 1)
        std::cout << "]";
    }
  }
  std::cout << std::endl;
}

bool IndexBTreeObject::PrintTree(ITuple * p_tuple) {
  bool found = false;
  LOG_DEBUG("==== Print Tree ====");
  std::queue<BTreeObjNode *> queue;
  auto node = AccessNode(root_);
  queue.push(node);
  while (!queue.empty()) {
    node = queue.front();
    queue.pop();
    if (!node)
      continue;
    std::cout << "node-" << node->id_ << " (level=" << node->level_
              << ", #keys=" << node->key_cnt_ << ")" << std::endl;
    if (!node->IsLeaf() && node->key_cnt_ == 0) {
      auto child = AccessNode(node->children_[0]);
      std::cout << "\t[ptr=node-" << child->id_ << "]" << std::endl;
      queue.push(child);
      continue;
    }
    for (auto i = 0; i < node->key_cnt_; i++) {
      if (node->IsLeaf()) {
        std::cout << "\t(key=" << node->keys_[i].ToString() << ", row=" <<
                  node->children_[i+1].Get() << "), ";
        if (p_tuple && node->children_[i+1].Get() == p_tuple)
          found = true;
        continue;
      } else {
        if (i == 0) {
          auto child = AccessNode(node->children_[0]);
          std::cout << "\t[ptr=node-" << child->id_;
          queue.push(child);
        }
        auto child = AccessNode(node->children_[i + 1]);
        std::cout << ", key=" << node->keys_[i].ToString() << ", ptr=node-"
                  << child->id_;
        if (i == node->key_cnt_ - 1)
          std::cout << "]";
        queue.push(child);
      }
    }
    std::cout << std::endl;
  }
  LOG_DEBUG("==== End Print ====");
  return found;
}

SharedPtr *IndexBTreeObject::ScanLeafNode(BTreeObjNode *node, SearchKey key) {
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      break;
    } else if (key == node->keys_[i]) {
      return &node->children_[i + 1];
    }
  }
  return nullptr;
}

void IndexBTreeObject::FreeStack(IndexBTreeObject::Stack full_stack) {
  auto stack = full_stack;
  while (stack) {
    if (stack->node_) stack->node_->latch_.Unlock();
    auto used_stack = stack;
    stack = stack->parent_;
    DEALLOC(used_stack);
  }
}

void IndexBTreeObject::MarkRightBoundAvailInStorage() {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  SearchKey key(UINT64_MAX - 1);
  Search(key, AccessType::READ, parent, child);
  auto tuple = reinterpret_cast<ITuple *>(child->children_[child->key_cnt_].Get());
  tuple->next_key_in_storage_ = true;
  child->latch_.Unlock();
}

} // arboretum