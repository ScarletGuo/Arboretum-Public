//
// Created by Zhihan Guo on 4/3/23.
//

#ifndef ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_
#define ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_

#include "IIndex.h"

namespace arboretum {

class ITuple;
class ITxn;
class IndexBTreeObject;

struct BTreeObjNode {
  OID id_{0}; // for debug
  size_t level_{0};
  SearchKey *keys_{};
  SharedPtr *children_{}; // leaf node: reserve children_[0] for neighbor ptr
  size_t key_cnt_{0};
  RWLock latch_{};
  // two-way link in leaf nodes for faster scan
  volatile BTreeObjNode *next_{nullptr};
  volatile BTreeObjNode *prev_{nullptr};
  // left bound info
  SearchKey left_bound_{0};
  SearchKey right_bound_{UINT64_MAX};
  std::atomic<uint32_t> left_bound_lock_{0};
  bool left_bound_in_storage_{false};
  inline bool IsLeaf() const { return level_ == 0; };
  SharedPtr* SafeInsert(SearchKey &key, void *val, AccessType ac,
                        ITxn *txn, IndexBTreeObject* tree);
  bool SafeDelete(SearchKey &key, ITuple *tuple = nullptr);
};

class IndexBTreeObject : public IIndex {

  struct StackData {
    BTreeObjNode * node_{nullptr};
    struct StackData * parent_{nullptr};
  };
  typedef StackData *Stack;

 public:
  IndexBTreeObject(OID tbl_id);
  SharedPtr* RangeSearch(SearchKey start, SearchKey end, ITxn *txn,
                         std::vector<std::pair<SearchKey, SearchKey>> &fetch_req,
                         RC &rc, size_t &cnt);
  SharedPtr* Search(SearchKey key, ITxn *txn, RC &rc);
  RC Insert(SearchKey key, ITuple * tuple, AccessType ac, ITxn *txn,
            SharedPtr *tag = nullptr);
  void Delete(SearchKey key, ITuple *p_tuple);
  void Load(size_t pg_cnt) override {};
  bool PrintTree(ITuple * p_tuple=nullptr);
  void PrintNode(BTreeObjNode *node);
  void MarkRightBoundAvailInStorage();

 private:
  void Search(SearchKey key, AccessType ac, BTreeObjNode * &parent,
              BTreeObjNode * &child);
  Stack PessimisticSearch(SearchKey key, BTreeObjNode * &parent,
                          BTreeObjNode * &child, AccessType ac);
  RC RecursiveInsert(Stack stack, SearchKey key, void * val,
                             BTreeObjNode *child, Stack full_stack,
                             AccessType ac, ITxn *txn, SharedPtr *tag);
  void RecursiveDelete(Stack stack, SearchKey key, BTreeObjNode *child,
                       ITuple * tuple, Stack full_stack);
  static OID ScanBranchNode(BTreeObjNode * node, SearchKey key);
  static SharedPtr* ScanLeafNode(BTreeObjNode * node, SearchKey key);

  // helper functions
  BTreeObjNode * AllocNewNode();
  inline bool SplitSafe(BTreeObjNode *node, size_t insert_sz = 1) const {
    return !node || node->key_cnt_ + insert_sz <= split_bar_;
  }
  inline bool DeleteSafe(BTreeObjNode *node) const {
    return !node || node->key_cnt_ > 1;
  }
  inline bool IsRoot(BTreeObjNode *node) { return node == root_.Get(); };
  static inline BTreeObjNode * AccessNode(SharedPtr& ptr) {
    return reinterpret_cast<BTreeObjNode *>(ptr.Get());
  }
  static inline BTreeObjNode * AccessNode(SharedPtr& ptr, bool exclusive) {
    auto node = AccessNode(ptr);
    if (node) node->latch_.Lock(exclusive);
    return node;
  }
  void FreeStack(Stack full_stack);

  SharedPtr root_{};
  size_t fanout_{6};
  size_t height_{0};
  size_t split_bar_{0};
  std::atomic<size_t> num_nodes_{0};
  size_t node_sz_{0};

};
}

#endif //ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_
