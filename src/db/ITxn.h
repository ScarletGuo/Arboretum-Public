//
// Created by Zhihan Guo on 4/1/23.
//

#ifndef ARBORETUM_SRC_DB_ITXN_H_
#define ARBORETUM_SRC_DB_ITXN_H_

#include "common/Common.h"
#include "local/ITuple.h"
#include "local/ITable.h"
#include "db/ARDB.h"

namespace arboretum {

struct WriteCopy {
  explicit WriteCopy(size_t sz) : sz_(sz) {
    data_ = static_cast<char *>(MemoryAllocator::Alloc(sz));
  };
  ~WriteCopy() { DEALLOC(data_); };
  void Copy(void * src) const { memcpy(data_, src, sz_); };
  char *data_{nullptr}; // copy-on-write
  size_t sz_{0};
};

struct Access {
  AccessType ac_{READ};
  SearchKey key_{};
  ITable * tbl_{nullptr};
  SharedPtr *rows_{nullptr};
  int num_rows_{0};
  WriteCopy *data_{nullptr};
  // needs to be dynamically allocated! vector may resize and change the address
  volatile RC *status_{nullptr};
  bool free_ptr{true};
};

struct IdxAccess {
  AccessType ac_{READ};
  OID tbl_id_{0};
  std::vector<std::atomic<uint32_t> *> locks_;
};

class ITxn {
 public:
  ITxn(OID txn_id, ARDB * db, ITxn *txn) : txn_id_(txn_id), db_(db) {
    starttime_ = GetSystemClock();
    if (!txn) {
      txn_starttime_ = starttime_;
    }
  };
  OID GetTxnId() const { return txn_id_; };
  uint64_t GetStartTime() const { return txn_starttime_; };
  void Abort();
  void PreCommit();
  RC Commit();
  RC GetTuple(ITable *p_table, OID idx_id, SearchKey key, AccessType ac,
              char *&data, size_t &size);
  RC GetTuples(ITable *p_table, OID idx_id, SearchKey low_key, SearchKey
  high_key);
  RC InsertTuple(SearchKey& pkey, ITable *p_table, char *data, size_t sz);

 public:
  std::vector<Access> tuple_accesses_;
  std::vector<IdxAccess> idx_accesses_;

 private:
  RC AccessTuple(SharedPtr *result, ITable * p_table, uint64_t expected_key,
                 AccessType ac_type, char *&data, size_t &sz);
  static void FinishAccess(SharedPtr &ptr, bool dirty = false, bool batch_begin = false);
  OID txn_id_{0};
  ARDB * db_;
  uint64_t txn_starttime_; // total execution, including abort time
  uint64_t starttime_; // current execution
  uint64_t lsn_;
};
}

#endif //ARBORETUM_SRC_DB_ITXN_H_
