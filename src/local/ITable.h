//
// Created by Zhihan Guo on 3/30/23.
//

#ifndef ARBORETUM_SRC_LOCAL_ITABLE_H_
#define ARBORETUM_SRC_LOCAL_ITABLE_H_


#include <utility>

#include "common/Common.h"
#include "IPage.h"
#include "ITuple.h"


namespace arboretum {

class IIndex;
class ITxn;
class ITable {

 public:
  ITable(std::string tbl_name, OID tbl_id, ISchema * schema);
  void RestoreTable(uint64_t num_rows);
  OID CreateIndex(OID col, IndexType tpe);
  void InitInsertTuple(char *data, size_t sz);

  SharedPtr* IndexSearch(SearchKey key, OID idx_id, ITxn *txn, RC &rc);
  SharedPtr * IndexRangeSearch(SearchKey low_key, SearchKey high_key, OID
  idx_id, ITxn *txn, RC &rc, size_t &cnt);
  RC InsertTuple(SearchKey& pkey, char *data, size_t sz, ITxn *txn);
  void IndexDelete(ITuple *tuple);

  inline ISchema *GetSchema() { return schema_; };
  inline OID GetTableId() const { return tbl_id_; };
  inline std::string GetStorageId() const { return storage_id_; };
  static std::string GetStorageId(OID tbl_id) {
    std::string id = g_dataset_id;
    // Index with phantom protection is directed to a different table.
    if (tbl_id >= (1 << 28)) {
      id = "page-wl" + id.substr(4, id.size() - 4);
    }
    return id + "-" + std::to_string(tbl_id); };
  inline size_t GetTotalTupleSize() {
    return schema_->tuple_sz_ + sizeof(ITuple);
  }; // including size of metadata
  inline size_t GetTupleDataSize() { return schema_->tuple_sz_; };
  std::atomic<uint32_t>& GetLockState(OID tid) { return *lock_tbl_[tid]; };
  void FinishLoadingData();
  size_t GetPgCnt();

  void FinishWarmupCache();
 private:
  void InitIndexInsert(ITuple * p_tuple, OID pg_id, OID offset);
  std::string tbl_name_;
  std::string storage_id_;
  OID tbl_id_{0};
  std::atomic<size_t> row_cnt_{0};
  std::atomic<size_t> pg_cnt_{1};
  std::unordered_map<OID, IIndex *> indexes_;
  BasePage * pg_cursor_{nullptr};
  ISchema * schema_;
  std::vector<std::atomic<uint32_t>*> lock_tbl_; // each lock: 1-bit EX lock, 31-bit ref cnt
};

} // arboretum

#endif //ARBORETUM_SRC_LOCAL_ITABLE_H_
