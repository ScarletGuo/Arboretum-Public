//
// Created by Zhihan Guo on 3/30/23.
//

#ifndef ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_
#define ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_

#include "common/ThreadPool.h"
#include "common/Common.h"

namespace arboretum {

class ITable;
class ISchema;
class ITxn;

class ARDB {

 public:
  ARDB();

  OID CreateTable(std::string tbl_name, ISchema * schema); // will use pkey as index
  OID CreateIndex(OID tbl_id, OID col, IndexType tpe);
  void BatchInitInsert(OID tbl, OID partition, std::multimap<std::string, std::string> &map);
  void InitInsert(OID tbl, char * data, size_t sz);
  // transactional
  RC InsertTuple(SearchKey& pkey, OID tbl, char * data, size_t sz, ITxn * txn);
  RC GetTuple(OID tbl, OID idx_id, SearchKey key,
              char *& data, size_t &sz, AccessType ac, ITxn * txn);
  RC GetTuples(OID tbl, OID idx_id, SearchKey low_key, SearchKey high_key, ITxn * txn);
  // non-transactional
  RC GetTuple(OID tbl_id, SearchKey key, std::string &data);
  ITxn * StartTxn(ITxn *txn = nullptr);
  void Terminate(std::vector<std::thread> & threads);
  RC CommitTxn(ITxn * txn, RC rc);
  static void CommitTask(ITxn * txn);

  ITable *GetTable(OID i) { return tables_[i]; };
  static void LoadConfig(int i, char **p_string);
  static bool CheckBufferWarmedUp(uint64_t i);
  static double RandDouble();
  static void InitRand(uint32_t thd_id) { rand_generator_ = new std::mt19937(thd_id); }

  void FinishLoadingData(OID tbl_id);
  void WaitForAsyncBatchLoading(int cnt);
  void RestoreTable(OID tbl_id, uint64_t i);

  size_t GetTotalPgCnt();
  void FinishWarmupCache();
 private:
  size_t table_cnt_{0};
  std::vector<ITable *> tables_;
  ThreadPool<std::function<void(ITxn *)>, ITxn> commit_thd_pool_;
  static __thread std::mt19937 * rand_generator_;
};

} // arboretum


#endif //ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_
