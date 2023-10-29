//
// Created by Zhihan Guo on 6/22/23.
//

#include "ARDB.h"
#include "ITxn.h"
#include "buffer/ObjectBufferManager.h"
#include "buffer/PageBufferManager.h"
#include "remote/IDataStore.h"
#include "local/ITable.h"

namespace arboretum {

double ARDB::RandDouble() {
  std::uniform_real_distribution<double> distribution(0, 1);
  return distribution(*rand_generator_);
}

bool ARDB::CheckBufferWarmedUp(uint64_t i) {
  if (g_buf_type == NOBUF) {
    return true;
  } else if (g_buf_type == OBJBUF) {
    return ((ObjectBufferManager *) g_buf_mgr)->IsWarmedUp(i);
  } else if (g_buf_type == PGBUF) {
    return ((PageBufferManager *) g_buf_mgr)->IsWarmedUp(i, true);
  }
  return false;
}

void ARDB::BatchInitInsert(OID tbl, OID partition, std::multimap<std::string, std::string> &map) {
  g_data_store->WriteBatch(tables_[tbl]->GetStorageId(), std::to_string(partition), map);
}

void ARDB::InitInsert(OID tbl, char *data, size_t sz) {
  tables_[tbl]->InitInsertTuple(data, sz);
}

void ARDB::FinishLoadingData(OID tbl_id) {
  tables_[tbl_id]->FinishLoadingData();
}

void ARDB::WaitForAsyncBatchLoading(int cnt) {
  g_data_store->WaitForAsyncBatchLoading(cnt);
}

void ARDB::CommitTask(ITxn * txn) {
  txn->Commit();
  DEALLOC(txn);
};

size_t ARDB::GetTotalPgCnt() {
  size_t cnt = 0;
  for (auto & tbl : tables_) {
    cnt += tbl->GetPgCnt();
  }
  return cnt;
}

void ARDB::FinishWarmupCache() {
  for (auto & tbl : tables_) {
    tbl->FinishWarmupCache();
  }
}

}
