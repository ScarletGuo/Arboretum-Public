//
// Created by Zhihan Guo on 8/15/23.
//

#include "YCSBWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "db/buffer/PageBufferManager.h"
#include "db/buffer/ObjectBufferManager.h"

namespace arboretum {

RC YCSBWorkload::RWTxn(YCSBQuery * query, ITxn *txn) {
  RC rc = RC::OK;
  for (size_t i = 0; i < query->req_cnt; i++) {
    YCSBRequest & req = query->requests[i];
    char * data;
    size_t size;
    rc = db_->GetTuple(tables_["MAIN_TABLE"], indexes_[0],
                       SearchKey(req.key),data, size, req.ac_type, txn);
    if (rc == ABORT)
      break;
    if (req.ac_type == UPDATE) {
      auto schema = schemas_[0];
      for (size_t cid = 1; cid < schema->GetColumnCnt(); cid++) {
        schema->SetNumericField(cid, data, txn->GetTxnId());
      }
    }
  }
  return rc;
}

RC YCSBWorkload::InsertTxn(YCSBQuery * query, ITxn *txn) {
  auto &req = query->requests[0];
  char data[schemas_[0]->GetTupleSz()];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "insert");
  schemas_[0]->SetPrimaryKey((char *) data, req.key);
  SearchKey pkey(req.key);
  return db_->InsertTuple(pkey, tables_["MAIN_TABLE"], data,
                          schemas_[0]->GetTupleSz(), txn);
}

RC YCSBWorkload::ScanTxn(YCSBQuery * query, ITxn *txn) {
  YCSBRequest & low_req = query->requests[0];
  YCSBRequest & high_req = query->requests[1];
  return db_->GetTuples(tables_["MAIN_TABLE"], indexes_[0],
                        SearchKey(low_req.key),
                        SearchKey(high_req.key),
                        txn);
}

}