//
// Created by Zhihan Guo on 8/15/23.
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

OID ITable::CreateIndex(OID col, IndexType tpe) {
  switch (tpe) {
    case REMOTE:
      break;
    case IndexType::BTREE:
      if (g_buf_type == OBJBUF) {
        auto index = NEW(IndexBTreeObject)(tbl_id_);
        indexes_[index->GetIndexId()] = index;
        index->AddCoveringCol(col);
        return index->GetIndexId();
      } else {
        auto index = NEW(IndexBTreePage)(tbl_id_);
        indexes_[index->GetIndexId()] = index;
        index->AddCoveringCol(col);
        return index->GetIndexId();
      }
  }
  // for only support non-composite key
  return 0;
}

void ITable::RestoreTable(uint64_t num_rows) {
  row_cnt_ = g_data_store->Read(GetStorageId(), "metadata", "row_cnt_");
  if (row_cnt_ != num_rows) {
    row_cnt_ = num_rows;
    g_data_store->Write(GetStorageId(), "metadata", "row_cnt_", row_cnt_);
  }
  LOG_DEBUG("restored table %s with %lu rows", GetStorageId().c_str(), row_cnt_.load());
  if (g_buf_type == PGBUF) {
    pg_cnt_ = g_data_store->Read(GetStorageId(), "metadata", "pg_cnt_");
    for (auto & pair : indexes_) {
      pair.second->Load(pg_cnt_);
    }
    LOG_DEBUG("restored table %s with %lu pages", GetStorageId().c_str(), pg_cnt_.load());
  }
  if (g_load_to_remote_only) return;
  for (size_t i = lock_tbl_.size(); i < row_cnt_; i++) {
    lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
  }
}

void ITable::FinishWarmupCache() {
  if (g_buf_type == OBJBUF && g_restore_from_remote) {
    // after loading need to mark the left flag as not in storage
    for (auto &pairs: indexes_) {
      ((IndexBTreeObject *) pairs.second)->MarkRightBoundAvailInStorage();
    }
  }
}

}