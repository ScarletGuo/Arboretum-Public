//
// Created by Zhihan Guo on 4/1/23.
//

#include "ITxn.h"
#include "CCManager.h"
#include "remote/IDataStore.h"
#include "remote/ILogStore.h"
#include "buffer/ObjectBufferManager.h"
#include "buffer/PageBufferManager.h"
#include "common/Worker.h"
#include "common/Stats.h"

namespace arboretum {
RC ITxn::InsertTuple(SearchKey& pkey, ITable * p_table, char * data, size_t sz) {
  auto starttime = GetSystemClock();
  auto rc = p_table->InsertTuple(pkey, data, sz, this);
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    g_stats->db_stats_[Worker::GetThdId()].incr_idx_time_(endtime - starttime);
  }
  return rc;
}

RC ITxn::GetTuples(ITable *p_table, OID idx_id, SearchKey low_key, SearchKey
high_key) {
  RC rc;
  auto starttime = GetSystemClock();
  // try to *read* the index to get tuple address
  size_t cnt = 0;
  auto result = p_table->IndexRangeSearch(low_key, high_key, idx_id,
                                          this, rc, cnt);
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    g_stats->db_stats_[Worker::GetThdId()].incr_idx_time_(endtime - starttime);
  }
  // abort on index accessing conflicts.
  if (rc == RC::ABORT) return rc;
  M_ASSERT(cnt > 0, "if not abort, result should not be empty.");
  for (size_t i = 0; i < cnt; i++) {
    char * data;
    size_t sz;
    if (!result[i].Get())
      LOG_ERROR("key %lu not found", low_key.ToUInt64() + i);
    rc = AccessTuple(&result[i], p_table, low_key.ToUInt64() + i,
                     SCAN, data, sz);
    if (rc == RC::ABORT) return rc;
  }
  tuple_accesses_[0].free_ptr = true; // scan allocates several shared ptrs all together
  return rc;
}

RC ITxn::GetTuple(ITable *p_table, OID idx_id, SearchKey key, AccessType ac_type,
                  char *&data, size_t &sz) {
  RC rc;
  auto starttime = GetSystemClock();
  // try to *read* the index to get tuple address
  auto result = p_table->IndexSearch(key, idx_id, this, rc);
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    g_stats->db_stats_[Worker::GetThdId()].incr_idx_time_(endtime - starttime);
  }
  // abort on index accessing conflicts.
  if (rc == RC::ABORT) return rc;
  return AccessTuple(result, p_table, key.ToUInt64(), ac_type, data, sz);
}

RC ITxn::AccessTuple(SharedPtr *result, ITable * p_table, uint64_t expected_key,
                     AccessType ac_type, char *&data, size_t &sz) {
  // access the tuple object (not the data)
  ITuple * tuple;
  if (g_buf_type == OBJBUF) {
    // pin the tuple
    tuple = arboretum::ObjectBufferManager::AccessTuple(*result, expected_key);
  } else if (g_buf_type == PGBUF) {
    // pin the page when accessing the tuple.
    tuple = ((PageBufferManager *) g_buf_mgr)->AccessTuple(p_table, *result, expected_key);
  } else {
    tuple = reinterpret_cast<ITuple *>(result->Get());
    g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
  }
  auto key = tuple->GetPrimaryKey(p_table->GetSchema());
  // M_ASSERT(key.ToUInt64() == expected_key, "key does not match!");
  // try to acquire lock for accessing the tuple
  auto starttime = GetSystemClock();
  auto rc = CCManager::AcquireLock(p_table->GetLockState(tuple->GetTID()), ac_type == UPDATE);
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_cc_time_(GetSystemClock() - starttime);
  }
  if (rc == ABORT) {
    // tuple is not added to access, must free here
    FinishAccess(*result, false);
    return rc;
  }
  // try to record the tuple access
  if (!result->Get()) {
    LOG_ERROR("null ptr!");
  }
  Access ac = {ac_type == UPDATE ? UPDATE : READ,
               key, p_table, result, 1};
  sz = p_table->GetTupleDataSize();
  if (ac_type == UPDATE) {
    // since limit = 1, write copy here is a single copy. for cases with more than
    // one rows sharing same key, here should call new for each of them
    auto write_copy = new WriteCopy(sz);
    write_copy->Copy(tuple->GetData());
    ac.data_ = write_copy;
    data = write_copy->data_;
    if (g_buf_type == NOBUF) {
      ac.status_ = NEW(volatile RC);
      *ac.status_ = RC::PENDING;
      // start flushing writes async if no buf.
      // Note: here assume updating is done. no performance-wise differences
      // as flushing after the actual updates.
      g_data_store->WriteAsync(p_table->GetStorageId(), key,
                               tuple->GetData(), sz, ac.status_);
    }
  } else {
    if (ac_type == SCAN) ac.free_ptr = false;
    data = tuple->GetData();
  }
  tuple_accesses_.push_back(ac);
  return rc;
}

void ITxn::Abort() {
  // release lock and dealloc space used for storing accesses
  for (const auto& access : idx_accesses_) {
    auto ac = (access.ac_ == READ || access.ac_ == SCAN) ? READ : UPDATE;
    for (auto & lk : access.locks_) {
      CCManager::ReleaseLock(*lk, ac);
    }
  }
  for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {
      auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
      auto lock_id = tuple->GetTID();
      // wait for async req to finish to avoid seg fault.
      if (access.status_) {
        while(*access.status_ == RC::PENDING) {};
        DEALLOC((RC *) access.status_);
      }
      // for now, always flush (line: 60) before unpin.
      FinishAccess(access.rows_[i], false, access.free_ptr);
      CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id), access.ac_);
      delete &access.data_[i];
    }
    // Free is already called above, here only dealloc shared ptr itself
    if (access.free_ptr)
      DEALLOC(access.rows_);
  }
  tuple_accesses_.clear();
  // collect stats
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_abort_cnt_(1);
  }
}

void ITxn::PreCommit() {
  std::stringstream to_flush;
  for (const auto& access : idx_accesses_) {
    auto ac = (access.ac_ == READ || access.ac_ == SCAN) ? READ : UPDATE;
    if (!g_early_lock_release && ac != READ)
      continue;
    for (auto & lk : access.locks_) {
      CCManager::ReleaseLock(*lk, ac);
    }
  }
  for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {
      auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
      if (access.ac_ == UPDATE) {
        tuple->SetData(access.data_[i].data_, access.data_[i].sz_);
        auto schema = db_->GetTable(tuple->GetTableId())->GetSchema();
        auto key = tuple->GetPrimaryKey(schema).ToString();
        if (g_buf_type != NOBUF) {
          to_flush << key << "," << std::string(
              reinterpret_cast<char *>(tuple), access.tbl_->GetTotalTupleSize())
                   << std::endl;
        }
      }
      if (g_early_lock_release || access.ac_ != UPDATE) {
        // release read locks (and write locks if using elr)
        if (g_buf_type == NOBUF && access.status_) {
          // need to wait until the data is flushed
          while (!g_terminate_exec && *access.status_ == RC::PENDING) {};
          M_ASSERT(*access.status_ != RC::ERROR || g_terminate_exec, "error in async request");
          if (!g_terminate_exec) {
            DEALLOC((RC *) access.status_);
          }
        }
        auto lock_id = tuple->GetTID();
        FinishAccess(access.rows_[i], access.ac_ == UPDATE, access.free_ptr); // unpin the data
        CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id), access.ac_);
      }
    }
  }
  if (g_buf_type == NOBUF && g_early_lock_release) {
    // no need to contain redo logs as write data will be forced.
    // if w/o elr, still need to contain write data since server may fail.
    to_flush << "committed";
  }
  auto log = to_flush.str();
  lsn_ = g_log_store->AppendToLogBuffer(GetTxnId(), log);
}

RC ITxn::Commit() {
  // wait for log to be flushed
  auto starttime = GetSystemClock();
  auto flushed = g_log_store->WaitForFlush(lsn_);
  // LOG_INFO("commit thread %u finished logging", Worker::GetCommitThdId());
  auto log_time = GetSystemClock() - starttime;
  // release locks
  for (const auto& access : idx_accesses_) {
    auto ac = (access.ac_ == READ || access.ac_ == SCAN) ? READ : UPDATE;
    if (ac == UPDATE && !g_early_lock_release) {
      for (auto & lk : access.locks_) {
        CCManager::ReleaseLock(*lk, ac);
      }
    }
  }
  for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {
      if (!g_early_lock_release && access.ac_ == UPDATE) {
        if (g_buf_type == NOBUF && access.status_) {
          // need to wait until the data is flushed
          while (!g_terminate_exec && *access.status_ == RC::PENDING) {};
          M_ASSERT(*access.status_ == RC::OK || g_terminate_exec, "error in async request");
          if (!g_terminate_exec) {
            DEALLOC((RC *) access.status_);
          }
        }
        auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
        auto lock_id = tuple->GetTID();
        FinishAccess(access.rows_[i], !g_force_write, access.free_ptr); // unpin the data
        CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id), access.ac_);
      }
      delete &access.data_[i];
    }
    // Free is already called, here only dealloc shared ptr itself
    if (access.free_ptr)
      DEALLOC(access.rows_);
  }
  tuple_accesses_.clear();
  // update stats
  if (flushed && g_warmup_finished && !g_terminate_exec) {
    // commit stats
    auto latency = GetSystemClock() - txn_starttime_;
    g_stats->commit_stats_[Worker::GetCommitThdId()].incr_commit_cnt_(1);
    g_stats->commit_stats_[Worker::GetCommitThdId()].incr_commit_latency_(latency);
    auto commit_cnt = g_stats->commit_stats_[Worker::GetCommitThdId()].commit_cnt_;
    if (commit_cnt % 1000 == 0) {
      LOG_DEBUG("commit thread %u finishes %lu txns", Worker::GetCommitThdId(), commit_cnt);
    }
    // log stats
    g_stats->commit_stats_[Worker::GetCommitThdId()].incr_log_latency_(log_time);
    g_stats->commit_stats_[Worker::GetCommitThdId()].incr_num_logs_(1);
  }
  return RC::OK;
}

void ITxn::FinishAccess(SharedPtr &ptr, bool dirty, bool batch_begin) {
  if (g_buf_type == OBJBUF) {
    arboretum::ObjectBufferManager::FinishAccessTuple(ptr, dirty);
  } else if (g_buf_type == PGBUF) {
    ((PageBufferManager *) g_buf_mgr)->FinishAccessTuple(ptr, dirty);
  } else{
    if (batch_begin) ptr.Free();
  }
}
}