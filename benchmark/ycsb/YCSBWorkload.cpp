//
// Created by Zhihan Guo on 3/31/23.
//

#include <cmath>
#include "YCSBWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "db/buffer/PageBufferManager.h"
#include "db/buffer/ObjectBufferManager.h"

namespace arboretum {

YCSBWorkload::YCSBWorkload(ARDB *db, YCSBConfig *config) : Workload(db) {
  config_ = config;
  std::istringstream in(YCSB_schema_string);
  InitSchema(in);
  LoadData();
  CheckData();
  CalculateDenom();
}

void YCSBWorkload::Execute(YCSBWorkload *workload, BenchWorker *worker) {
  arboretum::Worker::SetThdId(worker->worker_id_);
  arboretum::Worker::SetMaxThdTxnId(0);
  arboretum::ARDB::InitRand(worker->worker_id_);
  LOG_INFO("Thread-%u starts execution!", arboretum::Worker::GetThdId());
  YCSBQuery * query;
  YCSBWorkload::QueryType tpe;
  ITxn * txn = nullptr;
  RC rc = RC::OK;
  uint64_t starttime;
  while (!g_terminate_exec) {
    if (rc != ABORT) {
      // generate new query
      txn = nullptr;
      double r = ARDB::RandDouble();
      if (r < workload->config_->rw_txn_perc_) {
        query = workload->GenRWQuery();
        tpe = YCSB_RW;
      } else if (r < workload->config_->insert_txn_perc_ + workload->config_->rw_txn_perc_) {
        query = workload->GenInsertQuery();
        tpe = YCSB_INSERT;
      } else {
        query = workload->GenScanQuery();
        tpe = YCSB_SCAN;
      }
    }
    starttime = GetSystemClock();
    txn = workload->db_->StartTxn(txn);
    if (tpe == YCSB_RW) {
      rc = workload->RWTxn(query, txn);
    } else if (tpe == YCSB_INSERT) {
      rc = workload->InsertTxn(query, txn);
    } else {
      rc = workload->ScanTxn(query, txn);
    }
    workload->db_->CommitTxn(txn, rc);
    // stats
    if (g_warmup_finished) {
      auto endtime = GetSystemClock();
      auto latency = endtime - starttime;
      worker->bench_stats_.incr_thd_runtime_(latency);
      if (rc == RC::OK) {
        auto user_latency = endtime - txn->GetStartTime();
        switch(tpe) {
          case YCSB_RW:
            worker->bench_stats_.incr_rw_commit_cnt_(1);
            worker->bench_stats_.incr_rw_txn_latency_(latency);
            worker->bench_stats_.incr_rw_txn_user_latency_(user_latency);
            break;
          case YCSB_SCAN:
            worker->bench_stats_.incr_scan_commit_cnt_(1);
            worker->bench_stats_.incr_scan_txn_latency_(latency);
            worker->bench_stats_.incr_scan_txn_user_latency_(user_latency);
            break;
          case YCSB_INSERT:
            worker->bench_stats_.incr_insert_commit_cnt_(1);
            worker->bench_stats_.incr_insert_txn_latency_(latency);
            worker->bench_stats_.incr_insert_txn_user_latency_(user_latency);
            break;
        }
      }
    }
  }
  LOG_INFO("Thread-%u finishes execution!", Worker::GetThdId());
  // summarize stats
  worker->SumUp();
}

void YCSBWorkload::LoadData() {
  if (g_restore_from_remote) {
    WarmupCache();
    db_->RestoreTable(tables_["MAIN_TABLE"], config_->num_rows_);
    LOG_INFO("Finished warming up cache for YCSB workload");
  } else {
    Load();
    // if (g_load_to_remote_only && g_buf_type != PGBUF) BatchLoad();
    LOG_INFO("Finished loading YCSB workload");
  }
}

void YCSBWorkload::WarmupCache() {
  if (g_buf_type == NOBUF || g_buf_type == PGBUF) {
    return;
  } else if (g_buf_type == OBJBUF) {
    auto sz = schemas_[0]->GetTupleSz();
    LOG_DEBUG("tuple size = %u", sz);
    char data[sz];
    strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
    auto allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
    auto num_slots = ((ObjectBufferManager *)g_buf_mgr)->GetBufferSize();
    for (int64_t key = 0; allocated < num_slots && key < config_->num_rows_; key++) {
      if (key % (num_slots / 10) == 0) {
        LOG_DEBUG("warm up status: %ld / %lu", allocated, num_slots);
      }
      schemas_[0]->SetPrimaryKey(data, key);
      db_->InitInsert(tables_["MAIN_TABLE"], data, sz);
      allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
    }
    db_->FinishWarmupCache();
    LOG_INFO("Loaded %lu rows in memory", allocated);
  }
}

void YCSBWorkload::BatchLoad() {
  if (!g_load_range) {
    config_->loading_startkey = 0;
    config_->loading_endkey = config_->num_rows_;
  }
  LOG_DEBUG("Start loading data from %ld to %ld", config_->loading_startkey,
            config_->loading_endkey);

  // prepare tuple data
  auto sz = schemas_[0]->GetTupleSz();
  char data[sz];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  // set up batch
  size_t batch_sz = 5;
  std::multimap<std::string, std::string> batch;
  auto prev_part = config_->loading_startkey / g_partition_sz;
  auto part = prev_part;
  auto num_batches = 0;
  auto num_parallel = 4;
  int64_t progress = 0;
  // start insertion
  for (int64_t key = config_->loading_startkey; key < config_->loading_endkey; key++) {
    part = key / g_partition_sz;
    if (part != prev_part || (progress % batch_sz == 0 && progress != 0)) {
      // flush batch
      db_->BatchInitInsert(tables_["MAIN_TABLE"], part, batch);
      batch.clear();
      prev_part = part;
      num_batches++;
      // block until num_parallel batches are flushed
      if (num_batches % num_parallel == 0) {
        db_->WaitForAsyncBatchLoading(num_parallel);
        LOG_DEBUG("Loading progress: %ld / %ld (current key = %ld) ",
                  progress, config_->loading_endkey - config_->loading_startkey, key);
      }
    }
    // set primary key
    schemas_[0]->SetPrimaryKey(data, key);
    // insert tuple into current batch
    batch.insert({SearchKey(key).ToString(), std::string(data, sz)});
    progress++;
  }
  if (!batch.empty()) {
    db_->BatchInitInsert(tables_["MAIN_TABLE"], part, batch);
  }
  // if not loading partial ranges, flush metadata
  if (!g_load_range) {
    db_->FinishLoadingData(tables_["MAIN_TABLE"]);
  }
  LOG_DEBUG("Finish loading data from %ld to %ld", config_->loading_startkey,
            config_->loading_endkey);
}

void YCSBWorkload::Load() {
  char data[schemas_[0]->GetTupleSz()];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  auto progress = 0;
  for (int64_t key = 0; (uint64_t) key < config_->num_rows_; key++) {
    if (key % (config_->num_rows_ / 100) == 0 && progress <= 100) {
      LOG_DEBUG("Loading progress: %3d %%", progress);
      progress++;
    }
    // set primary key
    schemas_[0]->SetPrimaryKey((char *) data, key);
    db_->InitInsert(tables_["MAIN_TABLE"], data,schemas_[0]->GetTupleSz());
  }
  db_->FinishLoadingData(tables_["MAIN_TABLE"]);
}


void YCSBWorkload::CheckData() {
  if (g_buf_type == PGBUF || !g_check_loaded) return;
  auto progress = 0;
  auto batch_size = 10;
  for (int64_t key = 0; (uint64_t) key < config_->num_rows_; key++) {
    if (key % (config_->num_rows_ / 100) == 0 && progress <= 100) {
      LOG_DEBUG("Checking progress: %3d %%", progress);
      progress++;
    }
    if (key % batch_size != 0)
      continue;
    // set primary key
    std::string data;
    auto rc = db_->GetTuple(tables_["MAIN_TABLE"], SearchKey(key), data);
    if (rc == RC::OK) {
      auto col = schemas_[0]->GetPKeyColIds()[0];
      auto val = &(data[schemas_[0]->GetFieldOffset(col)]);
      auto pkey = *((int64_t *) val);
      if (pkey != key) {
        rc = RC::ERROR;
        LOG_ERROR("check key %lu not match in storage %lu", key, pkey);
      }
    } else {
      LOG_DEBUG("checking key %ld not found", key);
    }
    if (rc != RC::OK) {
      config_->loading_startkey = key;
      config_->loading_endkey = key + batch_size;
      g_load_range = true;
      BatchLoad();
    }
  }
  LOG_INFO("Finished checking all loaded data for YCSB workload");
}

YCSBQuery *YCSBWorkload::GenRWQuery() {
  auto query = NEW(YCSBQuery);
  query->req_cnt = config_->num_req_per_query_;
  query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
  size_t cnt = 0;
  while (cnt < query->req_cnt) {
    GenRequest(query, cnt);
  }
  return query;
}

YCSBQuery *YCSBWorkload::GenScanQuery() {
  auto g_num_nodes = 1;
  auto node_id = 0;
  auto query = NEW(YCSBQuery);
  query->req_cnt = 2;
  query->requests = NEW_SZ(YCSBRequest, 2);
  int64_t scan_length = round(ARDB::RandDouble() * 99) + 1;
  // generate start key and end key
  // recommended zipf for 80:20 hot-cold ratio is 0.877
  uint64_t row_id = zipf(config_->num_rows_ - 1 - scan_length, config_->zipf_theta_);
  query->requests[0].key = row_id * g_num_nodes + node_id; // inclusive
  query->requests[0].ac_type = SCAN;
  // pick from scan length from 1 to 100. uniformly random.
  query->requests[1].key = query->requests[0].key + scan_length; // exclusive
  query->requests[1].ac_type = SCAN;
  return query;
}

YCSBQuery *YCSBWorkload::GenInsertQuery() {
  auto query = NEW(YCSBQuery);
  query->req_cnt = 1;
  query->requests = NEW_SZ(YCSBRequest, 1);
  query->requests[0].key = ++config_->num_rows_;
  query->requests[0].ac_type = INSERT;
  query->requests[0].value = 0;
  return query;
}

void YCSBWorkload::GenRequest(YCSBQuery * query, size_t &cnt) {
  // TODO: add (bool) remote and g_node_id info in the future
  YCSBRequest & req = query->requests[cnt];
  auto g_num_nodes = 1;
  auto node_id = 0;
  uint64_t row_id = zipf(config_->num_rows_ - 1, config_->zipf_theta_);
  uint64_t primary_key = row_id * g_num_nodes + node_id;
  bool readonly = row_id != 0 && (int((int32_t) row_id * config_->read_perc_) >
      int(((int32_t) row_id - 1) * config_->read_perc_));
  if (readonly)
    req.ac_type = READ;
  else {
    double r = ARDB::RandDouble();
    req.ac_type = (r < config_->read_perc_) ? READ : UPDATE;
  }
  req.key = primary_key;
  req.value = 0;
  // remove duplicates
  bool exist = false;
  for (uint32_t i = 0; i < cnt; i++)
    if (query->requests[i].key == req.key)
      exist = true;
  if (!exist)
    cnt++;
}

void YCSBWorkload::CalculateDenom() {
  assert(the_n == 0);
  uint64_t table_size = config_->num_rows_;
  M_ASSERT(table_size % g_num_worker_threads == 0, "Table size must be multiples of worker threads");
  the_n = table_size / g_num_worker_threads - 1;
  denom = zeta(the_n, config_->zipf_theta_);
  zeta_2_theta = zeta(2, config_->zipf_theta_);
}

uint64_t YCSBWorkload::zipf(uint64_t n, double theta) {
  assert(theta == config_->zipf_theta_);
  double alpha = 1 / (1 - theta);
  double zetan = denom;
  double eta = (1 - pow(2.0 / n, 1 - theta)) /
      (1 - zeta_2_theta / zetan);
  double u = ARDB::RandDouble();
  double uz = u * zetan;
  if (uz < 1) return 0;
  if (uz < 1 + pow(0.5, theta)) return 1;
  return (uint64_t) (n * pow(eta * u - eta + 1, alpha));
}

double YCSBWorkload::zeta(uint64_t n, double theta) {
  double sum = 0;
  for (uint64_t i = 1; i <= n; i++)
    sum += pow(1.0 / (int32_t) i, theta);
  return sum;
}

} // namespace