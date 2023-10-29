//
// Created by Zhihan Guo on 3/30/23.
//

#include "IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"

namespace arboretum {

IDataStore::IDataStore() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  std::ifstream in(redis_config_fname);
  std::string line;
  auto cnt = 0;
  std::vector<std::string> lines;
  while (getline(in, line)) {
    if (line[0] == '#') continue;
    cnt++;
    lines.push_back(line);
  }
  client_ = NEW(RedisClient)(lines[1], std::stol(lines[2]), lines[0]);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_ = NEW(TiKVClient)();
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  client_ = NEW(AzureTableClient)(azure_conn_str);
#endif
}

int64_t IDataStore::CheckSize() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  return client_->CheckSize(g_granule_id);
#endif
  return -1;
}

RC IDataStore::Read(const std::string &tbl_name, SearchKey &key, std::string &data) {
  //TODO: change tbl id to str for all functions and change accordingly in redis & tikv.
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  auto rc = client_->LoadSync(tbl_name, partition, key.ToString(), data);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rd_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rds_(1);
  }
  return rc;
}

int64_t IDataStore::Read(const std::string &tbl_name, std::string part_key, std::string key) {
  return client_->LoadNumericSync(tbl_name, part_key, key);
}

RC IDataStore::Read(const std::string &tbl_name, std::string part_key, std::string key, std::string &data) {
  return client_->LoadSync(tbl_name, part_key, key, data);
}

void IDataStore::Write(const std::string &tbl_name, std::string part_key, std::string key, int64_t num) {
  client_->StoreNumericSync(tbl_name, part_key, key, num);
}

void IDataStore::Write(const std::string &tbl_name, std::string part_key, std::string key, char *data, size_t sz) {
  client_->StoreSync(tbl_name, part_key, key, data, sz);
}

void IDataStore::Write(const std::string &tbl_name, SearchKey &key, char *data, size_t sz) {
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->StoreSync(tbl_name, key.ToString(), data, sz);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  std::string value(data, sz);
  client_->StoreSync(tbl_name, key.ToString(), value);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  client_->StoreSync(tbl_name, partition, key.ToString(), data, sz);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_wr_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_wrs_(1);
  }
}

void IDataStore::WriteAndRead(const std::string &tbl_name, SearchKey &key, char *data, size_t sz,
                              SearchKey &load_key, std::string &load_data) {
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->StoreAndLoadSync(tbl_id, key, data, sz, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  std::string value(data, sz);
  client_->StoreSync(tbl_id, key, value);
  client_->LoadSync(load_tbl_id, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto store_part = std::to_string(key.ToUInt64() / g_partition_sz);
  auto load_part = std::to_string(load_key.ToUInt64() / g_partition_sz);
  client_->StoreAndLoadSync(tbl_name, store_part, key.ToString(), data, sz,
                            load_part, load_key.ToString(), load_data);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rw_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rws_(1);
  }
}

void IDataStore::WriteBatch(const std::string &tbl_name, std::string part,
                            std::multimap<std::string, std::string> &map) {
  // used only when data forcing is enabled
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->BatchStoreSync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->BatchStoreSync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  client_->BatchStoreSync(tbl_name, part, map);
#endif
}

void IDataStore::WriteBatchAsync(const std::string &tbl_name, std::string part,
                            std::multimap<std::string, std::string> &map,
                            volatile bool& is_done) {
  // used only when data forcing is enabled
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->BatchStoreASync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->BatchStoreASync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  client_->BatchStoreAsync(tbl_name, std::move(part), map, is_done);
#endif
}

void IDataStore::WriteAsync(const std::string &tbl_name, SearchKey &key,
                           char *data, size_t sz, volatile RC *rc) {
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  return client_->StoreAsync(tbl_name, partition, key.ToString(), data, sz, rc);
}

void IDataStore::ReadRange(std::string table_name, SearchKey low_key,
                           SearchKey high_key, char **data_ptrs) {
  auto starttime = GetSystemClock();
  auto low_part = std::to_string(low_key.ToUInt64() / g_partition_sz);
  auto high_part = std::to_string(high_key.ToUInt64() / g_partition_sz);
  auto cnt = high_key.ToUInt64() - low_key.ToUInt64() + 1;
  if (low_part == high_part) {
    // ReadRangeHelper(cnt, table_name, low_part, low_key, high_key, data_ptrs);
    client_->LoadRangePython(table_name, low_part, low_key.ToString(), high_key.ToString(), data_ptrs);
  } else {
    // split into two half (will not exceed two currently
    // as partition size is larger than max scan length)
    auto boundary_val = high_key.ToUInt64() / g_partition_sz * g_partition_sz;
    auto pos = boundary_val - low_key.ToUInt64();
    // [low_key, boundary - 1], [boundary, high_key]
    auto boundary = SearchKey(boundary_val - 1);
    client_->LoadRangePython(table_name, low_part,low_key.ToString(),
                             boundary.ToString(), data_ptrs);
    // ReadRangeHelper(pos, table_name, low_part, low_key, boundary, data_ptrs);
    boundary.SetValue(boundary_val, BIGINT);
    client_->LoadRangePython(table_name, high_part,boundary.ToString(),
                             high_key.ToString(), &data_ptrs[pos]);
    // ReadRangeHelper(cnt - pos, table_name, high_part, boundary, high_key, &data_ptrs[pos]);
  }
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scan_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scans_(1);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scan_sz_(cnt);
  }
}

} // arboretum