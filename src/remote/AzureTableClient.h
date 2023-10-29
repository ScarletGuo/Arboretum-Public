//
// Created by Zhihan Guo on 6/3/23.
//

#ifndef README_MD_SRC_REMOTE_AZURETABLECLIENT_H_
#define README_MD_SRC_REMOTE_AZURETABLECLIENT_H_

#include "AzurePythonHelper.h"
#include "common/Common.h"
#include "common/Worker.h"

#include <was/storage_account.h>
#include <was/table.h>

namespace arboretum {
class AzureTableClient {
 public:
  explicit AzureTableClient(const std::string &str) {
    const utility::string_t& storage_connection_string(U(str));
    storage_acct_ = azure::storage::cloud_storage_account::parse(storage_connection_string);
    table_client_ = storage_acct_.create_cloud_table_client();
    python_helper_ = NEW(AzurePythonHelper)(str);
    LOG_DEBUG("Created azure table client. ");
  }

  void CreateTable(const std::string &tbl_name) {
    tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    try {
      tables_[tbl_name].create_if_not_exists();
      LOG_DEBUG("Created table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to create table %s and exit.", tbl_name.c_str());
    }
  }

  void DeleteTable(const std::string &tbl_name) {
    auto tbl = table_client_.get_table_reference(U(tbl_name));
    try {
      tbl.delete_table();
      LOG_DEBUG("deleted table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to delete table and exit.");
    }
  }

  RC LoadSync(const std::string &tbl_name, std::string partition, const std::string &key, std::string &data) {
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    int retry_left = g_remote_req_retries;
    auto table = tables_[tbl_name];
    // auto table = storage_acct_.create_cloud_table_client().get_table_reference(U(tbl_name));
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(partition), U(key));
    while (retry_left >= 0) {
      try {
        auto retrieve_result = table.execute(retrieve_operation);
        azure::storage::table_entity entity = retrieve_result.entity();
        const azure::storage::table_entity::properties_type& properties = entity.properties();
        auto property = properties.at(U("Data"));
        if (property.property_type() == azure::storage::edm_type::binary) {
          auto ret = property.binary_value();
          data = std::string(ret.begin(), ret.end());
        } else if (property.property_type() == azure::storage::edm_type::string){
          data = property.string_value();
        }
        retry_left = -1;
      } catch (const std::exception &e) {
        retry_left--;
        if (retry_left >= 0) {
          if (g_warmup_finished) {
            auto backoff = Worker::GetThdId() * (g_remote_req_retries - retry_left);
            LOG_DEBUG("[thd-%u] Cannot load data %s: %s. %d-th retry after %u ms",
                      Worker::GetThdId(), key.c_str(), e.what(),
                      g_remote_req_retries - retry_left, backoff);
            usleep(backoff * 1000);
          } else {
            sleep(Worker::GetThdId() % 60 + 1);
          }
        } else {
          LOG_ERROR("[thd-%u] Cannot load data %s in table %s after retries: %s",
                    Worker::GetThdId(), key.c_str(), tbl_name.c_str(), e.what());
        }
      }
    }
    return RC::OK;
  }

  uint64_t LoadNumericSync(const std::string &tbl_name, std::string partition, const std::string &key) {
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    auto table = tables_[tbl_name];
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(partition), U(key));
    auto retrieve_result = table.execute(retrieve_operation);
    azure::storage::table_entity entity = retrieve_result.entity();
    const azure::storage::table_entity::properties_type& properties = entity.properties();
    try {
      return properties.at(U("Data")).int64_value();
    } catch (const std::exception &e) {
      LOG_ERROR("Cannot load data %s: %s", key.c_str(), e.what());
    }
  }

  void StoreSync(const std::string &tbl_name, azure::storage::table_entity &tuple) {
    auto table = tables_[tbl_name];
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreSync(const std::string &tbl_name, std::string partition, const std::string &key, char * data, size_t sz) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreAsync(const std::string &tbl_name, std::string partition,
      const std::string &key, char * data, size_t sz, volatile RC *rc) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto resp = table.execute_async(insert_op);
      resp.then([=] (azure::storage::table_result result) {
        *rc = RC::OK;
      });
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreNumericSync(const std::string &tbl_name, std::string partition, const std::string &key, int64_t number) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(number);
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreAndLoadSync(const std::string &tbl_name,
                        std::string store_part, const std::string &key,
                        char *data, size_t sz, std::string load_part,
                        const std::string &load_key, std::string &load_data) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(store_part), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(load_part), U(load_key));
    try {
      auto result = table.execute_async(insert_op);
      auto retrieve_result = table.execute(retrieve_operation);
      azure::storage::table_entity entity = retrieve_result.entity();
      const azure::storage::table_entity::properties_type& properties = entity.properties();
      auto ret = properties.at(U("Data")).binary_value();
      load_data = std::string(ret.begin(), ret.end());
      // wait for both requests to complete.
      result.wait();
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreAndLoad: %s", e.what());
    }
  }

  void BatchStoreSync(const std::string &tbl_name,
                      std::string part,
                      const std::multimap<std::string, std::string> &map) {
    auto table = tables_[tbl_name];
    azure::storage::table_batch_operation batch_op;
    for (auto & item : map) {
      azure::storage::table_entity tuple(U(part), U(item.first));
      auto& col = tuple.properties();
      col.reserve(1);
      col[U("Data")] = azure::storage::entity_property(
          std::vector<uint8_t>(item.second.begin(), item.second.end()));
      batch_op.insert_or_replace_entity(tuple);
    }
    try {
      auto results = table.execute_batch(batch_op);
    } catch (const std::exception &e) {
      LOG_DEBUG("Warning in BatchStore: %s", e.what());
    }
  }

  void BatchStoreSync(const std::string &tbl_name,
                      azure::storage::table_batch_operation &batch_op) {
    auto table = tables_[tbl_name];
    try {
      table.execute_batch(batch_op);
    } catch (const std::exception &e) {
      // LOG_DEBUG("Warning in BatchStore: %s", e.what());
    }
  }

  void BatchStoreAsync(const std::string &tbl_name, std::string part,
                      const std::multimap<std::string, std::string>& map,
                      volatile bool &is_done) {
    auto table = tables_[tbl_name];
    azure::storage::table_batch_operation batch_op;
    for (auto & item : map) {
      azure::storage::table_entity tuple(U(part), U(item.first));
      auto& col = tuple.properties();
      col.reserve(1);
      col[U("Data")] = azure::storage::entity_property(
          std::vector<uint8_t>(item.second.begin(), item.second.end()));
      batch_op.insert_or_replace_entity(tuple);
    }
    try {
      is_done = false;
      auto resp = table.execute_batch_async(batch_op);
      resp.then([&is_done] (const
      std::vector<azure::storage::table_result>& results) {
        is_done = true;
      });
    } catch (const std::exception &e) {
      LOG_DEBUG("Warning in BatchStore: %s", e.what());
    }
  }

  void WaitForAsync(int cnt) {
  }

  void LoadRangeSync(std::string &tbl_name,
                     std::string &partition,
                     uint64_t cnt,
                     uint64_t low_key,
                     char **data_ptrs) {
    auto table = tables_[tbl_name];
    // async execution.
    std::atomic<int32_t> semaphore(cnt);
    for (size_t i = 0; i < cnt; i++) {
      // Execute the query.
      try {
        auto key = SearchKey(low_key + i).ToString();
        auto op = azure::storage::table_operation::retrieve_entity(
            U(partition), U(key));
        auto resp = table.execute_async(op);
        auto data_dest = data_ptrs[i];
        resp.then([&semaphore, data_dest](azure::storage::table_result result) {
          azure::storage::table_entity entity = result.entity();
          auto &properties = entity.properties();
          auto property = properties.at(U("Data"));
          std::string data;
          if (property.property_type() == azure::storage::edm_type::binary) {
            auto ret = property.binary_value();
            data = std::string(ret.begin(), ret.end());
          } else if (property.property_type() == azure::storage::edm_type::string) {
            data = property.string_value();
          }
          memcpy(data_dest, data.c_str(), data.size());
          auto sem = atomic_fetch_sub(&semaphore, 1);
          // LOG_DEBUG("async request stored key %s at %p with semaphore = %u",
          //           entity.row_key().c_str(), data_dest, sem - 1);
        });
        if (i == cnt - 1) {
          resp.wait();
          while (semaphore > 0) {}
        }
      } catch (const azure::storage::storage_exception &e) {
        azure::storage::request_result result = e.result();
        azure::storage::storage_extended_error extended_error = result.extended_error();
        if (!extended_error.message().empty()) {
          ucout << extended_error.message() << std::endl;
        }
        LOG_ERROR("Error: %s", e.what());
      } catch (const std::exception &e) {
        ucout << _XPLATSTR("Error: ") << e.what() << std::endl;
      }
    }
  }

  void LoadRangePython(std::string &tbl_name, std::string &partition,
                       std::string low_key, std::string high_key, char **data_ptrs) {
    // async execution.
    python_helper_->ExecPyRangeSearch(tbl_name, partition,
                                      low_key, high_key, data_ptrs);
  }

 private:
  azure::storage::cloud_storage_account storage_acct_;
  azure::storage::cloud_table_client table_client_;
  std::unordered_map<std::string, azure::storage::cloud_table> tables_;
  AzurePythonHelper* python_helper_;

};
}

#endif //README_MD_SRC_REMOTE_AZURETABLECLIENT_H_
