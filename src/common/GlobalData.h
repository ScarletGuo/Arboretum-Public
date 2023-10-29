//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_COMMON_GLOBALDATA_H_
#define ARBORETUM_SRC_COMMON_GLOBALDATA_H_

#include "Types.h"

namespace arboretum {

#define DECL_GLOBAL_CONFIG2(tpe, var) extern tpe var;
#define DECL_GLOBAL_CONFIG3(tpe, var, val) DECL_GLOBAL_CONFIG2(tpe, var)
#define DECL_GLOBAL_CONFIG4(tpe, var, val, f) DECL_GLOBAL_CONFIG2(tpe, var)
#define DECL_GLOBAL_CONFIG5(tpe, var, val, f1, f2) DECL_GLOBAL_CONFIG2(tpe, var)
#define DEFN_GLOBAL_CONFIG2(tpe, var) tpe var;
#define DEFN_GLOBAL_CONFIG3(tpe, var, val) tpe var = val;
#define DEFN_GLOBAL_CONFIG4(tpe, var, val, f) DEFN_GLOBAL_CONFIG3(tpe, var, val)
#define DEFN_GLOBAL_CONFIG5(tpe, var, val, f1, f2) DEFN_GLOBAL_CONFIG3(tpe, var, val)
#define IF_GLOBAL_CONFIG2(tpe, var) ;
#define IF_GLOBAL_CONFIG3(tpe, var, val) \
if (item.first == #var) { (var) = item.second.get_value<tpe>(); continue; }
#define IF_GLOBAL_CONFIG4(tpe, var, val, f) IF_GLOBAL_CONFIG3(tpe, var, val)
#define IF_GLOBAL_CONFIG5(tpe, var, val, f1, f2)          \
if (item.first == #var) {                                 \
  auto str = item.second.get_value<std::string>();        \
  (var) = f2(str); continue;                              \
}
#define PRINT_DB_CONFIG2(tpe, name) ;
#define PRINT_DB_CONFIG3(tpe, name, val) { \
std::cout << #name << ": " << (name) << std::endl; \
if (g_save_output) g_out_str << "\"" << #name << "\"" << ": " << (name) << ", "; }
#define PRINT_DB_CONFIG4(tpe, name, val, func) { \
std::cout << #name << ": " << func(name) << std::endl; \
if (g_save_output) g_out_str << "\"" << #name << "\"" << ": \"" \
<< func(name) << "\", "; }
#define PRINT_DB_CONFIG5(tpe, name, val, f1, f2) \
PRINT_DB_CONFIG4(tpe, name, val, f1)

// pre-declaration
class BufferManager;
class IDataStore;
class ILogStore;
class Stats;

// Configuration
// ====================
extern char g_config_fname[100];
extern char tikv_config_fname[100];
extern char redis_config_fname[100];
extern char azure_config_fname[100];
extern char g_dataset_id[16];

// Index
// ====================
#define DB_INDEX_CONFIG(x, y, z, a)                         \
a(IndexType, g_index_type, IndexType::REMOTE,               \
IndexTypeToString, StringToIndexType)                       \
y(size_t, g_idx_btree_fanout, 250)                          \
y(double, g_idx_btree_split_ratio, 0.9)

// Local Storage
// ====================
#define AR_PAGE_SIZE 8192
#define DB_LOCAL_STORE_CONFIG(x, y, z, a)                    \
x(BufferManager *, g_buf_mgr)                                \
a(BufferType, g_buf_type, NOBUF,                             \
BufferTypeToString, StringToBufferType)                      \
y(size_t, g_total_buf_sz, 128 * 1024 * 1024)                 \
y(size_t, g_pagebuf_num_slots, g_total_buf_sz / AR_PAGE_SIZE) \
y(size_t, g_buf_entry_sz, 1000)

// Remote Storage
// ====================
#define DB_REMOTE_STORE_CONFIG(x, y, z, a)                 \
x(IDataStore *, g_data_store)                              \
x(ILogStore *, g_log_store)                                \
z(bool, g_enable_group_commit, true, BoolToString)         \
y(size_t, g_commit_queue_limit, 5)                         \
y(size_t, g_commit_group_sz, 32)                           \
y(uint64_t, g_log_freq_us, 50)                             \
z(bool, g_load_to_remote_only, false, BoolToString)        \
z(bool, g_restore_from_remote, true, BoolToString)         \
z(bool, g_check_loaded, false, BoolToString)               \
y(size_t, g_partition_sz, 10000)                           \
z(bool, g_load_range, false, BoolToString)                 \
y(size_t, g_num_restore_thds, 8)                           \
y(int, g_remote_req_retries, 3)

// System
// ====================
#define DB_SYSTEM_CONFIG(x, y, z, a)                       \
x(Stats *, g_stats) y(size_t, g_num_worker_threads, 1)     \
y(size_t, g_commit_pool_sz, 4) y(double, g_cpu_freq, 1)    \
z(bool, g_warmup_finished, false, BoolToString)            \
z(bool, g_enable_logging, true, BoolToString)              \
z(bool, g_force_write, false, BoolToString)                \
y(OID, g_node_id, 0) z(bool, g_save_output, false, BoolToString) \
z(bool, g_early_lock_release, false, BoolToString)         \
z(bool, g_enable_phantom_protection, false, BoolToString)

extern std::stringstream g_out_str;
extern std::ofstream g_out_file;
extern char g_out_fname[100];
extern volatile bool g_terminate_exec;

#define DB_CONFIGS(x, y, z, a)     \
DB_INDEX_CONFIG(x, y, z, a)        \
DB_LOCAL_STORE_CONFIG(x, y, z, a)  \
DB_REMOTE_STORE_CONFIG(x, y, z, a) \
DB_SYSTEM_CONFIG(x, y, z, a)
DB_CONFIGS(DECL_GLOBAL_CONFIG2, DECL_GLOBAL_CONFIG3,
           DECL_GLOBAL_CONFIG4, DECL_GLOBAL_CONFIG5)

} // arboretum

#endif //ARBORETUM_SRC_COMMON_GLOBALDATA_H_
