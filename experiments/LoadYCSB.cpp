//
// Created by Zhihan Guo on 4/25/23.
//
#include "ycsb/ycsb.h"

using namespace arboretum;

int main(int argc, char* argv[]) {
  // Note: must load db config first.
  ARDB::LoadConfig(argc, argv);
  auto bench_config = new(MemoryAllocator::Alloc(sizeof(YCSBConfig)))
      YCSBConfig(argc, argv);
  std::string dataset_id = (g_buf_type == PGBUF ?
      "page"+ std::to_string(g_idx_btree_fanout) + "-": "obj-")  +
      std::to_string(bench_config->num_rows_ / 1000000) + "g";
  std::strcpy(g_dataset_id, dataset_id.c_str());
  LOG_DEBUG("dataset id: %s", g_dataset_id);
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  auto workload = YCSBWorkload(db, bench_config);
  return 0;
}

