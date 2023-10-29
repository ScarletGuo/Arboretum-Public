//
// Created by Zhihan Guo on 4/3/23.
//

#ifndef ARBORETUM_SRC_DB_ACCESS_IINDEX_H_
#define ARBORETUM_SRC_DB_ACCESS_IINDEX_H_

#include "common/Common.h"

namespace arboretum {
class IIndex {
 public:
  IIndex(OID tbl_id) : tbl_id_(tbl_id) { idx_id_ = ++num_idx_; }
  void AddCoveringCol(OID col) { cols_.insert(col); };
  std::set<OID>& GetCols() { return cols_; };
  OID GetIndexId() const { return idx_id_; };
  virtual void Load(size_t pg_cnt) { assert(false); };
  OID GetTableId() const { return tbl_id_; };

 protected:
  std::set<OID> cols_;
  OID idx_id_{0};
  static OID num_idx_;
  OID tbl_id_{0};
};
}

#endif //ARBORETUM_SRC_DB_ACCESS_IINDEX_H_
