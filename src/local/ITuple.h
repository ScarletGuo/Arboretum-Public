//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_LOCAL_ITUPLE_H_
#define ARBORETUM_SRC_LOCAL_ITUPLE_H_

#include "common/Common.h"
#include "ISchema.h"

namespace arboretum {
class ITuple {

 public:
  ITuple() = default;;
  explicit ITuple(OID tbl_id, OID tid) : tbl_id_(tbl_id), tid_(tid) {};
  void SetFromLoaded(char * loaded_data, size_t sz) { SetData(loaded_data, sz); };
  void SetData(char * src, size_t sz);
  char * GetData() {
    if (g_buf_type == PGBUF) {
      // XXX: pg data in azure does not have the two newly added fields
      return (char *) this + sizeof(ITuple) - sizeof(std::atomic<uint32_t>) - sizeof(bool);
    }
    return (char *) this + sizeof(ITuple);
  };
  void SetTID(OID tid) { tid_ = tid; };
  OID GetTID() const { return tid_; };
  OID GetTableId() const { return tbl_id_; };
  void GetField(ISchema * schema, OID col, SearchKey &key);
  void SetField(ISchema * schema, OID col, std::string value);
  void SetField(ISchema * schema, OID col, int64_t value);
  void GetPrimaryKey(ISchema * schema, SearchKey &key);
  SearchKey GetPrimaryKey(ISchema * schema) {
    SearchKey key;
    GetPrimaryKey(schema, key);
    return key;
  }

  // buffer helper
  std::atomic<uint32_t> buf_state_{0};
  ITuple * clock_next_{nullptr};
  std::mutex buf_latch_{};

 private:
  OID tbl_id_{0};
 public:
  OID tid_{0}; // identifier used in the lock table
  std::atomic<uint32_t> next_key_lock_{0};
  bool next_key_in_storage_{false};
};

}

#endif //ARBORETUM_SRC_LOCAL_ITUPLE_H_
