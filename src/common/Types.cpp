//
// Created by Zhihan Guo on 3/31/23.
//
#include "Types.h"
#include "GlobalData.h"

namespace arboretum {

std::string SearchKey::ToString() const {
  if (g_buf_type == PGBUF)
    return std::to_string(numeric_);
  if (data_[ARBORETUM_KEY_SIZE - 1] == '0') {
    SetValue(numeric_, (char *) &data_[0]);
  }
  return "0" + std::string(data_, ARBORETUM_KEY_SIZE);
}

void SearchKey::SetValue(uint64_t key, DataType type) {
  numeric_ = key;
  type_ = type;
  SetValue(numeric_, &data_[0]);
}

void SearchKey::SetValue(uint64_t key, char* data) const {
  auto s = std::to_string(key);
  M_ASSERT(s.size() <= ARBORETUM_KEY_SIZE, "search key exceed bound");
  int cnt = 0;
  for (int i = ARBORETUM_KEY_SIZE - 1; i >= 0; i--) {
    cnt++;
    if (cnt <= s.size()) {
      data[i] = s[s.size() - cnt];
    } else {
      data[i] = '0';
    }
  }
}

DataType StringToDataType(std::string &s) {
  if (s == "string") {
    return DataType::CHAR;
  } else if (s == "int64_t") {
    return DataType::BIGINT;
  } else if (s == "double") {
    return DataType::FLOAT8;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

BufferType StringToBufferType(std::string &s) {
  if (s == "NOBUF") {
    return BufferType::NOBUF;
  } else if (s == "OBJBUF") {
    return BufferType::OBJBUF;
  } else if (s == "PGBUF") {
    return BufferType::PGBUF;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

IndexType StringToIndexType(std::string &s) {
  if (s == "REMOTE") {
    return IndexType::REMOTE;
  } else if (s == "BTREE") {
    return IndexType::BTREE;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

std::string BufferTypeToString(BufferType tpe) {
  switch (tpe) {
    case BufferType::OBJBUF:
      return "OBJBUF";
    case NOBUF:
      return "NOBUF";
    case PGBUF:
      return "PGBUF";
    default: LOG_ERROR("type not supported");
  }
}

std::string IndexTypeToString(IndexType tpe) {
  switch (tpe) {
    case IndexType::BTREE:
      return "BTREE";
    case IndexType::REMOTE:
      return "REMOTE";
    default: LOG_ERROR("type not supported");
  }
}

std::string BoolToString(bool tpe) {
  if (tpe) {
    return "True";
  }
  return "False";
}

std::string RCToString(RC rc) {
  switch (rc) {
    case RC::ABORT:
      return "ABORT";
    case RC::ERROR:
      return "ERROR";
    case RC::OK:
      return "OK";
    case RC::PENDING:
      return "PENDING";
  }
}

}