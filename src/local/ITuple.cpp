//
// Created by Zhihan Guo on 3/31/23.
//

#include "ITuple.h"

namespace arboretum {

void ITuple::SetData(char *src, size_t sz) {
  memcpy(GetData(), src, sz);
}

void ITuple::GetField(ISchema * schema, OID col, SearchKey &key) {
  auto val = &(GetData()[schema->GetFieldOffset(col)]);
  switch (schema->GetFieldType(col)) {
    case DataType::BIGINT:
      key.SetValue(*((int64_t *) val), DataType::BIGINT);
      break;
    case DataType::INTEGER:
      key.SetValue(*((int *) val), DataType::INTEGER);
      break;
    default:
      LOG_ERROR("Not support FLOAT8 as search key yet");
  }
}

void ITuple::SetField(ISchema * schema, OID col, int64_t value) {
  schema->SetNumericField(col, GetData(), value);
}

void ITuple::SetField(ISchema * schema, OID col, std::string value) {
  schema->SetCharField(col, GetData(), value.c_str(), value.size());
}

void ITuple::GetPrimaryKey(ISchema *schema, SearchKey &key) {
  // TODO: for now assume non-composite pkeys
  auto col = schema->GetPKeyColIds()[0];
  GetField(schema, col, key);
}

}