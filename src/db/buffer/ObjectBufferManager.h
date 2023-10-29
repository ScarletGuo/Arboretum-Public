//
// Created by Zhihan Guo on 4/3/23.
//

#ifndef ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_
#define ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_

#include "BufferManager.h"
#include "Common.h"
#include "ITable.h"

namespace arboretum {

class ARDB;

class ObjectBufferManager : public BufferManager {
 public:
  explicit ObjectBufferManager(ARDB *db);
  ITuple * AllocTuple(size_t tuple_sz, OID tbl_id, OID tid);
  static ITuple *AccessTuple(SharedPtr &ptr, uint64_t expected_key);
  ITuple * LoadFromStorage(OID tbl_id, SearchKey key);
  ITuple ** LoadRangeFromStorage(OID tbl_id, SearchKey low_key, SearchKey high_key);
  static void FinishAccessTuple(SharedPtr &ptr, bool dirty=false);
  static void FinishAccessTuple(ITuple *tuple, bool dirty);
  void AllocSpace(size_t sz) { other_allocated_ += sz; };
  void DeallocSpace(size_t sz) { other_allocated_ -= sz; };
  size_t GetAllocated() const { return allocated_ + other_allocated_ / g_buf_entry_sz; };
  size_t GetBufferSize() const { return num_slots_; };
  bool IsWarmedUp(uint64_t row_cnt) const {
    auto cnt = allocated_ + other_allocated_ / g_buf_entry_sz;
    bool warmed = cnt >= row_cnt - 1 || cnt >= num_slots_ * 0.9;
    LOG_DEBUG("checking warm up status = %d (allocated_ = %lu, total = %zu)",
              warmed, cnt, num_slots_);
    return warmed;
  };

 public:
  static inline bool CheckFlag(uint32_t state, uint64_t flag) { return (state & BUF_FLAG_MASK) & flag; }
  static inline uint32_t ClearFlag(uint32_t state, uint64_t flag) { state &= ~flag; return state; }
  static inline uint32_t SetFlag(uint32_t state, uint64_t flag) { state |= flag;
    return state;}
  static inline uint32_t GetPinCount(uint32_t state) { return BUF_STATE_GET_REFCOUNT(state); }
  static inline uint32_t GetUsageCount(uint32_t state) { return BUF_STATE_GET_USAGECOUNT(state); }
  static inline void SetUsageCount(ITuple *row, size_t cnt);
  static inline bool IsPinned(uint32_t state) { return GetPinCount(state) > 0; }
  static inline void Pin(ITuple * row);
  static inline void UnPin(ITuple * row);

 private:
  void BatchEvict(size_t cnt, volatile bool &is_done);
  void EvictAndLoad(SearchKey &load_key, ITuple *tuple, bool load = false);

  ARDB *db_{nullptr};
  ITuple *evict_hand_{nullptr};
  ITuple *evict_tail_{nullptr};
  size_t num_slots_{0};
  std::atomic<size_t> allocated_{0};
  std::atomic<size_t> other_allocated_{0};
  std::mutex latch_;

};

}

#endif //ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_
