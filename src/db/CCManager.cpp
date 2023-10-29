//
// Created by Zhihan Guo on 4/1/23.
//

#include "CCManager.h"

namespace arboretum {

RC CCManager::AcquireLock(std::atomic<uint32_t> &state, bool is_exclusive) {
  // NO_WAIT impl.
  uint32_t unlocked = 0;
  uint32_t wr_locked = 1;
  if (is_exclusive) {
    return state.compare_exchange_strong(unlocked, wr_locked) ? OK : ABORT;
  }
  // Acquire read lock
  auto current = state.load(std::memory_order_acquire);
  uint32_t rd_locked = current + (1 << 1);
  if (current & 1) {
    return ABORT; // write locked
  }
  // not write locked
  while (!state.compare_exchange_strong(current, rd_locked)) {
    if (current & 1) return ABORT;
    rd_locked = current + (1 << 1);
  }
  return OK;
}

void CCManager::ReleaseLock(std::atomic<uint32_t> &state, bool is_exclusive) {
  if (is_exclusive) {
    state = 0;
  } else {
    auto prev = state.fetch_sub(1 << 1);
    M_ASSERT(prev > 0, "Releasing a read lock that is not held");
  }
}

RC CCManager::TryEXLock(std::atomic<uint32_t> &state) {
  if (state > 0) return ABORT;
  uint32_t unlocked = 0;
  uint32_t wr_locked = 1;
  return state.compare_exchange_strong(unlocked, wr_locked) ? OK : ABORT;
}


} // arboretum