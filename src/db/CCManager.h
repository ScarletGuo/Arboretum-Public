//
// Created by Zhihan Guo on 4/1/23.
//

#ifndef ARBORETUM_SRC_DB_CCMANAGER_H_
#define ARBORETUM_SRC_DB_CCMANAGER_H_

#include <Types.h>
namespace arboretum {
class CCManager {
 public:
  static RC TryEXLock(std::atomic<uint32_t> &state);
  static RC AcquireLock(std::atomic<uint32_t> &state, bool is_exclusive);
  static void ReleaseLock(std::atomic<uint32_t> &state, bool is_exclusive);
};
}

#endif //ARBORETUM_SRC_DB_CCMANAGER_H_
