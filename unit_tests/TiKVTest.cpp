//
// Created by Zhihan Guo on 5/10/23.
//

#include "remote/TiKVClient.h"

using namespace arboretum;

int main(int argc, char* argv[]) {
  auto client = new TiKVClient();
  LOG_DEBUG("created client");
  char value[10] = "world";
  client->StoreSync(0, "hello", value, 10);
  LOG_DEBUG("sent log request");
  std::string data;
  client->LoadSync(0, "hello", data);
  std::cout << "resp: " << data << std::endl;
}
