# Arboretum-Distributed

## Hardware Setup
 ```
# if using cloudlab, need to mount extra storage using the following commands
sudo su 
mkdir /mydata
exit
sudo /usr/local/etc/emulab/mkextrafs.pl /mydata
```

## Install Dependencies

### on machine set up with github
```
# set up cpp redis
cd include/cpp_redis/
git reset --hard ab5ea8638bc51e3d407b0045aceb5c5fd3218aa0
```

### on machine to execute
For all storage types:
```
./BUILD
```
(Optional) If using Azure Storage Client Library for C++ (7.5.0)
```
cd tools; ./install_azure.sh
```
(Optional) If using TiKV, 
* install the client library and its dependencies
```
# working directory: /path/to/Arboretum-Distributed
cp tools/kvproto_cmake.txt include/client-c/third_party/kvproto/cpp/CMakeLists.txt
cd tools ;
chmod +x install_grpc.sh ; ./install_gprc.sh
chmod +x install_poco.sh ; ./install_poco.sh
./install_tikv.sh
cd ..
```

(Optional) If using Redis,
```
# working directory: /path/to/Arboretum-Distributed
cd tools/ ; 
chmod +x install_cppredis.sh ; ./install_cppredis.sh
git submodule update --init --recursive
cd include/cpp_redis/tacopie
git fetch origin pull/5/head:cmake-fixes
git checkout cmake-fixes
cd ..
```

## Compile and Run
generate makefile
```
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
```
expected output
```
-- Generating benchmark tests: 
-- [benchmark test] executable: bench_ycsb
-- Generating unit tests: 
-- [unit test] executable: AllUnitTests
-- [unit test] executable: CCTest
-- [unit test] executable: IndexObjBtreeTest
-- Generating experiments: 
-- [experiment] executable: ExpYCSB
-- Configuring done
-- Generating done
-- Build files have been written to: /users/scarletg/Arboretum-test/
```
compile using makefile
```
make -j <desire executable>
```
