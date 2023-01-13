ROCKSDB_DIR=/mnt/data/rocksdb

cd ${ROCKSDB_DIR}
rm -rf cmake-build-debug CMakeCache.txt CMakeFiles cmake_install.cmake
cd ${ROCKSDB_DIR}/rubble
rm -rf cmake-build-debug CMakeCache.txt CMakeFiles cmake_install.cmake

cd ${ROCKSDB_DIR}
cmake .
make clean
make -j48

cd ${ROCKSDB_DIR}/rubble
cmake .
make clean
make -j48


