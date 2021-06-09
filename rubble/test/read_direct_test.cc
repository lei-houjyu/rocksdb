#include <stdio.h> 
#include <unistd.h> 
#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string>
#include <assert.h>
#include <iostream>

int main(){
    std::string file{"/mnt/sdb/archive_dbs/tail/sst_dir/1"};
    size_t size = 512;
	int fd;
    char *buf = NULL;
    // 1. read primary's sst to buf
    // auto time_point_1 = std::chrono::high_resolution_clock::now();
    int ret = posix_memalign((void **)&buf, 512, size);
    // auto time_point_2 = std::chrono::high_resolution_clock::now();
    // std::cout << "Memalign time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_2 - time_point_1).count() << " microsecs\n";
    if (ret) {
        perror("posix_memalign failed");
        exit(1);
    }
  // memset(buf, 0, size);
  // auto time_point_3 = std::chrono::high_resolution_clock::now();
  // std::cout << "Memset time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_3 - time_point_2).count() << " microsecs\n";
  fd = open(file.c_str(), O_RDONLY | O_DIRECT, 0755);
  if (fd < 0) {
      perror("open sst failed");
      exit(1);
  }
  // auto time_point_4 = std::chrono::high_resolution_clock::now();
  // std::cout << "open file time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_4 - time_point_3).count() << " microsecs\n";
    
    std::cout << "read " << file << std::endl;
	ret = read(fd, buf, size);
  // auto time_point_5 = std::chrono::high_resolution_clock::now();
  // std::cout << "read file time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_5 - time_point_4).count() << " microsecs\n";
	if (ret < 0) {
		perror("read sst failed");
	}
    std::cout << "read " << ret << "bytes\n"; 
    close(fd);

    std::cout << buf << std::endl;
    // Slice s(&buf[size - 53] , 53);
    // std::cout << "size : " << size <<  ", magic number : " << s.ToString() << std::endl;
        
    // 2. write buf to secondary's sst   
    // printf("write to %s\n", to.c_str());
    // fd = open(to.c_str(), O_WRONLY | O_DIRECT , 0755);
    // if (fd < 0){
    //     perror("open sst failed");
    //     exit(1);
    // }

    // struct stat stat_buf;
    // fstat(fd, &stat_buf);
    // size_t f_size = static_cast<size_t>(stat_buf.st_size);
    // assert(f_size == size);

    // // auto time_point_6 = std::chrono::high_resolution_clock::now();
	// ret = write(fd, buf, size);
    // // auto time_point_7 = std::chrono::high_resolution_clock::now();
	// if (ret < 0) {
	// 	perror("write sst failed");
	// }

    // // std::cout << "wrote " << ret << " bytes to " << to << " ,latency : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_7 - time_point_6).count() << " microsecs\n";
    // close(fd);
    free(buf);
	return 0;
}