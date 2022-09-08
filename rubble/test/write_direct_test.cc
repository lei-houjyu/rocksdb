#include <stdio.h> 
#include <unistd.h> 
#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string>
#include <assert.h>
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cout << "Usage: ./write_direct src_fname dst_fname\n";
        return 0;
    }

    char* src_fname = argv[1];
    char* dst_fname = argv[2];
    size_t size = 17825792;
	int src_fd, dst_fd;
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
    src_fd = open(src_fname, O_RDONLY, 0755);
    if (src_fd < 0) {
        perror("open local sst failed");
        exit(1);
    }
    // auto time_point_4 = std::chrono::high_resolution_clock::now();
    // std::cout << "open file time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_4 - time_point_3).count() << " microsecs\n";
    
	ret = read(src_fd, buf, size);
    // auto time_point_5 = std::chrono::high_resolution_clock::now();
    // std::cout << "read file time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_5 - time_point_4).count() << " microsecs\n";
	if (ret < 0) {
		perror("read sst failed");
	}

    // std::cout << buf << std::endl;
    // Slice s(&buf[size - 53] , 53);
    // std::cout << "size : " << size <<  ", magic number : " << s.ToString() << std::endl;
        
   // 2. write buf to secondary's sst   
    dst_fd = open(dst_fname, O_WRONLY | O_DIRECT | O_DSYNC , 0755);
    if (dst_fd < 0){
        perror("open remote sst failed");
        exit(1);
    }

    // struct stat stat_buf;
    // fstat(fd, &stat_buf);
    // size_t f_size = static_cast<size_t>(stat_buf.st_size);
    // assert(f_size == size);

    // auto time_point_6 = std::chrono::high_resolution_clock::now();
	ret = write(dst_fd, buf, size);
    // auto time_point_7 = std::chrono::high_resolution_clock::now();
	if (ret < 0) {
		perror("write sst failed");
	}

    close(src_fd);
    close(dst_fd);
    free(buf);
	return 0;
}