#include <string>
#include <chrono>
#include <iostream>
#include <stdio.h> 
#include <unistd.h> 
#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>

int main(){
    size_t size = 64 << 20;
    int fd;
    char *buf = NULL;
    int ret = posix_memalign((void **)&buf, 512, size);
    // auto time_point_2 = std::chrono::high_resolution_clock::now();
    // std::cout << "Memalign time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_2 - time_point_1).count() << " microsecs\n";
    if (ret) {
        perror("posix_memalign failed");
        exit(1);
    }

    memset(buf, 'c', size);
    // auto time_point_3 = std::chrono::high_resolution_clock::now();
    // std::cout << "Memset time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_3 - time_point_2).count() << " microsecs\n";
    fd = open("1", O_WRONLY | O_CREAT , 0755);
    if (fd < 0) {
        perror("open sst failed");
        exit(1);
    }

    // auto time_point_2 = std::chrono::high_resolution_clock::now();
    auto time_point_6 = std::chrono::high_resolution_clock::now();
	ret = write(fd, buf, size);
    auto time_point_7 = std::chrono::high_resolution_clock::now();
	if (ret < 0) {
		perror("write sst failed");
	}
    std::cout << "wrote " << ret << " bytes , latency : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_7 - time_point_6).count() << " microsecs\n";
    close(fd);
    free(buf);
}