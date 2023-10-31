#pragma once

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <thread>
#include <string.h>

#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <iomanip>

#define PAGE_SIZE 4096
thread_local int worker_id = -1;
static const uint64_t pool_size_set = (uint64_t)2 * 1024 * 1024 * 1024;

class CLMemPool{
public: 
    char *m_buf;
    size_t m_size;
    char *m_current;
    char *m_end;

public:
    CLMemPool(){
        m_buf = nullptr;
        m_size = 0;
        m_current = nullptr;
        m_end = nullptr;
    }

    void initialize(char* start, size_t size){
        m_buf = start;
        m_size = size;
        m_current = start;
        m_end = start + size;
    }

    ~CLMemPool(){
        m_buf = nullptr;
        m_size = 0;
        m_current = nullptr;
        m_end = nullptr;
    }

    void* Allocate(size_t size){
        if (m_current + size <= m_end){
            register char *p;
            p = m_current;
            m_current += size;
            return (void *)p;
        }
        return nullptr;
    }
};

class CLThreadPMPool{
public: 
    CLMemPool *m_pools;
    int m_thread_num;
    char *m_buf;
    size_t m_pool_size;

    int fd;

public:
    CLThreadPMPool(){
        m_pools = nullptr;
        m_buf = nullptr;
        m_pool_size = 0;
        m_thread_num = 0;

        fd = -1;
    }

    void initialize(size_t pool_size, int threadNum){
        m_thread_num = threadNum;

        m_pools = new CLMemPool[threadNum];
        size_t sizeOfPool = (pool_size/(threadNum+threadNum-2)/PAGE_SIZE)*PAGE_SIZE;
        m_pool_size = sizeOfPool*(threadNum+threadNum-2);

        fd = open("/home/7948lkj/yzy/overhead-evaluation/pmem", O_RDWR, 0666);
        if(fd == -1)
        {
            perror("error open file for pmem!");
            return;
        }

        // 将文件映射到内存
        void* tmp_buf = mmap(nullptr, m_pool_size, PROT_READ | PROT_WRITE , MAP_SHARED, fd, 0);
				
        if(tmp_buf == MAP_FAILED)
        {
            close(fd);
            perror("error mmap for pmem!");
            return;
        }

        m_buf = (char *)tmp_buf;        

        m_pools[0].initialize(m_buf, sizeOfPool*(threadNum-1));
        for (int i = 1; i < threadNum; i++)
            m_pools[i].initialize(m_buf + (i-1+threadNum-1) * sizeOfPool, sizeOfPool);
    }

    ~CLThreadPMPool(){
        munmap(m_buf, m_pool_size);
        
        close(fd);
        fd = -1;

        m_buf = nullptr;
        m_pool_size = 0;
        m_thread_num = 0;

        delete [] m_pools;
        m_pools = nullptr;
    }
};

CLThreadPMPool* pmAllocator = new CLThreadPMPool();

void initializeMemoryPool(int threadNum, bool isRecover = false){
    pmAllocator->initialize(pool_size_set, threadNum);
}

void closeMemoryPool(){
    pmAllocator->~CLThreadPMPool();
}

inline void persist(char *addr, int len){
    // 刷入SSD
    char* aligned_addr = (char*)(~((uintptr_t)0xFFF) & (uintptr_t)addr);
    if(msync(aligned_addr, len, MS_SYNC) == -1)
    {
        perror("error msync for pmem!");
        return;
    }
    else
        std::cout << "msync success!" << std::endl;
}

void *alloc(size_t size) {
  void* ret;
  ret = pmAllocator->m_pools[worker_id].Allocate(size);
  return ret;
}