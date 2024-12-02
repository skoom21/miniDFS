#include <thread>
#include <sys/stat.h>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <omp.h>
#include "dataserver.h"

int chunkSize = 2 * 1024 * 1024;

DataServer::DataServer(const std::string &name):name_(name), buf(nullptr), finish(true){
    std::string cmd = "mkdir -p " + name_;
    system(cmd.c_str());
}

void DataServer::operator()(){
    while(true){
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&](){return !this->finish;});
        if (cmd == "put"){
            size_ += bufSize / 1024.0 / 1024.0;
            put();
        }
        else if(cmd == "read")
            read();
        else if(cmd == "locate")
            locate();
        else if(cmd == "fetch")
            fetch();
        this->finish = true;
        lk.unlock();
        cv.notify_all();
    }
}

void DataServer::put(){
    int start = 0;
    std::ofstream os;
    #pragma omp parallel for private(os)
    for(start = 0; start < bufSize; start += chunkSize){
        int offset = start / chunkSize;
        std::string filePath = name_ + "/" + std::to_string(fid) + " " + std::to_string(offset);
        os.open(filePath);
        if(!os)
            std::cerr << "create file error in dataserver: (file name) " << filePath << std::endl;
        os.write(&buf[start], std::min(chunkSize, bufSize - start));
        os.close();
        #pragma omp critical
        {
            std::cout << "Written chunk to file: " << filePath << std::endl;
        }
    }
}

void DataServer::read(){
    int start = 0;
    buf = new char[bufSize];
    bool fileNotFound = false;
    #pragma omp parallel for
    for(start = 0; start < bufSize; start += chunkSize){
        int offset = start / chunkSize;
        std::string filePath = name_ + "/" + std::to_string(fid) + " " + std::to_string(offset);
        std::ifstream is(filePath);
        // file found not in this server.
        if(!is){
            #pragma omp critical
            {
                delete []buf;
                bufSize = 0;
                std::cerr << "File not found: " << filePath << std::endl;
                fileNotFound = true;
            }
        } else {
            is.read(&buf[start], std::min(chunkSize, bufSize - start));
            #pragma omp critical
            {
                std::cout << "Read chunk from file: " << filePath << std::endl;
            }
        }
    }
    if(fileNotFound) {
        delete []buf;
        bufSize = 0;
    }
}

void DataServer::fetch(){
    buf = new char[chunkSize];
    std::string filePath = name_ + "/" + std::to_string(fid) + " " + std::to_string(offset);
    std::ifstream is(filePath);
    // file found not in this server.
    if(!is){
        delete []buf;
        bufSize = 0;
        std::cerr << "File not found: " << filePath << std::endl;
    }
    else{
        is.read(buf, std::min(chunkSize, bufSize - chunkSize * offset));
        bufSize = is.tellg();
        std::cout << "Fetched chunk from file: " << filePath << std::endl;
    }
}

void DataServer::locate(){
    std::string filePath = name_ + "/" + std::to_string(fid) + " " + std::to_string(offset);
    std::ifstream is(filePath);
    if(is)
        bufSize = 1;
    else
        bufSize = 0;
    std::cout << "Located file: " << filePath << " with bufSize: " << bufSize << std::endl;
}

std::string DataServer::get_name()const{
    return name_;
}
