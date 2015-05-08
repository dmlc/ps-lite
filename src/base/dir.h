#pragma once
#include <string>
#include <dirent.h>
#include <sys/stat.h>
namespace ps {

inline bool DirExists(const std::string& dir) {
  struct stat info;
  if (stat(dir.c_str(), &info) != 0) return false;
  if (info.st_mode & S_IFDIR) return true;
  return false;
}

inline bool CreateDir(const std::string& dir) {
  return mkdir(dir.c_str(), 0755) == 0;
}

}  // namespace ps
