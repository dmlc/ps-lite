#pragma once

#include <cstdlib>
#include <cstdio>
#include <string>
#include "zlib.h"
#include "base/common.h"
#include "glog/logging.h"
#include "proto/data.pb.h"

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/io/tokenizer.h"

namespace ps {

class File {
 public:
  // Opens file "name" with flags specified by "flag".
  // Flags are defined by fopen(), that is "r", "r+", "w", "w+". "a", and "a+".
  static File* open(const std::string& name, const char* const flag);
  // If open failed, program will exit.
  static File* openOrDie(const std::string& name, const char* const flag);
  // open the file in "name", support read-only hdfs file
  static File* open(const DataConfig& name, const char* const flag);
  // If open failed, program will exit.
  static File* openOrDie(const DataConfig& name, const char* const flag);
  // the size of a file
  static size_t size(const std::string& name);
  // Deletes a file.
  static bool remove(const std::string& name) { return std::remove(name.c_str()) == 0; }
  // Tests if a file exists.
  static bool exists(const char* const name) { return access(name, F_OK) == 0; }
  static bool gzfile(const std::string& name) {
    return (name.size() > 3 && std::string(name.end()-3, name.end()) == ".gz");
  }


  // Reads "size" bytes to buff from file, buff should be pre-allocated.
  size_t read(void* const buff, size_t size);
  void readOrDie(void* const buff, size_t size) {
    CHECK_EQ(read(buff, size), size);
  }
  // Reads a line from file to a std::string.
  // Each line must be no more than max_length bytes
  char* readLine(char* const output, uint64 max_length);
  // Reads the whole file to a std::string, with a maximum length of 'max_length'.
  // Returns the number of bytes read.
  int64 readToString(std::string* const line, uint64 max_length);
  // Writes "size" bytes of buff to file, buff should be pre-allocated.
  size_t write(const void* const buff, size_t size);
  // If write failed, program will exit.
  void writeOrDie(const void* const buff, size_t size) {
    CHECK_EQ(write(buff, size), size);
  }
  // Writes a std::string to file.
  size_t writeString(const std::string& line);
  // flush the buffer
  bool flush();
  // Closes the file.
  bool close();
  // Returns file size.
  size_t size();
  // seek a position, starting from the head
  bool seek(size_t position);
  // Returns the file name.
  std::string filename() const { return name_; }
  // check if it is open
  bool open() const { return (f_ != NULL); }
 private:
  File(FILE* f_des, const std::string& name)
      : f_(f_des), name_(name) { }
  File(gzFile gz_des, const std::string& name)
      : gz_f_(gz_des), name_(name) {
    is_gz_ = true;
  }
  FILE* f_ = NULL;
  gzFile gz_f_ = NULL;
  bool is_gz_ = false;
  const std::string name_;

  // Writes a std::string to file and append a "\n".
  // bool WriteLine(const std::string& line);
  // Flushes buffer.
  // // current file position
  // size_t position() {
  //   return (size_t) ftell(f_);
  // }
};


bool readFileToString(const std::string& file_name, std::string* output);
bool writeStringToFile(const std::string& data, const std::string& file_name);

//// convenient functions dealing with protobuf

typedef google::protobuf::Message GProto;
bool readFileToProto(const DataConfig& name, GProto* proto);
void readFileToProtoOrDie(const DataConfig& name, GProto* proto);

bool readFileToProto(const std::string& file_name, GProto* proto);
void readFileToProtoOrDie(const std::string& file_name, GProto* proto);

bool writeProtoToASCIIFile(const GProto& proto, const std::string& file_name);
void writeProtoToASCIIFileOrDie(const GProto& proto, const std::string& file_name);

bool writeProtoToFile(const GProto& proto, const std::string& file_name);
void writeProtoToFileOrDie(const GProto& proto, const std::string& file_name);

// return the hadoop fs command
std::string hadoopFS(const HDFSConfig& conf);

// operations about directories, may create class Directory
bool dirExists(const std::string& dir);
bool createDir(const std::string& dir);

// return the file names in a directory
std::vector<std::string> readFilenamesInDirectory(const std::string& directory);
std::vector<std::string> readFilenamesInDirectory(const DataConfig& directory);

string removeExtension(const string& file);
string getPath(const string& full);
string getFilename(const string& full);

} // namespace ps
