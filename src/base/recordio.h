#pragma once

#include <string>
#include <memory>
#include "base/file.h"
namespace ps {

namespace {
static const int kMagicNumber = 0x3ed7230a;
}

// This class appends a protocol buffer to a file in a binary format.
// The data written in the file follows the following format (sequentially):
// - MagicNumber (32 bits) to recognize this format.
// - data payload size (32 bits)
// - Payload
class RecordWriter {
 public:
  // Magic number when writing and reading protocol buffers.
  RecordWriter() { file_ = NULL; }
  explicit RecordWriter(File* const file) : file_(file) { }
  bool Close() { return file_ && file_->close(); }

  template <class P> bool WriteProtocolMessage(const P& proto) {
    if (file_ == NULL) return false;
    std::string buffer;
    proto.SerializeToString(&buffer);
    const uint32 buff_size = (uint32) buffer.size();
    if (file_->write(&kMagicNumber, sizeof(kMagicNumber)) !=
        sizeof(kMagicNumber)) {
      return false;
    }
    if (file_->write(&buff_size, sizeof(buff_size)) != sizeof(buff_size)) {
      return false;
    }
    if (file_->write(buffer.c_str(), buff_size) != buff_size) {
      return false;
    }
    return true;
  }

 private:
  File* file_;
};

// This class reads a protocol buffer from a file.
// The format must be the one described in RecordWriter, above.
class RecordReader {
 public:
  explicit RecordReader(File* const file) : file_(file) { }
  bool Close() { return file_->close(); }

  template <class P> bool ReadProtocolMessage(P* const proto) {
    uint32 size = 0;
    int magic_number = 0;

    if (file_->read(&magic_number, sizeof(magic_number)) !=
        sizeof(magic_number)) {
      return false;
    }
    if (magic_number != kMagicNumber) {
      return false;
    }
    if (file_->read(&size, sizeof(size)) != sizeof(size)) {
      return false;
    }
    std::unique_ptr<char[]> buffer(new char[size + 1]);
    if (file_->read(buffer.get(), size) != size) {
      return false;
    }
    proto->ParseFromArray(buffer.get(), size);
    return true;
  }

 private:
  File* file_;
};

}  // namespace ps
