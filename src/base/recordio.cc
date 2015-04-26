// #include "base/recordio.h"

// #include <zlib.h>
// #include <string>
// #include "glog/logging.h"

// namespace ps {
// const int RecordWriter::kMagicNumber = 0x3ed7230a;

// std::string RecordWriter::Compress(std::string const& s) const {
//   const unsigned long source_size = s.size();  // NOLINT
//   const char* source = s.c_str();

//   unsigned long dsize = source_size + (source_size * 0.1f) + 16;  // NOLINT
//   std::unique_ptr<char[]> destination(new char[dsize]);
//   // Use compress() from zlib.h.
//   const int result =
//       compress(reinterpret_cast<unsigned char*>(destination.get()), &dsize,
//                reinterpret_cast<const unsigned char*>(source), source_size);

//   if (result != Z_OK) {
//     LOG(FATAL) << "Compress error occured! Error code: " << result;
//   }
//   return std::string(destination.get(), dsize);
// }

// void RecordReader::Uncompress(const char* const source, uint64 source_size,
//                               char* const output_buffer,
//                               uint64 output_size) const {
//   unsigned long result_size = output_size;  // NOLINT
//   // Use uncompress() from zlib.h
//   const int result =
//       uncompress(reinterpret_cast<unsigned char*>(output_buffer), &result_size,
//                  reinterpret_cast<const unsigned char*>(source), source_size);
//   if (result != Z_OK) {
//     LOG(FATAL) << "Uncompress error occured! Error code: " << result;
//   }
//   CHECK_LE(result_size, static_cast<unsigned long>(output_size));  // NOLINT
// }

// }  // namespace ps
