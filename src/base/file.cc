#include "base/file.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <iostream>
#include <memory>
#include <dirent.h>
#include "base/common.h"
#include "base/split.h"
#if USE_S3
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#endif // USE_S3

// TODO read and write gz files, see zlib.h. evaluate the performace gain
namespace ps {

DECLARE_bool(verbose);

File* File::open(const std::string& name, const char* const flag) {
  File* f;
  if (name == "stdin") {
    f = new File(stdin, name);
  } else if (name == "stdout") {
    f = new File(stdout, name);
  } else if (name == "stderr") {
    f = new File(stderr, name);
  } else if (gzfile(name)) {
    gzFile des = gzopen(name.data(), flag);
    if (des == NULL) {
      // LOG(ERROR) << "cannot open " << name;
      return NULL;
    }
    f = new File(des, name);
  } else {
    FILE*  des = fopen(name.data(), flag);
    if (des == NULL) {
      // LOG(ERROR) << "cannot open " << name;
      return NULL;
    }
    f = new File(des, name);
  }
  return f;
}

File* File::openOrDie(const std::string& name, const char* const flag) {
  File* f = File::open(name, flag);
  CHECK(f != NULL && f->open());
  return f;
}
#if USE_S3
bool s3file(const std::string& name) {
  return (name.size() > 5 && std::string(name.begin(), name.begin()+5) == "s3://");
}
std::string s3Prefix(const std::string& path) {
  size_t pos = path.find('/',5);
  return std::string(path.begin()+pos+1,path.end());
}
std::string s3Bucket(const std::string& path) {
  size_t pos = path.find('/',5);
  return std::string(path.begin()+5,path.begin()+pos);
}

std::string s3DirectoryUrl(const std::string& path) {
  std::string str = "http://"+s3Bucket(path)+".s3.amazonaws.com/?prefix="+s3Prefix(path);
  return str;
}
std::string s3FileUrl(const std::string& path) {
  std::string str = "http://"+s3Bucket(path)+".s3.amazonaws.com/"+s3Prefix(path);
  return str;
}
#endif // USE_S3
File* File::open(const DataConfig& name,  const char* const flag) {
  CHECK_EQ(name.file_size(), 1);
  auto filename = name.file(0);
  if (name.has_hdfs()) {
    // TODO can use -text here, compare which one is better
    string cmd = hadoopFS(name.hdfs()) + " -cat " + filename;

    // .gz
    if (gzfile(filename)) {
      cmd += " | gunzip";
    }

    FILE* des = popen(cmd.c_str(), "r");
    if (des == NULL) {
      // LOG(ERROR) << "cannot open " << name.DebugString();
      return NULL;
    }
    auto f = new File(des, filename);
    return f;
  }
#if USE_S3
  else if (s3file(filename)) {
    std::string cmd = "curl -s -X GET "+s3FileUrl(filename);
    // .gz
    if (gzfile(filename)) {
      cmd += " | gunzip";
    }
    FILE* des = popen(cmd.c_str(), "r");
    CHECK(des);
    auto f = new File(des, filename);
    return f;
  }
#endif // USE_S3
  else {
    return open(filename, flag);
  }
}

File* File::openOrDie(const DataConfig& name,  const char* const flag) {
  File* f = open(name, flag);
  CHECK(f != NULL && f->open()) << "cannot open " << name.DebugString();
  return f;
}

size_t File::size(const std::string& name) {
  if (gzfile(name)) {
    LL << "didn't implement how to get a gz file size";
    return 0;
  }
  struct stat f_stat;
  stat(name.c_str(), &f_stat);
  return f_stat.st_size;
}

size_t File::size() {
  return File::size(name_);
}

bool File::flush() {
  return (is_gz_ ? gzflush(gz_f_, Z_FINISH) == Z_OK : fflush(f_) == 0);
}

bool File::close() {
  bool ret = is_gz_ ? gzclose(gz_f_) == Z_OK : fclose(f_) == 0;
  gz_f_ = NULL; f_ = NULL;
  return ret;
}

size_t File::read(void* const buf, size_t size) {
  return (is_gz_ ? gzread(gz_f_, buf, size) : fread(buf, 1, size, f_));
}

size_t File::write(const void* const buf, size_t size) {
  return (is_gz_ ? gzwrite(gz_f_, buf, size) : fwrite(buf, 1, size, f_));
}

char* File::readLine(char* const output, uint64 max_length) {
  return (is_gz_ ? gzgets(gz_f_, output, max_length) : fgets(output, max_length, f_));
}

bool File::seek(size_t position) {
  return (is_gz_ ?
          gzseek(gz_f_, position, SEEK_SET) == position :
          fseek(f_, position, SEEK_SET) == 0);
}

int64 File::readToString(std::string* const output, uint64 max_length) {
  CHECK_NOTNULL(output);
  output->clear();

  if (max_length == 0) return 0;
 // if (max_length < 0) return -1;

  int64 needed = max_length;
  int bufsize = (needed < (2 << 20) ? needed : (2 << 20));

  std::unique_ptr<char[]> buf(new char[bufsize]);

  int64 nread = 0;
  while (needed > 0) {
    nread = read(buf.get(), (bufsize < needed ? bufsize : needed));
    if (nread > 0) {
      output->append(buf.get(), nread);
      needed -= nread;
    } else {
      break;
    }
  }
  return (nread >= 0 ? static_cast<int64>(output->size()) : -1);
}

size_t File::writeString(const std::string& line) {
  return write(line.c_str(), line.size());
}

// bool File::WriteLine(const std::string& line) {
//   if (Write(line.c_str(), line.size()) != line.size()) return false;
//   return Write("\n", 1) == 1;
// }

/////////////////////////////////////////

bool readFileToString(const std::string& file_name, std::string* output) {
  File* file = File::open(file_name, "r");
  if (file == NULL) return false;
  size_t size = file->size();
  return (size <= file->readToString(output, size*100));
}

bool writeStringToFile(const std::string& data, const std::string& file_name) {
  File* file = File::open(file_name, "w");
  if (file == NULL) return false;
  return (file->writeString(data) == data.size() && file->close());
}

namespace {
class NoOpErrorCollector : public google::protobuf::io::ErrorCollector {
 public:
  virtual void AddError(int line, int column, const std::string& message) {}
};
}  // namespace


bool readFileToProto(const std::string& file_name, GProto* proto) {
  DataConfig conf; conf.add_file(file_name);
  return readFileToProto(conf, proto);
}

bool readFileToProto(const DataConfig& name, GProto* proto) {
  File* f = File::open(name, "r");
  if (f == NULL) return false;
  size_t size = 100000;
  if (!name.has_hdfs()) size = f->size();
  std::string str; f->readToString(&str, size*100);

  // Attempt to decode ASCII before deciding binary. Do it in this order because
  // it is much harder for a binary encoding to happen to be a valid ASCII
  // encoding than the other way around. For instance "index: 1\n" is a valid
  // (but nonsensical) binary encoding. We want to avoid printing errors for
  // valid binary encodings if the ASCII parsing fails, and so specify a no-op
  // error collector.
  NoOpErrorCollector error_collector;
  google::protobuf::TextFormat::Parser parser;
  parser.RecordErrorsTo(&error_collector);
  if (parser.ParseFromString(str, proto)) {
    return true;
  }
  if (proto->ParseFromString(str)) {
    return true;
  }
  // Re-parse the ASCII, just to show the diagnostics (we could also get them
  // out of the ErrorCollector but this way is easier).
  google::protobuf::TextFormat::ParseFromString(str, proto);
  LOG(ERROR) << "Could not parse contents of " << name.DebugString();
  return false;
}

void readFileToProtoOrDie(
    const DataConfig& name, GProto* proto) {
  CHECK(readFileToProto(name, proto));
}

void readFileToProtoOrDie(
    const std::string& file_name, GProto* proto) {
  CHECK(readFileToProto(file_name, proto)) << "file_name: " << file_name;
}

bool writeProtoToASCIIFile(
    const GProto& proto, const std::string& file_name) {
  std::string proto_string;
  return google::protobuf::TextFormat::PrintToString(proto, &proto_string) &&
         writeStringToFile(proto_string, file_name);
}

void writeProtoToASCIIFileOrDie(
    const GProto& proto, const std::string& file_name) {
  CHECK(writeProtoToASCIIFile(proto, file_name)) << "file_name: " << file_name;
}

bool writeProtoToFile(
    const GProto& proto, const std::string& file_name) {
  std::string proto_string;
  return proto.AppendToString(&proto_string) &&
      writeStringToFile(proto_string, file_name);
}

void writeProtoToFileOrDie(const GProto& proto,
                           const std::string& file_name) {
  CHECK(writeProtoToFile(proto, file_name)) << "file_name: " << file_name;
}

// TODO read home from $HDFS_HOME if empty
std::string hadoopFS(const HDFSConfig& conf) {
  string str = conf.home() + "/bin/hadoop fs";
  if (conf.has_namenode()) str += " -D fs.default.name=" + conf.namenode();
  if (conf.has_ugi()) str += " -D hadoop.job.ugi=" + conf.ugi();
  return str;
}
#if USE_S3
std::vector<std::string> s3GetFileNamesFromXml(const char* fbuf, int fsize, const xmlChar* nspace, const xmlChar* uri, const xmlChar* xpathExpr,std::string& prefix)
{
    std::vector<std::string> files;
    // get xml
    xmlDocPtr doc = xmlParseMemory(fbuf,fsize);
    // create context
    xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
    // register namespace
    if(xmlXPathRegisterNs(xpathCtx,nspace,uri)==0){
      // execute xpath
      xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(xpathExpr, xpathCtx);
      if(!xmlXPathNodeSetIsEmpty(xpathObj->nodesetval)){
        xmlNodeSetPtr nodes=xpathObj->nodesetval;
        for (int i=0; i < nodes->nodeNr; i++) {
          xmlChar* str = xmlNodeListGetString(doc, nodes->nodeTab[i]->xmlChildrenNode, 1);
          std::string file((char *)str);
          //make sure only files
          if(file.length()>prefix.length()+1 && file.find('/',prefix.length()+1)==std::string::npos)
            files.push_back(file);
          xmlFree(str);
        }
      }
      xmlXPathFreeObject(xpathObj);
    }
    xmlXPathFreeContext(xpathCtx);
    xmlFreeDoc(doc);
    return files;
}
#endif // USE_S3



std::vector<std::string> readFilenamesInDirectory(const std::string& directory) {
  std::vector<std::string> files;
  DIR *dir = opendir(directory.c_str());
  CHECK(dir != NULL) << "failed to open directory " << directory;
  struct dirent *ent;
  while ((ent = readdir (dir)) != NULL)
    files.push_back(string(ent->d_name));
  closedir (dir);
  return files;
}

std::vector<std::string> readFilenamesInDirectory(const DataConfig& directory) {
  CHECK_EQ(directory.file_size(), 1);
  auto dirname = directory.file(0);
  if (directory.has_hdfs()) {
    // read hdfs directory
    std::vector<std::string> files;
    string cmd = hadoopFS(directory.hdfs()) + " -ls " + dirname;

    VLOG(1) << "readFilenamesInDirectory hdfs ls [" << cmd << "]";

    FILE* des = popen(cmd.c_str(), "r"); CHECK(des);
    char line[10000];
    while (fgets(line, 10000, des)) {
      auto ents = split(std::string(line), ' ', true);
      if (ents.size() != 8) continue;
      if (ents[0][0] == 'd') continue;

      // remove tailing line break
      string this_is_file = ents.back();
      if ('\n' == this_is_file.back()) {
        this_is_file.resize(this_is_file.size() - 1);
      }

      files.push_back(this_is_file);
    }
    pclose(des);
    return files;
  }
#if USE_S3
  else if (s3file(dirname)) {
    // open xml
    std::string cmd = "curl -s -X GET "+s3DirectoryUrl(dirname);
    FILE* des = popen(cmd.c_str(), "r");
    CHECK(des);
    // alloc buffer for file
    // TODO check the exact size of file before allocation
    size_t fsize=20000;
    char* fbuf = (char*) malloc (sizeof(char)*fsize);
    CHECK(fbuf);
    memset(fbuf,0,fsize);
    size_t rsize=fread (fbuf,1,fsize,des);
    // execute xpath
    std::string prefix=s3Prefix(dirname);
    std::vector<std::string> files=s3GetFileNamesFromXml(fbuf,fsize,BAD_CAST "ListBucketResult",
      BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/",BAD_CAST "//ListBucketResult:Key",prefix);
    // clean up
    pclose (des);
    free (fbuf);
    return files;
  }
#endif // USE_S3
  else {
    return readFilenamesInDirectory(dirname);
  }
}

string getFilename(const string& full) {
  auto elems = split(full, '/');
  return elems.empty() ? "" : elems.back();
}
string getPath(const string& full) {
  auto elems = split(full, '/');
  if (elems.size() <= 1) return full;
  elems.pop_back();
  return join(elems, "/");
}
string removeExtension(const string& file) {
  auto elems = split(file, '.');
  if (elems.size() <= 1) return file;
  if (elems.back() == "gz" && elems.size() > 2) elems.pop_back();
  elems.pop_back();
  return join(elems, ".");
}

bool dirExists(const std::string& dir) {
  struct stat info;
  if (stat(dir.c_str(), &info) != 0) return false;
  if (info.st_mode & S_IFDIR) return true;
  return false;
}

bool createDir(const std::string& dir) {
  return mkdir(dir.c_str(), 0755) == 0;
}

} // namespace ps
