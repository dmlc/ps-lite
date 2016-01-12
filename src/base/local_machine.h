#include <stdio.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <net/if.h>

#include <string>

namespace ps {

// information of local machine, works on linux (probably some also work for mac)
class LocalMachine {
 public:
  // virtual memory used by my process in MB
  static double VirMem() {
    return getLine("VmSize:") / 1e3;
  }

  // physical memory used by my process in MB
  static double PhyMem() {
    return getLine("VmRSS:") / 1e3;
  }

  // return the IP address for given interface eth0, eth1, ...
  static std::string ip_string(struct ifaddrs * ifa) {
      char addressBuffer[INET6_ADDRSTRLEN];
      std::string rv;
      void * tmpAddrPtr = NULL;
      if (ifa->ifa_addr->sa_family==AF_INET) {
        // is a valid IP4 Address
        tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
        char addressBuffer[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
        rv = addressBuffer;
      } else if (
          (ifa->ifa_addr->sa_family==AF_INET6)) {
        // is a valid IP6 Address
        tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
        inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
        rv = addressBuffer;
      }
    return rv;
  }

  static std::string IP(const std::string& interface) {
    struct ifaddrs * ifAddrStruct = NULL;
    struct ifaddrs * ifa = NULL;
    std::string ret_ip;

    getifaddrs(&ifAddrStruct);
    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr == NULL) continue;

      ret_ip = ip_string(ifa);

      if (!ret_ip.empty() && strncmp(ifa->ifa_name,
                    interface.c_str(),
                    interface.size()) == 0) {
          break;
      }

    }
    if (ifAddrStruct != NULL) freeifaddrs(ifAddrStruct);
    return ret_ip;
  }

  // return the IP address and Interface
  //    the first interface which is not loopback
  //    only support IPv4
  static void pickupAvailableInterfaceAndIP(std::string& interface, std::string& ip) {
    struct ifaddrs * ifAddrStruct = nullptr;
    struct ifaddrs * ifa = nullptr;

    interface.clear();
    ip.clear();
    getifaddrs(&ifAddrStruct);
    for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
      if (nullptr == ifa->ifa_addr) continue;

      std::string tmp_ip = ip_string(ifa);

      if (!tmp_ip.empty() && 0 == (ifa->ifa_flags & IFF_LOOPBACK)) {
          interface = ifa->ifa_name;
          ip = tmp_ip;
          break;
      }
    }

    printf("got %s\n", ip.c_str());
    if (nullptr != ifAddrStruct) freeifaddrs(ifAddrStruct);
    return;
  }

  // return an available port on local machine
  //    only support IPv4
  //    return 0 on failure
  static unsigned short pickupAvailablePort() {
    struct sockaddr_in6 addr;
    addr.sin6_port = htons(0); // have system pick up a random port available for me
    addr.sin6_family = AF_INET6; // IPV4
    addr.sin6_addr = in6addr_any; // set our addr to any interface

    int sock = socket(AF_INET6, SOCK_STREAM, 0);
    if (0 != bind(sock, (struct sockaddr*)&addr, sizeof(struct sockaddr_in6))) {
      perror("bind():");
      return 0;
    }

    socklen_t addr_len = sizeof(struct sockaddr_in6);
    if (0 != getsockname(sock, (struct sockaddr*)&addr, &addr_len)) {
      perror("getsockname():");
      return 0;
    }

    unsigned short ret_port = ntohs(addr.sin6_port);
    close(sock);
    return ret_port;
  }

 private:
  static double getLine(const char *name) {
    FILE* file = fopen("/proc/self/status", "r");
    char line[128];
    int result = -1;
    while (fgets(line, 128, file) != NULL){
      if (strncmp(line, name, strlen(name)) == 0){
        result = parseLine(line);
        break;
      }
    }
    fclose(file);
    return result;
  }

  static int parseLine(char* line){
    int i = strlen(line);
    while (*line < '0' || *line > '9') line++;
    line[i-3] = '\0';
    i = atoi(line);
    return i;
  }
};

} // namespace ps
