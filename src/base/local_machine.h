#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
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
    } else if (ifa->ifa_addr->sa_family==AF_INET6) {
      // is a valid IP6 Address
      tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
    }

    if (tmpAddrPtr) {
      inet_ntop(ifa->ifa_addr->sa_family, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
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

    if (nullptr != ifAddrStruct) freeifaddrs(ifAddrStruct);
    return;
  }

  // return an available port on local machine
  //    return 0 on failure
  static unsigned short pickupAvailablePort() {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    struct addrinfo *result, *rp;

    if (0 != getaddrinfo("localhost", NULL, &hints, &result)) {
      perror("getaddrinfo():");
      return 0;
    }

    unsigned short ret_port = 0;
    for (rp = result; rp != nullptr; rp = rp->ai_next) {
      int sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
      if (sfd == -1) {
        perror("socket():");
      }

      if (bind(sfd, rp->ai_addr, rp->ai_addrlen) != 0) {
        perror("bind():");
        goto finish;
      }

      if (0 != getsockname(sfd, rp->ai_addr, &rp->ai_addrlen)) {
        perror("getsockname():");
        goto finish;
      }

      char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
      if (0 != getnameinfo(rp->ai_addr, rp->ai_addrlen, hbuf, sizeof(hbuf), sbuf, sizeof(sbuf), NI_NUMERICSERV)) {
        perror("getnameinfo():");
        goto finish;
      }
      ret_port = atoi(sbuf);
      close(sfd);
      break;
finish:
      close(sfd);
    }
    freeaddrinfo(result);
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
