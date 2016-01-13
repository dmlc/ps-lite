/**
 *  Copyright (c) 2015 by Contributors
 * @file   network_utils.h
 * @brief  network utilities
 */
#ifndef PS_NETWORK_UTILS_H_
#define PS_NETWORK_UTILS_H_
#include <unistd.h>
#ifdef _MSC_VER
#include <tchar.h>
#include <windows.h>
#include <winsock.h>
#include <iphlpapi.h>
#undef interface
#else
#include <net/if.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#endif
#include <string>

namespace ps {


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


/**
 * \brief return the IP address for given interface eth0, eth1, ...
 */
void GetIP(const std::string& interface, std::string* ip) {
#ifdef _MSC_VER
  typedef std::basic_string<TCHAR> tstring;
  // Try to get the Adapters-info table, so we can given useful names to the IP
  // addresses we are returning.  Gotta call GetAdaptersInfo() up to 5 times to handle
  // the potential race condition between the size-query call and the get-data call.
  // I love a well-designed API :^P
  IP_ADAPTER_INFO * pAdapterInfo = NULL;
  {
    ULONG bufLen = 0;
    for (int i = 0; i < 5; i++) {
      DWORD apRet = GetAdaptersInfo(pAdapterInfo, &bufLen);
      if (apRet == ERROR_BUFFER_OVERFLOW) {
        free(pAdapterInfo);  // in case we had previously allocated it
        pAdapterInfo = static_cast<IP_ADAPTER_INFO*>(malloc(bufLen));
      } else if (apRet == ERROR_SUCCESS) {
        break;
      } else {
        free(pAdapterInfo);
        pAdapterInfo = NULL;
        break;
      }
    }
  }
  if (pAdapterInfo) {
    tstring keybase = _T(
        "SYSTEM\\CurrentControlSet\\Control\\Network\\{4D36E972-E325-11CE-BFC1-08002BE10318}\\");
    tstring connection = _T("\\Connection");

    IP_ADAPTER_INFO *curpAdapterInfo = pAdapterInfo;
    while (curpAdapterInfo->Next) {
      HKEY hKEY;
      std::string AdapterName = curpAdapterInfo->AdapterName;
      // GUID only ascii
      tstring key_set = keybase + tstring(AdapterName.begin(), AdapterName.end()) + connection;
      LPCTSTR data_Set = key_set.c_str();
      LPCTSTR dwValue = NULL;
      if (ERROR_SUCCESS ==
          ::RegOpenKeyEx(HKEY_LOCAL_MACHINE, data_Set, 0, KEY_READ, &hKEY)) {
        DWORD dwSize = 0;
        DWORD dwType = REG_SZ;
        if (ERROR_SUCCESS ==
            ::RegQueryValueEx(hKEY, _T("Name"), 0, &dwType, (LPBYTE)dwValue, &dwSize)) {
          dwValue = new TCHAR[dwSize];
          if (ERROR_SUCCESS ==
              ::RegQueryValueEx(hKEY, _T("Name"), 0, &dwType, (LPBYTE)dwValue, &dwSize)) {
            // interface name must only ascii
            tstring tstr = dwValue;
            std::string s(tstr.begin(), tstr.end());
            if (s == interface) {
              *ip = std::string(curpAdapterInfo->IpAddressList.IpAddress.String,
                                (curpAdapterInfo->IpAddressList.IpAddress.String + 16));
              break;
            }
          }
        }
        ::RegCloseKey(hKEY);
      }
      curpAdapterInfo = curpAdapterInfo->Next;
    }
    free(pAdapterInfo);
  }
#else
  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;

  getifaddrs(&ifAddrStruct);
  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) continue;
    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr == NULL) continue;

      *ip = ip_string(ifa);

      if (ip->empty() && strncmp(ifa->ifa_name,
            interface.c_str(),
            interface.size()) == 0) {
        break;
      }

    }
  }
  if (ifAddrStruct != NULL) freeifaddrs(ifAddrStruct);
#endif
}


/**
 * \brief return the IP address and Interface the first interface which is not
 * loopback
 *
 * only support IPv4
 */
void GetAvailableInterfaceAndIP(
    std::string* interface, std::string* ip) {
#ifdef _MSC_VER
  typedef std::basic_string<TCHAR> tstring;
  IP_ADAPTER_INFO * pAdapterInfo = NULL;
  {
    ULONG bufLen = 0;
    for (int i = 0; i < 5; i++) {
      DWORD apRet = GetAdaptersInfo(pAdapterInfo, &bufLen);
      if (apRet == ERROR_BUFFER_OVERFLOW) {
        free(pAdapterInfo);  // in case we had previously allocated it
        pAdapterInfo = static_cast<IP_ADAPTER_INFO*>(malloc(bufLen));
      } else if (apRet == ERROR_SUCCESS) {
        break;
      } else {
        free(pAdapterInfo);
        pAdapterInfo = NULL;
        break;
      }
    }
  }
  if (pAdapterInfo) {
    tstring keybase = _T(
        "SYSTEM\\CurrentControlSet\\Control\\Network\\{4D36E972-E325-11CE-BFC1-08002BE10318}\\");
    tstring connection = _T("\\Connection");

    IP_ADAPTER_INFO *curpAdapterInfo = pAdapterInfo;
    HKEY hKEY = NULL;
    while (curpAdapterInfo->Next) {
      std::string curip = std::string(
          curpAdapterInfo->IpAddressList.IpAddress.String,
          (curpAdapterInfo->IpAddressList.IpAddress.String + 16));
      curip = std::string(curip.c_str());
      if (curip == "127.0.0.1") {
        curpAdapterInfo = curpAdapterInfo->Next;
        continue;
      }
      if (curip == "0.0.0.0") {
        curpAdapterInfo = curpAdapterInfo->Next;
        continue;
      }

      std::string AdapterName = curpAdapterInfo->AdapterName;
      // GUID only ascii
      tstring key_set = keybase + tstring(AdapterName.begin(), AdapterName.end()) + connection;
      LPCTSTR data_Set = key_set.c_str();
      LPCTSTR dwValue = NULL;
      if (ERROR_SUCCESS ==
          ::RegOpenKeyEx(HKEY_LOCAL_MACHINE, data_Set, 0, KEY_READ, &hKEY)) {
        DWORD dwSize = 0;
        DWORD dwType = REG_SZ;
        if (ERROR_SUCCESS ==
            ::RegQueryValueEx(hKEY, _T("Name"), 0, &dwType, (LPBYTE)dwValue, &dwSize)) {
          dwValue = new TCHAR[dwSize];
          if (ERROR_SUCCESS ==
              ::RegQueryValueEx(hKEY, _T("Name"), 0, &dwType, (LPBYTE)dwValue, &dwSize)) {
            // interface name must only ascii
            tstring tstr = dwValue;
            std::string s(tstr.begin(), tstr.end());

            *interface = s;
            *ip = curip;
            break;
          }
        }
        ::RegCloseKey(hKEY);
        hKEY = NULL;
      }
      curpAdapterInfo = curpAdapterInfo->Next;
    }
    if (hKEY != NULL) {
      ::RegCloseKey(hKEY);
    }
    free(pAdapterInfo);
  }
#else
  struct ifaddrs * ifAddrStruct = nullptr;
  struct ifaddrs * ifa = nullptr;

  interface->clear();
  ip->clear();
  getifaddrs(&ifAddrStruct);
  for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
    if (nullptr == ifa->ifa_addr) continue;

    std::string tmp_ip = ip_string(ifa);

    if (!tmp_ip.empty() && 0 == (ifa->ifa_flags & IFF_LOOPBACK)) {
      *interface = ifa->ifa_name;
      *ip = tmp_ip;
      break;
    }
  }
  if (nullptr != ifAddrStruct) freeifaddrs(ifAddrStruct);
  return;
#endif
}

/**
 * \brief return an available port on local machine
 *
 * only support IPv4
 * \return 0 on failure
 */
int GetAvailablePort() {
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
#ifdef  _MSC_VER
    closesocket(sfd);
#else
    close(sfd);
#endif
  }
  freeaddrinfo(result);
  return ret_port;
}


}  // namespace ps
#endif  // PS_NETWORK_UTILS_H_
