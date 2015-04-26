# Build the Parameter Server

**Requirement**

The parameter server needs a C++ compiler supporting c++11, such as `gcc` >=
4.7.2 (prefer >= 4.8) or `llvm` >= 3.4.
You can update `gcc` via either  downloading
packages, e.g. [centos](http://linux.web.cern.ch/linux/devtoolset/),
[ubuntu](http://ubuntuhandbook.org/index.php/2013/08/install-gcc-4-8-via-ppa-in-ubuntu-12-04-13-04/),
[mac os x](http://hpc.sourceforge.net/), or building from source, such as for
[centos](http://www.codersvoice.com/a/webbase/install/08/202014/131.html).

**Build the Parameter Server**

Assume `git` is installed:

```bash
git clone https://github.com/dmlc/parameter_server -b dev
cd parameter_server
./script/install_third.sh
make -j8
```

**Customized Building**

You can modify [config.mk](config.mk) to customize the building. You can copy
this file to the upper directory so that the changes will be ignored by git.
