<img src="http://parameterserver.org/images/parameterserver.png" alt="Parameter Server" style="width: 500px;">
[![Build Status](https://travis-ci.org/dmlc/ps-lite.svg?branch=master)](https://travis-ci.org/dmlc/ps-lite)

### Introduction

`ps-lite` provides simple yet powerful APIs for the parameter server
framework. For example, a worker node can communicate with the server nodes via
- `Push`: push a list of (key, value) to the server nodes
- `Pull`: pull the values from servers for a given key list
- `Wait`: wait a push or pull finished.

A simple example:

```
  std::vector<uint64_t> key = {1, 3, 5};
  std::vector<float> val = {1, 1, 1};
  std::vector<float> recv_val;

  ps::KVWorker<float> w;
  w.Wait(w.Push(key, val));
  w.Wait(w.Pull(key, &recv_val));
```
See more examples such as zero-copy communication, data consistency control,
variable length value, and user-defined filters for communication compression
in the [tutorial](guide/).

The server nodes support user-defined handles for flexible server-side
programming. And the scheduler node allows for monitoring progress and fault
tolerance.

### How to build

`ps-lite` requires a C++11 compiler such as `g++ >= 4.8` and `git`. You can
install them via
```
sudo apt-get update && sudo apt-get install -y build-essential git
```
on Ubuntu >= 13.10. On
[older Ubuntu](http://ubuntuhandbook.org/index.php/2013/08/install-gcc-4-8-via-ppa-in-ubuntu-12-04-13-04/),
[Centos](http://linux.web.cern.ch/linux/devtoolset/),
[Mac Os X](http://hpc.sourceforge.net/).

Then

```bash
git clone https://github.com/dmlc/ps-lite
cd ps-lite && make -j8
```

## More examples

`ps-lite` has been used by several DMLC projects and also in several companies'
internal system.
- [Wormhole](https://github.com/dmlc/wormhole) for distributed linear method,
  factorization machine, etc
- [CXXNET](https://github.com/dmlc/cxxnet) and
  [Minverva](https://github.com/minerva-developers/minerva) for distributed deep
  neural networks.

## Reference

```
@inproceedings{li2014scaling,
title={Scaling distributed machine learning with the parameter server},
author={Li, Mu and Andersen, David G and Park, Jun Woo and Smola, Alexander J and Ahmed, Amr and Josifovski, Vanja and Long, James and Shekita, Eugene J and Su, Bor-Yiing},
booktitle={Proc. OSDI},
pages={583--598},
year={2014}
}

@inproceedings{li2014communication,
title={Communication efficient distributed machine learning with the parameter server},
author={Li, Mu and Andersen, David G and Smola, Alex J and Yu, Kai},
booktitle={NIPS},
pages={19--27},
year={2014}
}
```
