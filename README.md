<img src="http://parameterserver.org/images/parameterserver.png" alt="Parameter Server" style="width: 500px;">

<!-- [xx](http://parameterserver.org/images/parameterserver.png) -->

[![Build Status](https://travis-ci.org/dmlc/ps-lite.svg?branch=master)](https://travis-ci.org/dmlc/ps-lite)

The parameter server is a distributed system scaling to industry size machine
learning problems. It provides asynchronous and zero-copy key-value pair
communications between worker machines and server machines. It also supports
flexiable data consistency model, data filters, and flexiable server machine
programming.

- [Tutorial](guide/)
- [API Document](doc/)
- [Wiki](https://github.com/dmlc/parameter_server/wiki/)
- How to [build](make/)
- Examples
  - [Linear method](example/linear)
  - Deep neural network, see [CXXNET](https://github.com/dmlc/cxxnet) and [Minverva](https://github.com/minerva-developers/minerva)
- Research papers: System
  [OSDI'14](http://www.cs.cmu.edu/~muli/file/parameter_server_osdi14.pdf),
  Algorithm [NIPS'14](http://www.cs.cmu.edu/~muli/file/parameter_server_nips14.pdf)
