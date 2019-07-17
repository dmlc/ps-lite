# History

We started to work on the parameter server framework since 2010.

1. The first generation was
designed and optimized for specific algorithms, such as logistic regression and
LDA, to serve the sheer size industrial machine learning tasks (hundreds billions of
examples and features with 10-100TB data size) .

2. Later we tried to build a open-source general purpose framework for machine learning
algorithms. The project is available at [dmlc/parameter_server](https://github.com/dmlc/parameter_server).

3. Given the growing demands from other projects, we created `ps-lite`, which provides a clean data communication API and a
lightweight implementation. The implementation is based on `dmlc/parameter_server`, but we refactored the job launchers, file I/O and machine
learning algorithms codes into different projects such as `dmlc-core` and
`wormhole`.

4. From the experience we learned during developing
   [dmlc/mxnet](https://github.com/dmlc/mxnet), we further refactored the API and implementation from [v1](https://github.com/dmlc/ps-lite/releases/tag/v1). The main
   changes include
   - less library dependencies
   - more flexible user-defined callbacks, which facilitate other language
   bindings
   - let the users, such as the dependency
     engine of mxnet, manage the data consistency
