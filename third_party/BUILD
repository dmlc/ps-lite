load("@bazel_skylib//rules:build_test.bzl", "build_test")

licenses(["notice"])  # Apache 2.0

package(default_visibility = ["//visibility:public"])

build_test(
    name = "build_test",
    targets = [
        "@lz4",
        "@zeromq//:libzmq",
    ],
    visibility = ["//:__pkg__"],
)
