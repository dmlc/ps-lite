load("@rules_cc//cc:defs.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

# For each .proto file, a proto_library target should be defined. This target
# is not bound to any particular language. Instead, it defines the dependency
# graph of the .proto files (i.e., proto imports) and serves as the provider
# of .proto source files to the protocol compiler.
#
# Remote repository "com_google_protobuf" must be defined to use this rule.
proto_library(
    name = "meta_proto",
    srcs = ["src/meta.proto"],
    deps = ["@com_google_protobuf//:timestamp_proto"],
)

# The cc_proto_library rule generates C++ code for a proto_library rule. It
# must have exactly one proto_library dependency. If you want to use multiple
# proto_library targets, create a separate cc_proto_library target for each
# of them.
#
# Remote repository "com_google_protobuf_cc" must be defined to use this rule.
cc_proto_library(
    name = "meta_cc_proto",
    deps = [":meta_proto"],
)

# Public C++ headers for the Flatbuffers library.
filegroup(
    name = "ps_headers",
    srcs = glob([
        "include/dmlc/*.h",
        "include/ps/*.h",
        "include/ps/internal/*.h",
    ]),
)

cc_library(
    name = "meta_cc_proto_hdrs",
    hdrs = [
        ":meta_cc_proto",
    ],
    strip_include_prefix = "src",
)

cc_library(
    name = "ps-lite",
    srcs = glob([
        "src/windows/*.h",
        "src/*.h",
        "src/*.cc",
    ]),
    hdrs = [
        "//:ps_headers",
    ],
    linkstatic = 1,
    strip_include_prefix = "/include",
    visibility = ["//visibility:public"],
    deps = [
        ":meta_cc_proto",
        ":meta_cc_proto_hdrs",
        "@zeromq//:libzmq",
    ],
)
