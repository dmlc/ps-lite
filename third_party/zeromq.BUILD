load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "libzmq",
    build_args = [
        "-j `nproc`",
    ],
    cache_entries = {
        "NOFORTRAN": "on",
        "BUILD_WITHOUT_LAPACK": "no",
        "ENABLE_DRAFTS": "True",
        "CXXFLAGS": "-D ZMQ_BUILD_DRAFT_API",
    },
    lib_source = ":all_srcs",
    out_shared_libs = select({
        "@bazel_tools//src/conditions:linux_x86_64": ["libzmq.so.5"],
        "@bazel_tools//src/conditions:darwin": ["libzmq.5.dylib"],
    }),
)
