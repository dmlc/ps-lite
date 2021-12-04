"""Workspace initialization. Consult the WORKSPACE on how to use it."""

load("@rules_cc_toolchain//:rules_cc_toolchain_deps.bzl", "rules_cc_toolchain_deps")
load("@rules_cc_toolchain//cc_toolchain:cc_toolchain.bzl", "register_cc_toolchains")
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

def workspace():
    rules_cc_toolchain_deps()
    register_cc_toolchains()
    protobuf_deps()

# Alias so it can be loaded without assigning to a different symbol to prevent
# shadowing previous loads and trigger a buildifier warning.
tf_workspace1 = workspace
