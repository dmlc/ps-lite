"""Workspace initialization. Consult the WORKSPACE on how to use it."""

# Import third party config rules.
load("//bazel:tensorflow.bzl", "check_bazel_version_at_least")

# Import external repository rules.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Define all external repositories
def _tf_repositories():
    """All external dependencies for TF builds."""

    # To update any of the dependencies bellow:
    # a) update URL and strip_prefix to the new git commit hash
    # b) get the sha256 hash of the commit by running:
    #    curl -L <url> | sha256sum
    # and update the sha256 with the result.

    http_archive(
        name = "com_google_protobuf",
        sha256 = "25f1292d4ea6666f460a2a30038eef121e6c3937ae0f61d610611dfb14b0bd32",
        strip_prefix = "protobuf-3.19.1",
        urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.19.1.zip"],
    )

    http_archive(
        name = "zeromq",
        sha256 = "805e3feab885c027edad3d09c4ac1a7e9bba9a05eac98f36127520e7af875010",
        strip_prefix = "libzmq-4.3.4",
        build_file = Label("//third_party:zeromq.BUILD"),
        urls = ["https://github.com/zeromq/libzmq/archive/v4.3.4.zip"],
    )

def workspace():
    # Check the bazel version before executing any repository rules, in case
    # those rules rely on the version we require here.
    check_bazel_version_at_least("1.0.0")

    # Import all other repositories. This should happen before initializing
    # any external repositories, because those come with their own
    # dependencies. Those recursive dependencies will only be imported if they
    # don't already exist (at least if the external repository macros were
    # written according to common practice to query native.existing_rule()).
    _tf_repositories()

# Alias so it can be loaded without assigning to a different symbol to prevent
# shadowing previous loads and trigger a buildifier warning.
tf_workspace2 = workspace
