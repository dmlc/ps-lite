workspace(name = "ps-lite")

# Initialize the repository and all dependencies.
#
# The cascade of load() statements and tf_workspace?() calls works around the
# restriction that load() statements need to be at the top of .bzl files.
# E.g. we can not retrieve a new repository with http_archive and then load()
# a macro from that repository in the same file.
load("@//bazel:workspace3.bzl", "tf_workspace3")

tf_workspace3()

load("@//bazel:workspace2.bzl", "tf_workspace2")

tf_workspace2()

load("@//bazel:workspace1.bzl", "tf_workspace1")

tf_workspace1()

load("@//bazel:workspace0.bzl", "tf_workspace0")

tf_workspace0()
