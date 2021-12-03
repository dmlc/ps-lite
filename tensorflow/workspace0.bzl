"""TensorFlow workspace initialization. Consult the WORKSPACE on how to use it."""

load("@rules_compressor//tensorflow:workspace2.bzl", "tf_workspace2")

def workspace():
    tf_workspace2()

# Alias so it can be loaded without assigning to a different symbol to prevent
# shadowing previous loads and trigger a buildifier warning.
tf_workspace0 = workspace
