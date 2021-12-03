# version for the shared libraries, can
# not contain rc or alpha, only numbers.
# Also update tensorflow/core/public/version.h
# and tensorflow/tools/pip_package/setup.py
VERSION = "2.8.0"
VERSION_MAJOR = VERSION.split(".")[0]

# The workspace root, to be used to set workspace 'include' paths in a way that
# will still work correctly when TensorFlow is included as a dependency of an
# external project.
workspace_root = Label("//:WORKSPACE").workspace_root or "."

def clean_dep(target):
    """Returns string to 'target' in @rules_compressor repository.

    Use this function when referring to targets in the @rules_compressor
    repository from macros that may be called from external repositories.
    """

    # A repo-relative label is resolved relative to the file in which the
    # Label() call appears, i.e. @rules_compressor.
    return str(Label(target))

def if_oss(oss_value, google_value = []):
    """Returns one of the arguments based on the non-configurable build env.

    Specifically, it does not return a `select`, and can be used to e.g.
    compute elements of list attributes.
    """
    return oss_value  # copybara:comment_replace return google_value

def if_google(google_value, oss_value = []):
    """Returns one of the arguments based on the non-configurable build env.

    Specifically, it does not return a `select`, and can be used to e.g.
    compute elements of list attributes.
    """
    return oss_value  # copybara:comment_replace return google_value

def filegroup(**kwargs):
    native.filegroup(**kwargs)

def genrule(**kwargs):
    native.genrule(**kwargs)

