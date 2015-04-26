namespace ps {

// DISALLOW_COPY_AND_ASSIGN disallows the copy and operator= functions.
// It goes in the private: declarations in a class.
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#define SINGLETON(Typename)                     \
  static Typename& instance() {                 \
    static Typename e;                          \
    return e;                                  \
  }

} // namespace ps
