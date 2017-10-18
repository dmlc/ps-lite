#include "ps/ps.h"

int main(int argc, char *argv[]) {
  ps::Start(0);
  // do nothing
  ps::Finalize(0, true);
  return 0;
}
