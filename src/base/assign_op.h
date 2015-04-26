#pragma once
#include "proto/assign_op.pb.h"
#include "glog/logging.h"
namespace ps {
// The cost of the switch is minimal. Once "op" is a constant, the compiler will
// do optimization. see test/assign_op_test.cc

// Returns right op= left. bascial version, works for both floast and intergers
template<typename T>
T& AssignOp(T& right, const T& left, const AssignOpType& op) {
  switch (op) {
    case AssignOpType::ASSIGN:
      right = left; break;
    case AssignOpType::PLUS:
      right += left; break;
    case AssignOpType::MINUS:
      right -= left; break;
    case AssignOpType::TIMES:
      right *= left; break;
    case AssignOpType::DIVIDE:
      right /= left; break;
    default:
      LOG(FATAL) << "use AssignOpI.." ;
  }
  return right;
}

// Returns right op= left. for integers
template<typename T>
T& AssignOpI(T& right, const T& left, const AssignOpType& op) {
  switch (op) {
    case AssignOpType::ASSIGN:
      right = left; break;
    case AssignOpType::PLUS:
      right += left; break;
    case AssignOpType::MINUS:
      right -= left; break;
    case AssignOpType::TIMES:
      right *= left; break;
    case AssignOpType::DIVIDE:
      right /= left; break;
    case AssignOpType::AND:
      right &= left; break;
    case AssignOpType::OR:
      right |= left; break;
    case AssignOpType::XOR:
      right ^= left; break;
  }
  return right;
}

}  // namespace ps
