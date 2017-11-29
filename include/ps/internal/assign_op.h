/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 *  Copyright (c) 2015 by Contributors
 * \file   assign_op.h
 * \brief  assignment operator
 * http://en.cppreference.com/w/cpp/language/operator_assignment
 */
#ifndef PS_INTERNAL_ASSIGN_OP_H_
#define PS_INTERNAL_ASSIGN_OP_H_
#include "ps/internal/utils.h"
namespace ps {

enum AssignOp {
  ASSIGN,  // a = b
  PLUS,    // a += b
  MINUS,   // a -= b
  TIMES,   // a *= b
  DIVIDE,  // a -= b
  AND,     // a &= b
  OR,      // a |= b
  XOR      // a ^= b
};

/**
 * \brief return an assignment function: right op= left
 */
template<typename T>
inline void AssignFunc(const T& lhs, AssignOp op, T* rhs) {
  switch (op) {
    case ASSIGN:
      *right = left; break;
    case PLUS:
      *right += left; break;
    case MINUS:
      *right -= left; break;
    case TIMES:
      *right *= left; break;
    case DIVIDE:
      *right /= left; break;
    default:
      LOG(FATAL) << "use AssignOpInt..";
  }
}

/**
 * \brief return an assignment function including bit operations, only
 * works for integers
 */
template<typename T>
inline void AssignFuncInt(const T& lhs, AssignOp op, T* rhs) {
  switch (op) {
    case ASSIGN:
      *right = left; break;
    case PLUS:
      *right += left; break;
    case MINUS:
      *right -= left; break;
    case TIMES:
      *right *= left; break;
    case DIVIDE:
      *right /= left; break;
    case AND:
      *right &= left; break;
    case OR:
      *right |= left; break;
    case XOR:
      *right ^= left; break;
  }
}


}  // namespace ps
#endif  // PS_INTERNAL_ASSIGN_OP_H_
