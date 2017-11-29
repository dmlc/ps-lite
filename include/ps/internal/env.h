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
 * Copyright (c) 2016 by Contributors
 */
#ifndef PS_INTERNAL_ENV_H_
#define PS_INTERNAL_ENV_H_
#include <cstdlib>
#include <unordered_map>
#include <memory>
#include <string>
namespace ps {

/**
 * \brief Environment configurations
 */
class Environment {
 public:
  /**
   * \brief return the singleton instance
   */
  static inline Environment* Get() {
    return _GetSharedRef(nullptr).get();
  }
  /**
   * \brief return a shared ptr of the singleton instance
   */
  static inline std::shared_ptr<Environment> _GetSharedRef() {
    return _GetSharedRef(nullptr);
  }
  /**
   * \brief initialize the environment
   * \param envs key-value environment variables
   * \return the initialized singleton instance
   */
  static inline Environment* Init(const std::unordered_map<std::string, std::string>& envs) {
    return _GetSharedRef(&envs).get();
  }
  /**
   * \brief find the env value.
   *  User-defined env vars first. If not found, check system's environment
   * \param k the environment key
   * \return the related environment value, nullptr when not found
   */
  const char* find(const char* k) {
    std::string key(k);
    return kvs.find(key) == kvs.end() ? getenv(k) : kvs[key].c_str();
  }

 private:
  explicit Environment(const std::unordered_map<std::string, std::string>* envs) {
    if (envs) kvs = *envs;
  }

  static std::shared_ptr<Environment> _GetSharedRef(
      const std::unordered_map<std::string, std::string>* envs) {
    static std::shared_ptr<Environment> inst_ptr(new Environment(envs));
    return inst_ptr;
  }

  std::unordered_map<std::string, std::string> kvs;
};

}  // namespace ps
#endif  // PS_INTERNAL_ENV_H_
