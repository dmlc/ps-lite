find_package(Threads REQUIRED)

add_executable(connection_test test_connection.cc)
target_link_libraries(connection_test pslite ${CMAKE_THREAD_LIBS_INIT})

add_executable(simple_app_test test_simple_app.cc)
target_link_libraries(simple_app_test pslite ${CMAKE_THREAD_LIBS_INIT})

add_executable(kv_app_test test_kv_app.cc)
target_link_libraries(kv_app_test pslite ${CMAKE_THREAD_LIBS_INIT})

add_executable(kv_app_benchmark_test test_kv_app_benchmark.cc)
target_link_libraries(kv_app_benchmark_test pslite ${CMAKE_THREAD_LIBS_INIT})

add_executable(kv_app_multi_workers_test test_kv_app_multi_workers.cc)
target_link_libraries(kv_app_multi_workers_test pslite ${CMAKE_THREAD_LIBS_INIT})

# Add tests to CTest
enable_testing()
add_test(NAME connection_test COMMAND connection_test)
add_test(NAME simple_app_test COMMAND simple_app_test)
add_test(NAME kv_app_test COMMAND kv_app_test)
add_test(NAME kv_app_benchmark_test COMMAND kv_app_benchmark_test)
add_test(NAME kv_app_multi_workers_test COMMAND kv_app_multi_workers_test)
