cmake_minimum_required(VERSION 3.12)
project(iotdb_rest)

set(CMAKE_CXX_STANDARD 98)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

add_executable(iotdb_rest examples.cpp rest_client.cpp)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
set(CURL_INCLUDE_DIR /usr/include/x86_64-linux-gnu) # 头文件路径
set(CURL_LIBRARIES /usr/lib/x86_64-linux-gnu/libcurl.so) # 库文件路径

set(JSON_CPP_INCLUDE_DIR /usr/include/x86_64-linux-gnu) # 头文件路径
set(JSON_CPP_LIBRARIES /usr/local/lib/libjsoncpp.so) # 库文件路径

include_directories(${CURL_INCLUDE_DIR})
target_link_libraries(iotdb_rest ${CURL_LIBRARIES}  ${JSON_CPP_LIBRARIES})