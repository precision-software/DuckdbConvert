cmake_minimum_required(VERSION 2.8.12)
project(postgres)
set(CMAKE_CXX_STANDARD 17)


include_directories(duckdb/src/include)
link_directories(duckdb/build/debug/src)

add_library(duckconvert STATIC SendDuckType.cpp SendDuckData.cpp send.cpp)

add_executable(typetest  typetest.cpp)
target_link_libraries(typetest duckdb duckconvert)
