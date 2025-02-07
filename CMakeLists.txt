cmake_minimum_required(VERSION 3.13)

project(codecrafters-kafka)

# set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard

# specify the C++ standard
add_library(compiler_flags INTERFACE)
target_compile_features(compiler_flags INTERFACE cxx_std_23)
target_compile_options(compiler_flags INTERFACE -Wall -Wextra -Wpedantic)

add_subdirectory(src/util)
add_subdirectory(src/cls)
add_subdirectory(src/global)
add_subdirectory(src/api)

find_package(Catch2 3 REQUIRED)

add_executable(test_primitive src/test/test_primitive.cpp)
target_link_libraries(test_primitive PUBLIC Catch2::Catch2WithMain cls compiler_flags)

file(GLOB_RECURSE SOURCE_FILES main.cpp)

add_executable(kafka ${SOURCE_FILES})
target_link_libraries(kafka PUBLIC global util cls api compiler_flags)
