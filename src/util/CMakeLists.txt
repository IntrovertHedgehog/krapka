add_library(util hexutil.hpp hexutil.cpp)
target_include_directories(util INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(util INTERFACE compiler_flags)
