#ifndef INCLUDE_GLOBAL_DATAMAP_HPP_
#define INCLUDE_GLOBAL_DATAMAP_HPP_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "response_message.hpp"

extern std::unordered_map<std::string,
                          std::vector<std::shared_ptr<res_partition>>>
    topic_uuid_to_partitions;

extern std::unordered_map<std::string, std::string> topic_name_to_uuid;

extern void initialize();

#endif  // INCLUDE_GLOBAL_DATAMAP_HPP_
