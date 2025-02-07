#ifndef INCLUDE_GLOBAL_DATAMAP_HPP_
#define INCLUDE_GLOBAL_DATAMAP_HPP_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "primitive.hpp"
#include "record.hpp"
#include "response_message.hpp"

extern std::unordered_map<std::string,
                          std::vector<std::shared_ptr<res_partition>>>
    topic_uuid_to_partitions;

extern std::unordered_map<std::string,
                          std::unordered_map<int32_t, scarray<sint8>>>
    topic_uuid_to_partition_to_records;

extern std::unordered_map<std::string, std::string> topic_name_to_uuid;

extern void initialize();

#endif  // INCLUDE_GLOBAL_DATAMAP_HPP_
