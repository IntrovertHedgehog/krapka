#include "api_all.hpp"

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "primitive.hpp"
#include "record.hpp"

void api_api_version_k18_v4(request_k18_v4* req, response_k18_v4* res) {
  if (req->header->request_api_version.val < API_VERSION_MIN_18 ||
      req->header->request_api_version.val > API_VERSION_MAX_18) {
    res->error_code = sint16(ERR_UNSUPPORTED_VERSION);
  } else {
    res->error_code = sint16(0);
    res->version_infos = scarray<version_info>(std::vector<version_info>{
        version_info(18, API_VERSION_MIN_18, API_VERSION_MAX_18),
        version_info(75, API_VERSION_MIN_75, API_VERSION_MAX_75),
        version_info(1, API_VERSION_MIN_1, API_VERSION_MAX_1),
    });
    res->throttle_time_ms = sint32(0);
    res->tagged_buffer = stagged_fields();
  }
}

void api_describe_topic_partitions(request_k75_v0* req, response_k75_v0* res,
                                   std::string const& log_fn) {
  if (req->header->request_api_version.val < API_VERSION_MIN_75 ||
      req->header->request_api_version.val > API_VERSION_MAX_75) {
    std::vector<res_topic_info> topics;
    for (req_topic_info& req_topic : req->topics.val) {
      topics.emplace_back();
      res_topic_info& res_topic = topics.back();
      res_topic.error_code.val = ERR_UNSUPPORTED_VERSION;
      res_topic.name = scnstring(req_topic.name.val);
    }
    res->topics.val = std::move(topics);
    res->topics.is_null = false;
    res->next_cursor.is_null = true;
  } else {
    std::unordered_map<std::string, std::vector<std::shared_ptr<res_partition>>>
        topic_uuid_to_partitions;
    std::unordered_map<std::string, std::string> topic_name_to_uuid;
    int8_t log_fn_read_buf[BUFSIZ];
    FILE* fs = fopen(log_fn.c_str(), "r");
    int32_t fsize = fread(log_fn_read_buf, sizeof(int8_t), BUFSIZ, fs);
    if (fsize == -1) {
      std::cerr << "file reading error" << std::endl;
    }
    int32_t offset{};
    while (offset < fsize) {
      record_batch rb;
      offset += rb.deserialize(log_fn_read_buf + offset);
      std::cout << "file offset " << offset << std::endl;
      for (record& r : rb.records.val) {
        switch (r.value.type.val) {
          case 2: {
            std::shared_ptr<record_value_type2_t> rv =
                std::dynamic_pointer_cast<record_value_type2_t>(r.value.value);
            topic_name_to_uuid[rv->topic_name.val] = rv->topic_uuid.str();
            break;
          }
          case 3:
            std::shared_ptr<record_value_type3_t> rv =
                std::dynamic_pointer_cast<record_value_type3_t>(r.value.value);
            std::shared_ptr<res_partition> p =
                std::make_shared<res_partition>();
            p->error_code.val = 0;
            p->partition_index.val = rv->paritition_id.val;
            topic_uuid_to_partitions[rv->topic_uuid.str()].push_back(p);
        }
      }
    }

    std::vector<res_topic_info> topics;
    for (req_topic_info& req_topic : req->topics.val) {
      topics.emplace_back();
      res_topic_info& res_topic = topics.back();
      res_topic.name = scnstring(req_topic.name.val);
      if (topic_name_to_uuid.find(res_topic.name.val) ==
          topic_name_to_uuid.end()) {
        res_topic.error_code = sint16(ERR_UNKNOWN_TOPIC_OR_PARTITION);
        res_topic.partitions.is_null = false;
      } else {
        std::string topic_uuid = topic_name_to_uuid[res_topic.name.val];
        res_topic.topic_id = suuid(topic_uuid);
        res_topic.partitions.is_null = false;
        std::vector<std::shared_ptr<res_partition>>& p =
            topic_uuid_to_partitions[topic_uuid];
        std::transform(p.begin(), p.end(),
                       std::back_inserter(res_topic.partitions.val),
                       [](std::shared_ptr<res_partition> prp) {
                         return res_partition(*prp);
                       });
      }
    }

    res->topics.val = std::move(topics);
    res->topics.is_null = false;
    res->next_cursor.is_null = true;
  }
}
