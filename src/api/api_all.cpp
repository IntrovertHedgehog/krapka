#include "api_all.hpp"

#include <algorithm>
#include <iostream>
#include <ostream>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "message.hpp"
#include "primitive.hpp"

void api_api_version_k18_v4(request_k18_v4* req, response_k18_v4* res) {
  if (req->header->request_api_version.val < API_VERSION_MIN_18 ||
      req->header->request_api_version.val > API_VERSION_MAX_18) {
    res->error_code = sint16(ERR_UNSUPPORTED_VERSION);
  } else {
    res->error_code = sint16(0);
    res->version_infos = scarray<version_info>(std::vector<version_info>{
        version_info(18, API_VERSION_MIN_18, API_VERSION_MAX_18),
        version_info(75, API_VERSION_MIN_75, API_VERSION_MAX_75),
    });
    res->throttle_time_ms = sint32(0);
    res->tagged_buffer = stagged_fields();
  }
}

void api_describe_topic_partitions(request_k75_v0* req, response_k75_v0* res) {
  if (req->header->request_api_version.val < API_VERSION_MIN_75 ||
      req->header->request_api_version.val > API_VERSION_MAX_75) {
    // not implemented
  } else {
    std::vector<res_topic_info> topics;
    for (req_topic_info& req_topic : req->topics.val) {
      topics.emplace_back();
      res_topic_info& res_topic = topics.back();
      res_topic.error_code = sint16(ERR_UNKNOWN_TOPIC_OR_PARTITION);
      res_topic.name = scnstring(req_topic.name.val);
      res_topic.partitions.is_null = false;
    }
    res->topics.val = std::move(topics);
    res->topics.is_null = false;
    res->next_cursor.is_null = true;
  }
}
