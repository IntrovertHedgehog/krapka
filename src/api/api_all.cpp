#include "api_all.hpp"

#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "datamap.hpp"
#include "primitive.hpp"
#include "request_message.hpp"
#include "response_message.hpp"

void api_fetch_k1_v16(request_k1_v16* req, response_k1_v16* res) {
  res->responses.val.clear();
  if (req->header->request_api_version.val < API_VERSION_MIN_1 ||
      req->header->request_api_version.val > API_VERSION_MAX_1) {
    res->responses.is_null = false;
    for (k1_topic& topic : req->topics.val) {
      k1_reponse& rep = res->responses.val.emplace_back();
      rep.topic_id = topic.topic_id;
      rep.partitions.is_null = false;
      for (k1_partition& part : topic.partitions.val) {
        res_k1_partition& p = rep.partitions.val.emplace_back();
        p.partition_index = part.partition;
        p.error_code.val = ERR_UNSUPPORTED_VERSION;
      }
    }
  } else {
    // read_log(topic_uuid_to_partitions, topic_name_to_uuid, log_fn);
    res->responses.is_null = false;
    for (k1_topic& topic : req->topics.val) {
      k1_reponse& rep = res->responses.val.emplace_back();
      rep.topic_id = topic.topic_id;

      bool unkown_topic = topic_uuid_to_partitions.find(topic.topic_id.str()) ==
                          topic_uuid_to_partitions.end();

      rep.partitions.is_null = false;
      for (k1_partition& part : topic.partitions.val) {
        res_k1_partition& p = rep.partitions.val.emplace_back();
        p.partition_index = part.partition;
        p.records.is_null = false;
        if (unkown_topic) {
          p.error_code.val = ERR_UNKNOWN_TOPIC;
        } else {
          p.records = topic_uuid_to_partition_to_records[topic.topic_id.str()]
                                                        [p.partition_index.val];
        }
      }
    }
  }
  res->throttle_time_ms.val = 0;
  res->session_id = req->session_id;
}

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

void api_describe_topic_partitions(request_k75_v0* req, response_k75_v0* res) {
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
    // read_log(topic_uuid_to_partitions, topic_name_to_uuid, log_fn);
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
