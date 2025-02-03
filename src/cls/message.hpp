#ifndef MESSAGE_H
#define MESSAGE_H

#include <cstdint>
#include <cstring>
#include <execution>
#include <iostream>
#include <memory>
#include <type_traits>

#include "./primitive.hpp"

struct request_header_v2 final : sbase {
  sint16 request_api_key;
  sint16 request_api_version;
  sint32 correlation_id;
  snstring client_id;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += request_api_key.serialize(buf + sz);
    sz += request_api_version.serialize(buf + sz);
    sz += correlation_id.serialize(buf + sz);
    sz += client_id.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += request_api_key.deserialize(buf + sz);
    // std::cout << "request_api_key: " << request_api_key.val << " consuming "
    //           << sz << std::endl;
    sz += request_api_version.deserialize(buf + sz);
    // std::cout << "request_api_version: " << request_api_version.val
    //           << " consuming " << sz << std::endl;
    sz += correlation_id.deserialize(buf + sz);
    // std::cout << "correlation_id: " << correlation_id.val << " consuming " <<
    // sz
    //           << std::endl;
    sz += client_id.deserialize(buf + sz);
    // std::cout << "client_id: " << client_id.val << " consuming " << sz
    //           << std::endl;
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};


struct response_header_v0 final : sbase {
  sint32 correlation_id;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += correlation_id.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += correlation_id.deserialize(buf + sz);
    return sz;
  }
};

struct response_header_v1 final : sbase {
  sint32 correlation_id;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += correlation_id.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += correlation_id.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct request_k18_v4 final : sbase {
  request_header_v2* header;
  scstring client_software_name;
  scstring client_software_version;
  stagged_fields tagged_fields;
  request_k18_v4(request_header_v2* h) : header(h) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += client_software_name.serialize(buf + sz);
    sz += client_software_version.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += client_software_name.deserialize(buf + sz);
    sz += client_software_version.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct req_topic_info final : sbase {
  scstring name;
  stagged_fields tagged_buffer;
  int32_t serialize(int8_t* buf) {
    int32_t sz{};
    sz += name.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) {
    int32_t sz{};
    sz += name.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct topic_cursor final : sbase {
  scstring topic_name;
  sint32 partition_index;
  stagged_fields tagged_buffer;
  int32_t serialize(int8_t* buf) {
    int32_t sz{};
    sz += topic_name.serialize(buf + sz);
    sz += partition_index.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) {
    int32_t sz{};
    sz += topic_name.deserialize(buf + sz);
    sz += partition_index.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct request_k75_v0 final : sbase {
  request_header_v2* header;
  scarray<req_topic_info> topics;
  sint32 response_partition_limit;
  topic_cursor cursor;
  stagged_fields tagged_buffer;
  request_k75_v0(request_header_v2* h) : header(h) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += topics.serialize(buf + sz);
    sz += response_partition_limit.serialize(buf + sz);
    sz += cursor.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += topics.deserialize(buf + sz);
    sz += response_partition_limit.serialize(buf + sz);
    sz += cursor.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct version_info final : sbase {
  sint16 api_key;
  sint16 min_version;
  sint16 max_version;
  stagged_fields tagged_buffer;
  version_info() = default;
  version_info(int16_t ak, int16_t min, int16_t max)
      : api_key(ak), min_version(min), max_version(max) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += api_key.serialize(buf + sz);
    sz += min_version.serialize(buf + sz);
    sz += max_version.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += api_key.deserialize(buf + sz);
    sz += min_version.deserialize(buf + sz);
    sz += max_version.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct response_k18_v4 final : sbase {
  response_header_v0* header;
  sint16 error_code;
  scarray<version_info> version_infos;
  sint32 throttle_time_ms;
  stagged_fields tagged_buffer;
  response_k18_v4(response_header_v0* h) : header(h) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->serialize(buf + sz);
    sz += error_code.serialize(buf + sz);
    sz += version_infos.serialize(buf + sz);
    sz += throttle_time_ms.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->deserialize(buf + sz);
    sz += error_code.deserialize(buf + sz);
    sz += version_infos.deserialize(buf + sz);
    sz += throttle_time_ms.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct res_partition final : sbase {
  sint16 error_code;
  sint32 partition_index;
  sint32 leader_id;
  sint32 leader_epoch;
  sint32 replica_nodes;
  sint32 isr_nodes;
  sint32 eligible_leader_replicas;
  sint32 last_known_elr;
  sint32 offline_replicas;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += error_code.serialize(buf + sz);
    sz += partition_index.serialize(buf + sz);
    sz += leader_id.serialize(buf + sz);
    sz += leader_epoch.serialize(buf + sz);
    sz += replica_nodes.serialize(buf + sz);
    sz += isr_nodes.serialize(buf + sz);
    sz += eligible_leader_replicas.serialize(buf + sz);
    sz += last_known_elr.serialize(buf + sz);
    sz += offline_replicas.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += error_code.deserialize(buf + sz);
    sz += partition_index.deserialize(buf + sz);
    sz += leader_id.deserialize(buf + sz);
    sz += leader_epoch.deserialize(buf + sz);
    sz += replica_nodes.deserialize(buf + sz);
    sz += isr_nodes.deserialize(buf + sz);
    sz += eligible_leader_replicas.deserialize(buf + sz);
    sz += last_known_elr.deserialize(buf + sz);
    sz += offline_replicas.deserialize(buf + sz);
    return sz;
  }
};

struct res_topic_info final : sbase {
  sint16 error_code;
  scnstring name;
  suuid topic_id;
  sbool is_internal;
  scarray<res_partition> partitions;
  sint32 topic_authorized_operations;
  stagged_fields tagged_buffer;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += error_code.serialize(buf + sz);
    sz += name.serialize(buf + sz);
    sz += topic_id.serialize(buf + sz);
    sz += is_internal.serialize(buf + sz);
    sz += partitions.serialize(buf + sz);
    sz += topic_authorized_operations.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += error_code.deserialize(buf + sz);
    sz += name.deserialize(buf + sz);
    sz += topic_id.deserialize(buf + sz);
    sz += is_internal.deserialize(buf + sz);
    sz += partitions.deserialize(buf + sz);
    sz += topic_authorized_operations.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct res_topic_next_cursor final : sbase {
  bool is_null;
  scstring topic_name;
  sint32 partition_index;
  stagged_fields tagged_buffer;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    if (is_null) {
      sz += sint8(-1).serialize(buf + sz);
    } else {
      sz += topic_name.serialize(buf + sz);
      sz += partition_index.serialize(buf + sz);
      sz += tagged_buffer.serialize(buf + sz);
    }
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += topic_name.deserialize(buf + sz);
    sz += partition_index.deserialize(buf + sz);
    sz += tagged_buffer.deserialize(buf + sz);
    return sz;
  }
};

struct response_k75_v0 final : sbase {
  response_header_v1* header;
  sint32 throttle_time_ms;
  scarray<res_topic_info> topics;
  res_topic_next_cursor next_cursor;
  stagged_fields tagged_buffer;
  explicit response_k75_v0(response_header_v1* h) : header(h) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->serialize(buf + sz);
    sz += throttle_time_ms.serialize(buf + sz);
    sz += topics.serialize(buf + sz);
    sz += next_cursor.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->serialize(buf + sz);
    sz += throttle_time_ms.serialize(buf + sz);
    sz += topics.serialize(buf + sz);
    sz += next_cursor.serialize(buf + sz);
    sz += tagged_buffer.serialize(buf + sz);
    return sz;
  }
};

inline int32_t write_message(int8_t* buf, sbase* msg) {
  sint32 size{msg->serialize(buf + sizeof(int32_t))};
  size.serialize(buf);
  return size.val + sizeof(int32_t);
}

#endif
