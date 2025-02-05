#ifndef REQUEST_MESSAGE_H
#define REQUEST_MESSAGE_H

#include <cstdint>
#include <cstring>

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

struct k1_partition final : sbase {
  sint32 partition;
  sint32 current_leader_epoch;
  sint64 fetch_offset;
  sint32 last_fetched_eopich;
  sint64 log_start_offset;
  sint32 partition_max_bytes;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += partition.serialize(buf + sz);
    sz += current_leader_epoch.serialize(buf + sz);
    sz += fetch_offset.serialize(buf + sz);
    sz += last_fetched_eopich.serialize(buf + sz);
    sz += log_start_offset.serialize(buf + sz);
    sz += partition_max_bytes.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += partition.deserialize(buf + sz);
    sz += current_leader_epoch.deserialize(buf + sz);
    sz += fetch_offset.deserialize(buf + sz);
    sz += last_fetched_eopich.deserialize(buf + sz);
    sz += log_start_offset.deserialize(buf + sz);
    sz += partition_max_bytes.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct k1_topic final : sbase {
  suuid topic_id;
  scarray<k1_partition> partitions;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += topic_id.serialize(buf + sz);
    sz += partitions.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += topic_id.deserialize(buf + sz);
    sz += partitions.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct k1_forgotten_topic_data final : sbase {
  suuid topic_id;
  scarray<sint32> partitions;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += topic_id.serialize(buf + sz);
    sz += partitions.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += topic_id.deserialize(buf + sz);
    sz += partitions.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct request_k1_v16 final : sbase {
  request_header_v2* header;
  sint32 max_wait_ms;
  sint32 min_bytes;
  sint32 max_bytes;
  sint8 isolation_level;
  sint32 session_id;
  sint32 session_epoch;
  scarray<k1_topic> topics;
  scarray<k1_forgotten_topic_data> forgotten_topic_data;
  scstring rack_id;
  stagged_fields tagged_fields;
  explicit request_k1_v16(request_header_v2* h) : header(h) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->serialize(buf + sz);
    sz += max_wait_ms.serialize(buf + sz);
    sz += min_bytes.serialize(buf + sz);
    sz += max_bytes.serialize(buf + sz);
    sz += isolation_level.serialize(buf + sz);
    sz += session_id.serialize(buf + sz);
    sz += session_epoch.serialize(buf + sz);
    sz += topics.serialize(buf + sz);
    sz += forgotten_topic_data.serialize(buf + sz);
    sz += rack_id.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->deserialize(buf + sz);
    sz += max_wait_ms.deserialize(buf + sz);
    sz += min_bytes.deserialize(buf + sz);
    sz += max_bytes.deserialize(buf + sz);
    sz += isolation_level.deserialize(buf + sz);
    sz += session_id.deserialize(buf + sz);
    sz += session_epoch.deserialize(buf + sz);
    sz += topics.deserialize(buf + sz);
    sz += forgotten_topic_data.deserialize(buf + sz);
    sz += rack_id.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

#endif
