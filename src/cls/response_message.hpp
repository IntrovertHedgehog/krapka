#ifndef RESPONSE_HEADER_H
#define RESPONSE_HEADER_H
#include <sched.h>

#include <cstdint>
#include <cstring>
#include <scoped_allocator>

#include "./primitive.hpp"

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
  scarray<sint32> replica_nodes;
  scarray<sint32> isr_nodes;
  scarray<sint32> eligible_leader_replicas;
  scarray<sint32> last_known_elr;
  scarray<sint32> offline_replicas;
  stagged_fields tagged_fields;
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
    sz += tagged_fields.serialize(buf + sz);
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
    sz += tagged_fields.deserialize(buf + sz);
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

struct k1_aborted_transaction final : sbase {
  sint64 producer_id;
  sint64 first_offset;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += producer_id.serialize(buf + sz);
    sz += first_offset.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += producer_id.deserialize(buf + sz);
    sz += first_offset.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct res_k1_partition final : sbase {
  sint32 partition_index;
  sint16 error_code;
  sint64 high_watermark;
  sint64 last_stable_offset;
  sint64 log_start_offset;
  scarray<k1_aborted_transaction> aborted_transaction;
  sint32 preferred_read_replica;
  scarray<sint8> records;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += partition_index.serialize(buf + sz);
    sz += error_code.serialize(buf + sz);
    sz += high_watermark.serialize(buf + sz);
    sz += last_stable_offset.serialize(buf + sz);
    sz += log_start_offset.serialize(buf + sz);
    sz += aborted_transaction.serialize(buf + sz);
    sz += preferred_read_replica.serialize(buf + sz);
    sz += records.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += partition_index.deserialize(buf + sz);
    sz += error_code.deserialize(buf + sz);
    sz += high_watermark.deserialize(buf + sz);
    sz += last_stable_offset.deserialize(buf + sz);
    sz += log_start_offset.deserialize(buf + sz);
    sz += aborted_transaction.deserialize(buf + sz);
    sz += preferred_read_replica.deserialize(buf + sz);
    sz += records.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct k1_reponse final : sbase {
  suuid topic_id;
  scarray<res_k1_partition> partitions;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t* buf) {
    int32_t sz{};
    sz += topic_id.serialize(buf + sz);
    sz += partitions.serialize(buf + sz);
    tagged_fields.serialize(buf);
    return sz;
  }
  int32_t deserialize(int8_t* buf) {
    int32_t sz{};
    sz += topic_id.deserialize(buf + sz);
    sz += partitions.deserialize(buf + sz);
    tagged_fields.deserialize(buf);
    return sz;
  }
};

struct response_k1_v16 final : sbase {
  response_header_v1* header;
  sint32 throttle_time_ms;
  sint16 error_code;
  sint32 session_id;
  scarray<k1_reponse> responses;
  stagged_fields tagged_fields;
  explicit response_k1_v16(response_header_v1* h) : header(h) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->serialize(buf + sz);
    sz += throttle_time_ms.serialize(buf + sz);
    sz += error_code.serialize(buf + sz);
    sz += session_id.serialize(buf + sz);
    sz += responses.serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += header->deserialize(buf + sz);
    sz += throttle_time_ms.deserialize(buf + sz);
    sz += error_code.deserialize(buf + sz);
    sz += session_id.deserialize(buf + sz);
    sz += responses.deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

inline int32_t write_message(int8_t* buf, sbase* msg) {
  sint32 size{msg->serialize(buf + sizeof(int32_t))};
  size.serialize(buf);
  return size.val + sizeof(int32_t);
}
#endif
