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


#endif
