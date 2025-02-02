#ifndef MESSAGE_H
#define MESSAGE_H

#include <cstdint>
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
    // std::cout << "correlation_id: " << correlation_id.val << " consuming " << sz
    //           << std::endl;
    sz += client_id.deserialize(buf + sz);
    // std::cout << "client_id: " << client_id.val << " consuming " << sz
    //           << std::endl;
    sz += tagged_fields.deserialize(buf + sz);
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

struct version_info final : sbase {
  sint16 api_key;
  sint16 min_version;
  sint16 max_version;
  version_info() = default;
  version_info(int16_t ak, int16_t min, int16_t max)
      : api_key(ak), min_version(min), max_version(max) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += api_key.serialize(buf + sz);
    sz += min_version.serialize(buf + sz);
    sz += max_version.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    sz += api_key.deserialize(buf + sz);
    sz += min_version.deserialize(buf + sz);
    sz += max_version.deserialize(buf + sz);
    return sz;
  }
};

struct response_k18_v4 final : sbase {
  response_header_v1* header;
  sint16 error_code;
  scarray<version_info> version_infos;
  sint32 throttle_time_ms;
  stagged_fields tagged_buffer;
  response_k18_v4(response_header_v1* h) : header(h) {}
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

inline int32_t write_message(int8_t* buf, sbase* msg) {
  sint32 size{msg->serialize(buf + sizeof(int32_t))};
  size.serialize(buf);
  return size.val + sizeof(int32_t);
}

#endif
