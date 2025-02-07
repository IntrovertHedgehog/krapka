#ifndef INCLUDE_CLS_RECORD_HPP_
#define INCLUDE_CLS_RECORD_HPP_
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>

#include "hexutil.hpp"
#include "primitive.hpp"

struct record_string_t final : sbase {
  std::string val;
  bool is_null{true};
  record_string_t() = default;
  record_string_t(std::string v) : val(v), is_null(false) {}
  int32_t serialize(int8_t *buf) override {
    if (is_null) {
      return svint(-1).serialize(buf);
    } else {
      int32_t sz{};
      sz += svint(val.size()).serialize(buf + sz);
      std::copy(val.c_str(), val.c_str() + val.size(),
                reinterpret_cast<char *>(buf + sz));
      return sz + val.size();
    }
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    svint len;
    sz += len.deserialize(buf + sz);
    val.clear();
    if (len.val == -1) {
      is_null = true;
    } else {
      val.reserve(len.val);
      std::copy(reinterpret_cast<char *>(buf + sz),
                reinterpret_cast<char *>(buf + sz + len.val),
                std::back_inserter(val));
      sz += len.val;
    }
    return sz;
  }
};

struct record_value_gen_t : sbase {};

// feature level record
struct record_value_type12_t final : record_value_gen_t {
  scstring name;
  sint16 feature_level;
  int32_t serialize(int8_t *buf) override {
    int32_t sz{};
    sz += name.serialize(buf + sz);
    sz += feature_level.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    sz += name.deserialize(buf + sz);
    sz += feature_level.deserialize(buf + sz);
    return sz;
  }
};

// topic record
struct record_value_type2_t final : record_value_gen_t {
  scstring topic_name;
  suuid topic_uuid;
  int32_t serialize(int8_t *buf) override {
    int32_t sz{};
    sz += topic_name.serialize(buf + sz);
    sz += topic_uuid.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    sz += topic_name.deserialize(buf + sz);
    sz += topic_uuid.deserialize(buf + sz);
    return sz;
  }
};

// partition record
struct record_value_type3_t final : record_value_gen_t {
  sint32 paritition_id;
  suuid topic_uuid;
  scarray<sint32> replica_array;
  scarray<sint32> in_sync_replica_array;
  scarray<sint32> removing_replica_array;
  scarray<sint32> adding_replica_array;
  sint32 leader;
  sint32 leader_epoch;
  sint32 partition_epoch;
  scarray<suuid> directories_array;
  int32_t serialize(int8_t *buf) override {
    int32_t sz{};
    sz += paritition_id.serialize(buf + sz);
    sz += topic_uuid.serialize(buf + sz);
    sz += replica_array.serialize(buf + sz);
    sz += in_sync_replica_array.serialize(buf + sz);
    sz += removing_replica_array.serialize(buf + sz);
    sz += adding_replica_array.serialize(buf + sz);
    sz += leader.serialize(buf + sz);
    sz += leader_epoch.serialize(buf + sz);
    sz += partition_epoch.serialize(buf + sz);
    sz += directories_array.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    sz += paritition_id.deserialize(buf + sz);
    sz += topic_uuid.deserialize(buf + sz);
    sz += replica_array.deserialize(buf + sz);
    sz += in_sync_replica_array.deserialize(buf + sz);
    sz += removing_replica_array.deserialize(buf + sz);
    sz += adding_replica_array.deserialize(buf + sz);
    sz += leader.deserialize(buf + sz);
    sz += leader_epoch.deserialize(buf + sz);
    sz += partition_epoch.deserialize(buf + sz);
    sz += directories_array.deserialize(buf + sz);
    return sz;
  }
};

struct record_value_t final : sbase {
  // record len in signed vint
  sint8 frame_version;
  sint8 type;
  sint8 version;
  std::shared_ptr<record_value_gen_t> value;
  stagged_fields tagged_fields;
  int32_t serialize(int8_t *buf) override {
    if (!value) {
      std::cerr << "record value is null" << std::endl;
    }
    int32_t sz{};
    svint len;
    sz += len.serialize(buf + sz);
    sz += frame_version.serialize(buf + sz);
    sz += type.serialize(buf + sz);
    sz += version.serialize(buf + sz);
    sz += value->serialize(buf + sz);
    sz += tagged_fields.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    std::cout << "record value from " << tohex(buf, 30) << std::endl;
    svint len;
    sz += len.deserialize(buf + sz);
    sz += frame_version.deserialize(buf + sz);
    sz += type.deserialize(buf + sz);
    sz += version.deserialize(buf + sz);
    std::cout << "parsing record val type " << (int)type.val << " at " << sz
              << std::endl;
    switch (type.val) {
      case 2:
        value.reset(new record_value_type2_t());
        break;
      case 3:
        value.reset(new record_value_type3_t());
        break;
      case 12:
        value.reset(new record_value_type12_t());
        break;
    }
    sz += value->deserialize(buf + sz);
    sz += tagged_fields.deserialize(buf + sz);
    return sz;
  }
};

struct batch_header final : sbase {
  int32_t serialize(int8_t *buf) override { return 0; }
  int32_t deserialize(int8_t *buf) override { return 0; }
};

struct record final : sbase {
  // length signed v int;
  sint8 attributes;
  svlong timestamp_delta;
  svint offset_delta;
  record_string_t key;
  record_value_t value;
  scarray<batch_header> headers;
  int32_t serialize(int8_t *buf) override {
    int32_t sz{};
    svint len;
    sz += len.serialize(buf + sz);
    sz += attributes.serialize(buf + sz);
    sz += timestamp_delta.serialize(buf + sz);
    sz += offset_delta.serialize(buf + sz);
    sz += key.serialize(buf + sz);
    sz += value.serialize(buf + sz);
    sz += headers.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    svint len;
    std::cout << "record dese" << tohex(buf, 16) << std::endl;
    sz += len.deserialize(buf + sz);
    sz += attributes.deserialize(buf + sz);
    sz += timestamp_delta.deserialize(buf + sz);
    sz += offset_delta.deserialize(buf + sz);
    sz += key.deserialize(buf + sz);
    std::cout << "len til value " << sz << std::endl;
    sz += value.deserialize(buf + sz);
    sz += headers.deserialize(buf + sz);
    std::cout << "done record " << tohex(buf + sz, 16) << std::endl;
    return sz;
  }
};

struct record_batch final : sbase {
  sint64 base_offset;
  sint32 batch_length;
  sint32 partition_leader_epoch;
  sint8 magic_byte;
  sint32 crc;
  sint16 attributes;
  sint32 last_offset_data;
  suint64 base_timestamp;
  suint64 max_timestamp;
  sint64 producer_id;
  sint16 producer_epoch;
  sint32 base_sequence;
  sarray<record> records;
  int32_t serialize(int8_t *buf) override {
    int32_t sz{};
    sz += base_offset.serialize(buf + sz);
    sz += batch_length.serialize(buf + sz);
    sz += partition_leader_epoch.serialize(buf + sz);
    sz += magic_byte.serialize(buf + sz);
    sz += crc.serialize(buf + sz);
    sz += attributes.serialize(buf + sz);
    sz += last_offset_data.serialize(buf + sz);
    sz += base_timestamp.serialize(buf + sz);
    sz += max_timestamp.serialize(buf + sz);
    sz += producer_id.serialize(buf + sz);
    sz += producer_epoch.serialize(buf + sz);
    sz += base_sequence.serialize(buf + sz);
    sz += records.serialize(buf + sz);
    return sz;
  }
  int32_t deserialize(int8_t *buf) override {
    int32_t sz{};
    std::cout  << "starting batch at " << tohex(buf, 16) << std::endl;
    sz += base_offset.deserialize(buf + sz);
    sz += batch_length.deserialize(buf + sz);
    sz += partition_leader_epoch.deserialize(buf + sz);
    sz += magic_byte.deserialize(buf + sz);
    sz += crc.deserialize(buf + sz);
    sz += attributes.deserialize(buf + sz);
    sz += last_offset_data.deserialize(buf + sz);
    sz += base_timestamp.deserialize(buf + sz);
    sz += max_timestamp.deserialize(buf + sz);
    sz += producer_id.deserialize(buf + sz);
    sz += producer_epoch.deserialize(buf + sz);
    sz += base_sequence.deserialize(buf + sz);
    std::cout << "records parsing at " << sz << std::endl;
    sz += records.deserialize(buf + sz);
    return sz;
  }
};

#endif  // INCLUDE_CLS_RECORD_HPP_
