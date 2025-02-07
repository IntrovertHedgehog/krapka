#ifndef PRIMITIVE_H
#define PRIMITIVE_H

#include <arpa/inet.h>
#include <netinet/in.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <ostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "hexutil.hpp"

struct sbase {
  virtual int32_t serialize(int8_t*) = 0;
  virtual int32_t deserialize(int8_t*) = 0;
  sbase() = default;
  sbase(const sbase&) = default;
  sbase(sbase&&) noexcept = default;
  sbase& operator=(sbase const&) = default;
  sbase& operator=(sbase&&) noexcept = default;
  virtual ~sbase() = default;
};

struct sbool : public sbase {
  bool val;
  sbool() = default;
  explicit sbool(bool v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *buf = val;
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val = *buf;
    return sizeof(val);
  }
};

struct sint8 : public sbase {
  int8_t val;
  sint8() = default;
  explicit sint8(int8_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *buf = val;
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val = *buf;
    return sizeof(val);
  }
};

struct sint16 : public sbase {
  int16_t val;
  sint16() = default;
  explicit sint16(int16_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *reinterpret_cast<int16_t*>(buf) = htons(val);
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val = ntohs(*reinterpret_cast<int16_t*>(buf));
    return sizeof(val);
  }
};

struct sint32 : public sbase {
  int32_t val;
  sint32() = default;
  explicit sint32(int32_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *reinterpret_cast<int32_t*>(buf) = htonl(val);
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val = ntohl(*reinterpret_cast<int32_t*>(buf));
    return sizeof(val);
  }
};

struct sint64 : public sbase {
  int64_t val;
  sint64() = default;
  explicit sint64(int64_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *reinterpret_cast<int32_t*>(buf) = htonl(val >> 32);
    *reinterpret_cast<int32_t*>(buf + sizeof(int32_t)) =
        htonl(val & 0xffffffff);
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val =
        (static_cast<int64_t>(ntohl(*reinterpret_cast<int32_t*>(buf))) << 32) |
        ntohl(*reinterpret_cast<int32_t*>(buf + sizeof(int32_t)));
    return sizeof(val);
  }
};

struct suint64 : public sbase {
  uint64_t val;
  suint64() = default;
  explicit suint64(uint64_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *reinterpret_cast<uint32_t*>(buf) = htonl(val >> 32);
    *reinterpret_cast<uint32_t*>(buf + sizeof(int32_t)) =
        htonl(val & 0xffffffff);
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val = (static_cast<uint64_t>(ntohl(*reinterpret_cast<uint32_t*>(buf)))
           << 32) |
          ntohl(*reinterpret_cast<uint32_t*>(buf + sizeof(uint32_t)));
    return sizeof(val);
  }
};

struct suint32 : public sbase {
  uint32_t val;
  suint32() = default;
  explicit suint32(uint32_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *reinterpret_cast<uint32_t*>(buf) = htonl(val);
    return sizeof(val);
  }
  int32_t deserialize(int8_t* buf) override {
    val = ntohl(*reinterpret_cast<uint32_t*>(buf));
    return sizeof(val);
  }
};

struct suvint final : public sbase {
  uint32_t val;
  suvint() = default;
  explicit suvint(uint32_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    uint32_t tmp{val};
    int32_t size{};
    do {
      uint32_t part = tmp & 0x7f;
      tmp >>= 7;
      part |= static_cast<bool>(tmp) << 7;
      *buf++ = part;
      ++size;
    } while (tmp);
    return size;
  }
  int32_t deserialize(int8_t* buf) override {
    val = 0;
    int32_t size{}, order{};
    do {
      val |= static_cast<uint32_t>(*buf & 0x7f) << order;
      order += 7;
      ++size;
    } while (*buf++ & 0x80);
    return size;
  }
};

struct svint : public sbase {
  int32_t val;
  svint() = default;
  explicit svint(int32_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    if (val < 0)
      return suvint(2 * -(val + 1) + 1).serialize(buf);
    else
      return suvint(val * 2).serialize(buf);
  }
  int32_t deserialize(int8_t* buf) override {
    suvint tmp;
    int32_t size = tmp.deserialize(buf);
    if (tmp.val & 1)
      // wrong: val = -(tmp.val + 1) / 2
      val = -(tmp.val >> 1) - 1;
    else
      val = tmp.val >> 1;
    return size;
  }
};

struct suvlong : public sbase {
  uint64_t val;
  suvlong() = default;
  explicit suvlong(uint64_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    uint64_t tmp{val};
    int32_t size{};
    do {
      uint32_t part = tmp & 0x7f;
      tmp >>= 7;
      part |= static_cast<bool>(tmp) << 7;
      *buf++ = part;
      ++size;
    } while (tmp);
    return size;
  }
  int32_t deserialize(int8_t* buf) override {
    val = 0;
    int32_t size{}, order{};
    do {
      val |= static_cast<uint64_t>(*buf & 0x7f) << order;
      order += 7;
      ++size;
    } while (*buf++ & 0x80);
    return size;
  }
};

struct svlong : public sbase {
  int64_t val;
  svlong() = default;
  explicit svlong(int64_t v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    if (val & 1)
      return suvlong(2 * -(val + 1) + 1).serialize(buf);
    else
      return suvlong(val * 2).serialize(buf);
  }
  int32_t deserialize(int8_t* buf) override {
    suvlong tmp;
    int32_t size = tmp.deserialize(buf);
    if (tmp.val & 1)
      val = -(tmp.val >> 1) - 1;
    else
      val = tmp.val >> 1;
    return size;
  }
};

struct suuid : public sbase {
  int8_t val[16]{};
  suuid() = default;
  explicit suuid(std::string const& v) {
    std::istringstream is(v);
    int i{};
    for (; i < 4; ++i) {
      readbyte(is, val + i);
    }
    is.ignore();
    for (; i < 6; ++i) {
      readbyte(is, val + i);
    }
    is.ignore();
    for (; i < 8; ++i) {
      readbyte(is, val + i);
    }
    is.ignore();
    for (; i < 10; ++i) {
      readbyte(is, val + i);
    }
    is.ignore();
    for (; i < 16; ++i) {
      readbyte(is, val + i);
    }
  }
  int32_t serialize(int8_t* buf) override {
    std::copy(val, val + 16, buf);
    return 16;
  }
  int32_t deserialize(int8_t* buf) override {
    std::copy(buf, buf + 16, val);
    return 16;
  }
  std::string str() {
    std::ostringstream os;
    os << std::hex;
    int i{};
    for (; i < 4; ++i) {
      os << std::setw(2) << std::setfill('0')
         << static_cast<int>(static_cast<uint8_t>(val[i]));
    }
    os << '-';
    for (; i < 6; ++i) {
      os << std::setw(2) << std::setfill('0')
         << static_cast<int>(static_cast<uint8_t>(val[i]));
    }
    os << '-';
    for (; i < 8; ++i) {
      os << std::setw(2) << std::setfill('0')
         << static_cast<int>(static_cast<uint8_t>(val[i]));
    }
    os << '-';
    for (; i < 10; ++i) {
      os << std::setw(2) << std::setfill('0')
         << static_cast<int>(static_cast<uint8_t>(val[i]));
    }
    os << '-';
    for (; i < 16; ++i) {
      os << std::setw(2) << std::setfill('0')
         << static_cast<int>(static_cast<uint8_t>(val[i]));
    }
    return os.str();
  }
};

struct sstring : public sbase {
  std::string val;
  sstring() = default;
  explicit sstring(std::string v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    *reinterpret_cast<int16_t*>(buf) = htons(static_cast<int16_t>(val.size()));
    buf += sizeof(int16_t);
    std::copy(val.c_str(), val.c_str() + val.size(),
              reinterpret_cast<char*>(buf));
    return sizeof(int16_t) + val.size();
  }
  int32_t deserialize(int8_t* buf) override {
    int16_t size = ntohs(*reinterpret_cast<int16_t*>(buf));
    val.reserve(size);
    val.clear();
    buf += sizeof(int16_t);
    std::copy(reinterpret_cast<char*>(buf), reinterpret_cast<char*>(buf) + size,
              std::back_inserter(val));
    val.resize(size);
    return sizeof(int16_t) + size;
  }
};

struct snstring : public sbase {
  std::string val;
  bool is_null{true};
  snstring() = default;
  explicit snstring(std::string v) : val(v), is_null(false) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{sizeof(int16_t)};
    if (is_null) {
      *reinterpret_cast<int16_t*>(buf) = htons(static_cast<int16_t>(-1));
    } else {
      *reinterpret_cast<int16_t*>(buf) =
          htons(static_cast<int16_t>(val.size()));
      buf += sizeof(int16_t);
      std::copy(val.c_str(), val.c_str() + val.size(),
                reinterpret_cast<char*>(buf));
      sz += val.size();
    }
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{sizeof(int16_t)};
    int16_t size = ntohs(*reinterpret_cast<int16_t*>(buf));
    val.clear();
    if (size != -1) {
      val.reserve(size);
      buf += sizeof(int16_t);
      std::copy(reinterpret_cast<char*>(buf),
                reinterpret_cast<char*>(buf) + size, std::back_inserter(val));
      val.resize(size);
      sz += size;
    }
    return sz;
  }
};

struct scstring final : public sbase {
  std::string val;
  scstring() = default;
  explicit scstring(std::string v) : val(v) {}
  int32_t serialize(int8_t* buf) override {
    int32_t len_sz{suvint(val.size() + 1).serialize(buf)};
    std::copy(val.c_str(), val.c_str() + val.size(),
              reinterpret_cast<char*>(buf) + len_sz);
    return val.size() + len_sz;
  }
  int32_t deserialize(int8_t* buf) override {
    suvint sz;
    int32_t len_sz{sz.deserialize(buf)};
    val.clear();
    uint32_t n = sz.val - 1;
    val.reserve(n);
    // sz = N + 1
    // std::cout << "copying " << n << " characters into a " << val.capacity() << " sized" << std::endl;
    std::copy(reinterpret_cast<char*>(buf) + len_sz,
              reinterpret_cast<char*>(buf) + len_sz + n,
              std::back_inserter(val));
    return len_sz + n;
  }
};

struct scnstring final : public sbase {
  std::string val;
  bool is_null{true};
  scnstring() = default;
  explicit scnstring(std::string v) : val(v), is_null(false) {}
  int32_t serialize(int8_t* buf) override {
    if (is_null) {
      return suvint(0).serialize(buf);
    }
    int32_t len_sz{suvint(val.size() + 1).serialize(buf)};
    std::copy(val.c_str(), val.c_str() + val.size(),
              reinterpret_cast<char*>(buf) + len_sz);
    return val.size() + len_sz;
  }
  int32_t deserialize(int8_t* buf) override {
    suvint sz;
    int32_t len_sz{sz.deserialize(buf)};
    val.clear();
    if (sz.val == 0) {
      return len_sz;
    } else {
      uint32_t n = sz.val - 1;
      val.reserve(n);
      // sz = N + 1
      std::copy(reinterpret_cast<char*>(buf) + len_sz,
                reinterpret_cast<char*>(buf) + len_sz + n,
                std::back_inserter(val));
      return len_sz + n;
    }
  }
};

template <typename T,
          std::enable_if_t<std::is_base_of_v<sbase, T>, bool> = true>
struct sarray : public sbase {
  std::vector<T> val;
  bool is_null{true};
  sarray() = default;
  explicit sarray(std::vector<T> v) : val(v), is_null(false) {}
  int32_t serialize(int8_t* buf) override {
    int32_t size{};
    if (is_null) {
      size += sint32(-1).serialize(buf);
    } else {
      size += sint32(val.size()).serialize(buf);
      for (T& e : val) {
        size += e.serialize(buf + size);
      }
    }
    return size;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t size{};
    sint32 n;
    size += n.deserialize(buf);

    val.clear();
    if (n.val == -1) {
      is_null = true;
    } else {
      is_null = false;
      for (int32_t i = 0; i < n.val; ++i) {
        val.push_back(T());
        size += val.back().deserialize(buf + size);
      }
    }
    return size;
  }
};

template <typename T,
          std::enable_if_t<std::is_base_of_v<sbase, T>, bool> = true>
struct scarray : public sbase {
  std::vector<T> val;
  bool is_null{true};
  scarray() = default;
  explicit scarray(std::vector<T> v) : val(v), is_null(false) {}
  int32_t serialize(int8_t* buf) override {
    int32_t size{};
    if (is_null) {
      size += suvint(0).serialize(buf);
    } else {
      size += suvint(val.size() + 1).serialize(buf);
      for (T& e : val) {
        size += e.serialize(buf + size);
      }
    }
    return size;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t size{};
    suvint n;
    size += n.deserialize(buf);
    val.clear();
    // null array, abort
    if (n.val == 0) {
      is_null = true;
    } else {
      is_null = false;
      for (uint32_t i = 0; i < n.val - 1; ++i) {
        val.push_back(T());
        size += val.back().deserialize(buf + size);
      }
    }
    return size;
  }
};

struct stagged_fields final : sbase {
  struct field {
    suvint tag;
    std::vector<int8_t> data;
    field() = default;
    field(uint32_t t, std::vector<int8_t> d) : tag{t}, data{d} {}
  };
  std::vector<field> fields;
  stagged_fields() = default;
  explicit stagged_fields(std::vector<field> v) : fields(v) {}
  int32_t serialize(int8_t* buf) override {
    int32_t sz{};
    sz += suvint(fields.size()).serialize(buf + sz);
    for (field& f : fields) {
      sz += f.tag.serialize(buf + sz);
      sz += suvint(f.data.size()).serialize(buf + sz);
      std::copy(f.data.begin(), f.data.end(), buf + sz);
      sz += f.data.size();
    }
    return sz;
  }
  int32_t deserialize(int8_t* buf) override {
    int32_t sz{};
    suvint array_len;
    sz += array_len.deserialize(buf + sz);
    fields.clear();
    for (uint32_t i = 0; i < array_len.val; ++i) {
      fields.push_back(field());
      field& f = fields.back();
      sz += f.tag.deserialize(buf + sz);
      suvint field_size;
      sz += field_size.deserialize(buf + sz);
      std::cout << "field tag " << f.tag.val << " size " << field_size.val
                << std::endl;
      f.data.reserve(field_size.val);
      std::copy(buf + sz, buf + sz + field_size.val, f.data.begin());
      sz += field_size.val;
    }
    return sz;
  }
};

#endif
