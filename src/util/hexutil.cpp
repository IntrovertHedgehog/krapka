#include "hexutil.hpp"

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

int tobuf(std::string const & hexstr, int8_t *beg, size_t len) {
  if (len < hexstr.size() - 2) return -1;
  std::istringstream ss(hexstr);
  ss.ignore(2);

  char inp[2]{};
  size_t pos{};
  int8_t first, second;

  while (ss.read(inp, 2)) {
    TRANS(inp[0], first);
    TRANS(inp[1], second);
    beg[pos++] = first << 4 | second;
  }
  return pos;
}

std::string tohex(int8_t *beg, size_t len) {
  std::ostringstream ss;
  ss << "0x" << std::hex;
  for (size_t i = 0; i < len; ++i) {
    ss << std::setw(2) << std::setfill('0')
       << static_cast<int>(static_cast<uint8_t>(beg[i]));
  }
  return ss.str();
}

int readbyte(std::istream &is, int8_t *out) {
  char c[2];
  if (!is.read(c, 2)) return -1;
  int8_t first, second;
  TRANS(c[0], first);
  TRANS(c[1], second);
  *out = first << 4 | second;
  return 1;
}
