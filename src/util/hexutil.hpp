#ifndef HEXUTIL_H
#define HEXUTIL_H

#include <cstddef>
#include <cstdint>
#include <istream>
#include <iterator>
#include <string>

#define TRANS(SRC, DST) \
  switch (SRC) {        \
    case '0':           \
      DST = 0;          \
      break;            \
    case '1':           \
      DST = 1;          \
      break;            \
    case '2':           \
      DST = 2;          \
      break;            \
    case '3':           \
      DST = 3;          \
      break;            \
    case '4':           \
      DST = 4;          \
      break;            \
    case '5':           \
      DST = 5;          \
      break;            \
    case '6':           \
      DST = 6;          \
      break;            \
    case '7':           \
      DST = 7;          \
      break;            \
    case '8':           \
      DST = 8;          \
      break;            \
    case '9':           \
      DST = 9;          \
      break;            \
    case 'a':           \
      DST = 10;         \
      break;            \
    case 'b':           \
      DST = 11;         \
      break;            \
    case 'c':           \
      DST = 12;         \
      break;            \
    case 'd':           \
      DST = 13;         \
      break;            \
    case 'e':           \
      DST = 14;         \
      break;            \
    case 'f':           \
      DST = 15;         \
      break;            \
  }

std::string tohex(int8_t *beg, size_t len);

int tobuf(std::string const &hexstr, int8_t *beg, size_t len);

int readbyte(std::istream &is, int8_t *out);

#endif
