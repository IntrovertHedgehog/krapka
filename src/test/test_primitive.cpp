#include <netinet/in.h>
#include <unistd.h>

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <complex>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ostream>
#include <vector>

#include "hexutil.hpp"
#include "primitive.hpp"

int const BS = 1024;

TEST_CASE("Testing boolean", "[bool][fixed]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = sbool(false).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x00");

  sz = sbool(true).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x01");

  sbool sb;
  in[0] = 0x01;
  sz = sb.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(sb.val == true);

  in[0] = 0x00;
  sz = sb.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(sb.val == false);
}

TEST_CASE("Testing int8", "[int8]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = sint8(123).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x7b");

  sz = sint8(-123).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x85");

  sint8 si;
  in[0] = 0x7b;
  sz = si.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(si.val == 123);

  in[0] = -123;
  sz = si.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(si.val == -123);
}

TEST_CASE("Testing int16", "[int16]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = sint16(0x1234).serialize(out);
  REQUIRE(sz == 2);
  REQUIRE(tohex(out, sz) == "0x1234");

  sz = sint16(-0x0233).serialize(out);
  REQUIRE(sz == 2);
  REQUIRE(tohex(out, sz) == "0xfdcd");

  sint16 si;
  in[0] = 0x12;
  in[1] = 0x34;
  sz = si.deserialize(in);
  REQUIRE(sz == 2);
  REQUIRE(si.val == 0x1234);

  in[0] = -3;
  in[1] = -52;
  sz = si.deserialize(in);
  REQUIRE(sz == 2);
  REQUIRE(si.val == -564);
}

TEST_CASE("Testing int64", "[int32]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = sint64(-1).serialize(out);
  REQUIRE(sz == 8);
  REQUIRE(tohex(out, sz) == "0xffffffffffffffff");

  sz = sint64(INT64_MAX).serialize(out);
  REQUIRE(sz == 8);
  REQUIRE(tohex(out, sz) == "0x7fffffffffffffff");

  sz = suint64(UINT64_MAX).serialize(out);
  REQUIRE(sz == 8);
  REQUIRE(tohex(out, sz) == "0xffffffffffffffff");

  sint64 si;
  REQUIRE(tobuf("0xffffffffffffffff", in, BS) != -1);
  sz = si.deserialize(in);
  REQUIRE(sz == 8);
  REQUIRE(si.val == -1);

  REQUIRE(tobuf("0x7ffffffffffffff", in, BS) != -1);
  sz = si.deserialize(in);
  REQUIRE(sz == 8);
  REQUIRE(si.val == INT64_MAX);

  suint64 sui;
  REQUIRE(tobuf("0xffffffffffffffff", in, BS) != -1);
  sz = sui.deserialize(in);
  REQUIRE(sz == 8);
  REQUIRE(sui.val == UINT64_MAX);
}

TEST_CASE("Testing suvint", "[suvint]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = suvint(0).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x00");

  sz = suvint(UINT32_MAX).serialize(out);
  REQUIRE(sz == 5);
  REQUIRE(tohex(out, sz) == "0xffffffff0f");

  sz = suvint(12).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x0c");

  sz = suvint(150).serialize(out);
  REQUIRE(sz == 2);
  REQUIRE(tohex(out, sz) == "0x9601");

  sz = suvint(0b1001101'0010110'0001110).serialize(out);
  REQUIRE(sz == 3);
  REQUIRE(tohex(out, sz) == "0x8e964d");

  suvint si;
  in[0] = 0x01;
  sz = si.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(si.val == 1);

  in[0] = -106;
  in[1] = 0x01;
  sz = si.deserialize(in);
  REQUIRE(sz == 2);
  REQUIRE(si.val == 150);

  in[0] = 0;
  sz = si.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(si.val == 0);

  REQUIRE(tobuf("0xffffffff0f", in, 1024) != -1);
  sz = si.deserialize(in);
  REQUIRE(sz == 5);
  REQUIRE(si.val == UINT32_MAX);
}

TEST_CASE("Testing svint", "[svint]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = svint(0).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x00");

  sz = svint(INT32_MIN).serialize(out);
  REQUIRE(sz == 5);
  REQUIRE(tohex(out, sz) == "0xffffffff0f");

  sz = svint(INT32_MAX).serialize(out);
  REQUIRE(sz == 5);
  REQUIRE(tohex(out, sz) == "0xfeffffff0f");

  sz = svint(150).serialize(out);
  REQUIRE(sz == 2);
  REQUIRE(tohex(out, sz) == "0xac02");

  sz = svint(-150).serialize(out);
  REQUIRE(sz == 2);
  REQUIRE(tohex(out, sz) == "0xab02");

  suvint si;
  in[0] = 0x01;
  sz = si.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(si.val == 1);

  in[0] = -106;
  in[1] = 0x01;
  sz = si.deserialize(in);
  REQUIRE(sz == 2);
  REQUIRE(si.val == 150);
}

TEST_CASE("Testing UUID", "[uuid]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = suuid("550e8400-e29b-41d4-a716-446655440000").serialize(out);
  REQUIRE(sz == 16);
  REQUIRE(tohex(out, sz) == "0x550e8400e29b41d4a716446655440000");

  sz = suuid().serialize(out);
  REQUIRE(sz == 16);
  REQUIRE(tohex(out, sz) == "0x00000000000000000000000000000000");

  REQUIRE(tobuf("0x550e8400e29b41d4a716446655440000", in, BS) != -1);
  suuid si;
  sz = si.deserialize(in);
  REQUIRE(sz == 16);
  REQUIRE(tohex(si.val, 16) == "0x550e8400e29b41d4a716446655440000");
}

TEST_CASE("Testing string", "[string]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = sstring("").serialize(out);
  REQUIRE(sz == 2);
  REQUIRE(tohex(out, sz) == "0x0000");

  sz = sstring("hello").serialize(out);
  REQUIRE(sz == 7);
  REQUIRE(tohex(out, sz) == "0x000568656c6c6f");

  sstring ss;
  REQUIRE(tobuf("0x0000", in, BS) != -1);
  sz = ss.deserialize(in);
  REQUIRE(sz == 2);
  REQUIRE(ss.val == "");

  REQUIRE(tobuf("0x000568656c6c6f", in, BS) != -1);
  sz = ss.deserialize(in);
  REQUIRE(sz == 7);
  REQUIRE(ss.val == "hello");
}

TEST_CASE("Testing compact string", "[string]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = scstring("").serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x01");

  sz = scstring("hello").serialize(out);
  REQUIRE(sz == 6);
  REQUIRE(tohex(out, sz) == "0x0668656c6c6f");

  scstring ss;
  REQUIRE(tobuf("0x01", in, BS) != -1);
  sz = ss.deserialize(in);
  REQUIRE(sz == 1);
  REQUIRE(ss.val == "");

  REQUIRE(tobuf("0x0668656c6c6f", in, BS) != -1);
  sz = ss.deserialize(in);
  REQUIRE(sz == 6);
  REQUIRE(ss.val == "hello");
}

TEST_CASE("Testing array", "[array]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = sarray<sint16>({sint16(1), sint16(12), sint16(24), sint16(126)})
           .serialize(out);
  REQUIRE(sz == 12);
  REQUIRE(tohex(out, sz) == "0x000000040001000c0018007e");

  // null array
  sz = sarray<sint16>().serialize(out);
  REQUIRE(sz == 4);
  REQUIRE(tohex(out, sz) == "0xffffffff");

  // empty array
  sz = sarray<sint16>(std::vector<sint16>{}).serialize(out);
  REQUIRE(sz == 4);
  REQUIRE(tohex(out, sz) == "0x00000000");

  sarray<sint16> sa;
  std::vector<sint16> exp{sint16(1), sint16(12), sint16(24), sint16(126)};
  REQUIRE(tobuf("0x000000040001000c0018007e", in, BS) != -1);
  REQUIRE((sz = sa.deserialize(in)) == 12);
  REQUIRE(sa.val.size() == 4);
  bool flag = true;
  for (size_t i = 0; i < sa.val.size(); ++i) {
    if (sa.val[i].val != exp[i].val) {
      flag = false;
      break;
    }
  }
  REQUIRE(flag);

  // empty
  REQUIRE(tobuf("0x00000000", in, BS) != -1);
  REQUIRE((sz = sa.deserialize(in)) == 4);
  REQUIRE(sa.val.size() == 0);
}

TEST_CASE("Testing compact array", "[array]") {
  int8_t in[BS], out[BS];
  int32_t sz;

  sz = scarray<sint16>({sint16(1), sint16(12), sint16(24), sint16(126)})
           .serialize(out);
  REQUIRE(sz == 9);
  REQUIRE(tohex(out, sz) == "0x050001000c0018007e");

  // null array
  sz = scarray<sint16>().serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x00");

  // empty array
  sz = scarray<sint16>(std::vector<sint16>{}).serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x01");

  scarray<sint16> sa;
  std::vector<sint16> exp{sint16(1), sint16(12), sint16(24), sint16(126)};
  REQUIRE(tobuf("0x050001000c0018007e", in, BS) != -1);
  REQUIRE((sz = sa.deserialize(in)) == 9);
  REQUIRE(sa.val.size() == 4);
  bool flag = true;
  for (size_t i = 0; i < sa.val.size(); ++i) {
    if (sa.val[i].val != exp[i].val) {
      flag = false;
      break;
    }
  }
  REQUIRE(flag);

  // empty
  REQUIRE(tobuf("0x00000000", in, BS) != -1);
  REQUIRE((sz = sa.deserialize(in)) == 1);
  REQUIRE(sa.val.size() == 0);
}

TEST_CASE("Testing tagged buffer", "[tags]") {
  typedef stagged_fields::field field;
  int8_t in[BS], out[BS];
  int32_t sz;
  field f(1, {'h', 'e', 'l', 'l', 'o', '!'});
  stagged_fields t0{},
      t1(std::vector<field>{field(1, {'h', 'e', 'l', 'l', 'o', '!'}),
                            field(2, {'w', 'o', 'r', 'l', 'd'})});

  sz = t0.serialize(out);
  REQUIRE(sz == 1);
  REQUIRE(tohex(out, sz) == "0x00");

  sz = t1.serialize(out);
  REQUIRE(sz == 16);
  REQUIRE(tohex(out, sz) == "0x02010668656c6c6f210205776f726c64");

  stagged_fields t;
  REQUIRE(tobuf("0x00", in, BS) != -1);
  sz = t.deserialize(in);
  REQUIRE(t.fields.size() == 0);

  REQUIRE(tobuf("0x02010668656c6c6f210205776f726c64", in, BS) != -1);
  sz = t.deserialize(in);
  REQUIRE(sz == 16);
  REQUIRE(t1.fields.size() == t.fields.size());
  for (size_t i = 0; i < t1.fields.size(); ++i) {
    REQUIRE(t1.fields[i].tag.val == t.fields[i].tag.val);
    REQUIRE(std::equal(t1.fields[i].data.begin(), t1.fields[i].data.end(),
                       t.fields[i].data.begin()));
  }
}
