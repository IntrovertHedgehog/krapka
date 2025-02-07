#include "datamap.hpp"

#include <memory.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>

#include "hexutil.hpp"
#include "record.hpp"

std::unordered_map<std::string, std::vector<std::shared_ptr<res_partition>>>
    topic_uuid_to_partitions;

std::unordered_map<std::string, std::string> topic_name_to_uuid;

void initialize() {
  std::string log_fn =
      "/tmp/kraft-combined-logs/__cluster_metadata-0/"
      "00000000000000000000.log";
  int8_t log_fn_read_buf[BUFSIZ];
  FILE *fs = fopen(log_fn.c_str(), "r");
  int32_t fsize = fread(log_fn_read_buf, sizeof(int8_t), BUFSIZ, fs);
  if (fsize == -1) {
    std::cerr << "file reading error" << std::endl;
  }
  int32_t offset{};
  while (offset < fsize) {
    record_batch rb;
    offset += rb.deserialize(log_fn_read_buf + offset);
    std::cout << "file offset " << offset << std::endl;
    for (record &r : rb.records.val) {
      switch (r.value.type.val) {
        case 2: {
          std::shared_ptr<record_value_type2_t> rv =
              std::dynamic_pointer_cast<record_value_type2_t>(r.value.value);
          topic_name_to_uuid[rv->topic_name.val] = rv->topic_uuid.str();
          break;
        }
        case 3:
          std::shared_ptr<record_value_type3_t> rv =
              std::dynamic_pointer_cast<record_value_type3_t>(r.value.value);
          std::shared_ptr<res_partition> p = std::make_shared<res_partition>();
          p->error_code.val = 0;
          p->partition_index.val = rv->paritition_id.val;
          topic_uuid_to_partitions[rv->topic_uuid.str()].push_back(p);
      }
    }
  }

  std::filesystem::recursive_directory_iterator dir(
      "/tmp/kraft-combined-logs/");

  for (std::filesystem::directory_entry const &d : dir) {
    std::cout << "content of " << d.path().string() << std::endl;
    std::fstream fs(d.path());
    char buf[BUFSIZ];
    fs.read(buf, BUFSIZ);
    std::cout << tohex(reinterpret_cast<int8_t *>(buf), fs.gcount())
              << std::endl;
  }
}
