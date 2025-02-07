#include "datamap.hpp"

#include <memory.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <experimental/filesystem>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <iterator>
#include <ostream>
#include <regex>
#include <string>

#include "hexutil.hpp"
#include "record.hpp"

std::unordered_map<std::string, std::vector<std::shared_ptr<res_partition>>>
    topic_uuid_to_partitions;

std::unordered_map<std::string, std::unordered_map<int32_t, scarray<sint8>>>
    topic_uuid_to_partition_to_records;

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
          std::cout << "adding topic " << rv->topic_uuid.str() << ":"
                    << rv->topic_name.val << std::endl;
          break;
        }
        case 3:
          std::shared_ptr<record_value_type3_t> rv =
              std::dynamic_pointer_cast<record_value_type3_t>(r.value.value);
          std::shared_ptr<res_partition> p = std::make_shared<res_partition>();
          p->error_code.val = 0;
          p->partition_index.val = rv->paritition_id.val;
          topic_uuid_to_partitions[rv->topic_uuid.str()].push_back(p);
          std::cout << "adding partition " << p->partition_index.val << " into "
                    << rv->topic_uuid.str() << std::endl;
      }
    }
  }

  for (auto &topic : topic_name_to_uuid) {
    auto part_it = topic_uuid_to_partitions.find(topic.second);
    if (part_it == topic_uuid_to_partitions.end()) continue;
    for (auto &p : part_it->second) {
      std::string pathname = std::format("/tmp/kraft-combined-logs/{}-{}",
                                         topic.first, p->partition_index.val);
      if (!std::filesystem::is_directory(pathname)) continue;

      std::filesystem::directory_iterator dir(pathname);

      for (std::filesystem::directory_entry const &d : dir) {
        // only read well-formatted log file name
        std::regex log_fn_pattern("^\\d{20}.log");
        if (!std::regex_match(d.path().filename().string(), log_fn_pattern))
          continue;

        std::fstream fs(d.path());
        char buf[BUFSIZ];
        fs.read(buf, BUFSIZ);
        auto &ve = topic_uuid_to_partition_to_records[topic.second]
                                                     [p->partition_index.val];
        ve.is_null = false;
        std::transform(buf, buf + fs.gcount(), std::back_inserter(ve.val),
                       [](char c) { return sint8(c); });
        std::cout << "read from file " << topic.second << ":"
                  << p->partition_index.val << " -> "
                  << tohex((int8_t *)buf, fs.gcount()) << std::endl;
      }
    }
  }
}
