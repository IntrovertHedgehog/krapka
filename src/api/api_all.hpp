#ifndef API_ALL_H
#define API_ALL_H
#include <string>

#include "request_message.hpp"
#include "response_message.hpp"

void api_fetch_k1_v16(request_k1_v16* req, response_k1_v16* res,
                      std::string const&);
void api_api_version_k18_v4(request_k18_v4* req, response_k18_v4* res);
void api_describe_topic_partitions(request_k75_v0* req, response_k75_v0* res,
                                   std::string const&);
#endif
