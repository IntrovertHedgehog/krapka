#ifndef API_ALL_H
#define API_ALL_H
#include "request_message.hpp"
#include "response_message.hpp"
#include <string>
void api_api_version_k18_v4(request_k18_v4* req, response_k18_v4* res);
void api_describe_topic_partitions(request_k75_v0* req, response_k75_v0* res, std::string const &);
#endif
