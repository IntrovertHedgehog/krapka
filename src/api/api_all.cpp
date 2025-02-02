#include "api_all.hpp"

#include <iostream>
#include <ostream>

#include "constants.hpp"

void api_api_version_k18(request_k18_v4* req, response_k18_v4* res) {
  std::cout << "in func" << std::endl;
  std::cout << req->header->request_api_version.val << std::endl;
  if (req->header->request_api_version.val < API_VERSION_MIN ||
      req->header->request_api_version.val > API_VERSION_MAX) {
    res->error_code = sint16(ERR_UNSUPPORTED_VERSION);
  } else {
    res->error_code = sint16(0);
    res->version_infos = scarray<version_info>(std::vector<version_info>{
        version_info(18, API_VERSION_MIN, API_VERSION_MAX)});
    res->throttle_time_ms = sint32(0);
    res->tagged_buffer = stagged_fields();
  }
  std::cout << "finished k18: ";
  std::cout << res->error_code.val << ":" << res->throttle_time_ms.val
            << std::endl;
}
