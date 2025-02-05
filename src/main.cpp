#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <ostream>
#include <string>
#include <thread>

#include "api/api_all.hpp"
#include "constants.hpp"
#include "primitive.hpp"
#include "request_message.hpp"
#include "response_message.hpp"

void process_connection(int client_fd, std::atomic<int> *pool,
                        std::string const &cluster_metadata_log_fn) {
  int8_t in[BUFSIZ], out[BUFSIZ];
  int32_t len_in, len_out;
  while ((len_in = recv(client_fd, in, BUFSIZ, 0)) > 0) {
    int32_t offset{};
    while (offset < len_in) {
      int32_t orig_offset{offset};
      sint32 msg_len;
      offset += msg_len.deserialize(in + offset);

      request_header_v2 req_header;
      offset += req_header.deserialize(in + offset);

      switch (req_header.request_api_key.val) {
        case 18: {
          response_header_v0 res_header;
          res_header.correlation_id = req_header.correlation_id;
          request_k18_v4 req(&req_header);
          response_k18_v4 res(&res_header);
          offset += req.deserialize(in + offset);
          // std::cout << "read " << offset << " characters" << std::endl;
          api_api_version_k18_v4(&req, &res);
          len_out = write_message(out, &res);
          break;
        }
        case 75: {
          response_header_v1 res_header;
          res_header.correlation_id = req_header.correlation_id;
          request_k75_v0 req(&req_header);
          response_k75_v0 res(&res_header);
          offset += req.deserialize(in + offset);
          api_describe_topic_partitions(&req, &res, cluster_metadata_log_fn);
          len_out = write_message(out, &res);
          break;
        }
        default:
          std::cout << "no api match" << std::endl;
      }

      send(client_fd, out, len_out, 0);
      offset = orig_offset + sizeof(int32_t) + msg_len.val;
      std::cout << "done sending " << len_in << std::endl
                << "offset " << offset << " len " << len_in << std::endl;
    }
  }

  close(client_fd);
  --(*pool);
}

int main(int argc, char *argv[]) {
  // Disable output buffering
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  std::string log_fn;

  log_fn =
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
  // log_fn = "tmp/meta";
  // prepare log file

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket: " << std::endl;
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    close(server_fd);
    std::cerr << "setsockopt failed: " << std::endl;
    return 1;
  }

  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(9092);

  if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
           sizeof(server_addr)) != 0) {
    close(server_fd);
    std::cerr << "Failed to bind to port 9092" << std::endl;
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    close(server_fd);
    std::cerr << "listen failed" << std::endl;
    return 1;
  }

  std::cout << "Waiting for a client to connect...\n";

  struct sockaddr_in client_addr{};
  socklen_t client_addr_len = sizeof(client_addr);

  std::cerr << "Logs from your program will appear here!\n";

  std::atomic<int> pool(0);

  while (int client_fd = accept(
             server_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
             &client_addr_len)) {
    std::cout << "Client connected: " << client_addr.sin_addr.s_addr << ":"
              << client_addr.sin_port << std::endl;
    if (++pool > THPOOL_SIZE) {
      std::cout << "ran out of thread in pool" << std::endl;
      --pool;
    } else {
      std::thread t(process_connection, client_fd, &pool, log_fn);
      t.detach();
    }
  }

  close(server_fd);
  return 0;
}
