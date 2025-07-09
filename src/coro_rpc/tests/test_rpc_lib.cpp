#define DOCTEST_CONFIG_IMPLEMENT

#include "doctest.h"

#include "ylt/coro_io/coro_io.hpp"
#include "../examples/base_examples/coro_rpc_lib/coro_rpc.h"
#include <memory>

void load_service(void *ctx, uint64_t req_id) {
  if (req_id == 99) {
    response_error(ctx, 1001, "error from server");
    return;
  }

  auto str = std::make_shared<std::string>("response string from server");
  auto promise = response_msg(ctx, str->data(), str->size());

  coro_io::post([promise, str] {
    auto result = wait_response_finish(promise);
    if (result.ec) {
      std::cout << result.err_msg << "\n";
      free(result.err_msg);
    }
  }).start([](auto &&) {});
}

TEST_CASE("test start stop server") {
  std::string addr = "0.0.0.0:8804";
  auto server = start_rpc_server(addr.data(), server_config{2});
  CHECK(server != nullptr);
  auto server1 = start_rpc_server(addr.data(), server_config{2});
  CHECK(server1 == nullptr);

  stop_rpc_server(server);
}

TEST_CASE("test start stop client and server") {
  std::string addr = "0.0.0.0:8804";
  auto server = start_rpc_server(addr.data(), server_config{2});
  CHECK(server != nullptr);

  auto pool = create_client_pool(addr.data(), client_config{2, 30});
  CHECK(pool != nullptr);
  char resp_buf[100];
  auto result = load(pool, 1, resp_buf, 100);
  CHECK(result.ec == 0);
  CHECK(std::string_view(resp_buf, result.len) ==
        "response string from server");
  std::cout << "load result: " << std::string_view(resp_buf, result.len)
            << "\n";
  result = load(pool, 99, resp_buf, 100);
  CHECK(result.ec == 1001);
  CHECK(std::string_view(result.err_msg) == "error from server");

  char short_buf[10];
  result = load(pool, 1, short_buf, 10);
  CHECK(result.ec != 0);
  CHECK(result.err_msg != nullptr);
  free(result.err_msg);

  free_client_pool(pool);
  stop_rpc_server(server);
  free_client_pool(nullptr);
  stop_rpc_server(nullptr);
}

TEST_CASE("test client request failed") {
  std::string addr = "0.0.0.0:8804";

  auto pool = create_client_pool(addr.data(), client_config{2, 30});
  CHECK(pool != nullptr);
  char resp_buf[100];
  auto result = load(pool, 1, resp_buf, 100);
  CHECK(result.ec != 0);
  CHECK(result.err_msg != nullptr);
  std::cout << result.err_msg << "\n";
  free(result.err_msg);

  free_client_pool(pool);
}

TEST_CASE("test invalid device name") {
  std::string addr = "0.0.0.0:8804";
  server_config conf{};
  conf.parallel = 2;
  conf.enable_ib = true;
  std::string dev_name = "invalid dev name";
  conf.device_name = dev_name.data();

  auto server = start_rpc_server(addr.data(), conf);
  CHECK(server == nullptr);

  client_config client_conf{};
  client_conf.connect_timeout_sec = 2;
  client_conf.req_timeout_sec = 0;
  client_conf.enable_ib = true;
  client_conf.local_ip = dev_name.data();
  auto pool = create_client_pool(addr.data(), client_conf);
  CHECK(pool == nullptr);  
}

TEST_CASE("test request timeout") {
  std::string addr = "0.0.0.0:8804";
  auto server = start_rpc_server(addr.data(), server_config{2});
  CHECK(server != nullptr);

  auto pool = create_client_pool(addr.data(), client_config{2, 0});
  CHECK(pool != nullptr);
  char resp_buf[100];
  auto result = load(pool, 1, resp_buf, 100);
  CHECK(result.ec != 0);
  CHECK(result.err_msg != nullptr);
  std::cout << result.err_msg << "\n";
  free(result.err_msg);

  free_client_pool(pool);
  stop_rpc_server(server);
}

TEST_CASE("test connect timeout") {
  std::string addr = "0.0.0.0:8804";
  auto server = start_rpc_server(addr.data(), server_config{2});
  CHECK(server != nullptr);

  auto pool = create_client_pool(addr.data(), client_config{0, 30});
  CHECK(pool != nullptr);
  char resp_buf[100];
  auto result = load(pool, 1, resp_buf, 100);
  CHECK(result.ec != 0);
  CHECK(result.err_msg != nullptr);
  std::cout << result.err_msg << "\n";
  free(result.err_msg);

  free_client_pool(pool);
  stop_rpc_server(server);
}

DOCTEST_MSVC_SUPPRESS_WARNING_WITH_PUSH(4007)
int main(int argc, char **argv) { return doctest::Context(argc, argv).run(); }
DOCTEST_MSVC_SUPPRESS_WARNING_POP