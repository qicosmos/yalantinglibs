/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <chrono>
#include <string>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "async_simple/Try.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/Lazy.h"
#include "cmdline.h"
#include "rpc_service.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::string_literals;

struct bench_config {
  std::string host;
  std::string port;
  uint32_t client_concurrency;
  size_t data_len;
  size_t max_request_count;
};

Lazy<void> show_rpc_call(const bench_config& conf) {
  size_t req_count = 0;
#if NDEBUG
  req_count = 10000000;
  easylog::set_min_severity(easylog::Severity::INFO);
#endif
  coro_rpc_client client;

  [[maybe_unused]] auto ec = co_await client.connect(conf.host, conf.port);
  assert(!ec);

  /*----------------rdma---------------*/
  resources resource{};
  resources_create(&resource);

  asio::posix::stream_descriptor cq_fd(
      client.get_executor().get_asio_executor(),
      resource.complete_event_channel->fd);
  int r = ibv_req_notify_cq(resource.cq, 0);
  assert(r >= 0);

  resources* res = &resource;
  auto qp = create_qp(res->pd, res->cq);
  auto mr = create_mr(res->pd);
  auto ctx = std::make_shared<conn_context>(conn_context{mr, qp});

  cm_con_data_t local_data{};
  local_data.addr = (uintptr_t)mr->addr;
  local_data.rkey = mr->rkey;
  local_data.qp_num = qp->qp_num;
  local_data.lid = res->port_attr.lid;
  union ibv_gid my_gid;
  memset(&my_gid, 0, sizeof(my_gid));

  if (config.gid_idx >= 0) {
    CHECK(ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid));
  }

  memcpy(local_data.gid, &my_gid, 16);

  auto rdma_ret = co_await client.call_for<&rdma_service_t::get_con_data>(
      std::chrono::seconds(3600), local_data);
  // g_remote_con_data = rdma_ret.value();
  connect_qp(qp, rdma_ret.value());

  async_simple::Promise<void> promise;
  auto on_response =
      [res, &cq_fd, &promise](
          std::shared_ptr<conn_context> ctx) -> async_simple::coro::Lazy<void> {
    while (true) {
      coro_io::callback_awaitor<std::error_code> awaitor;
      auto ec = co_await awaitor.await_resume([&cq_fd](auto handler) {
        cq_fd.async_wait(asio::posix::stream_descriptor::wait_read,
                         [handler](const auto& ec) mutable {
                           handler.set_value_then_resume(ec);
                         });
      });
      if (ec) {
        ELOG_INFO << ec.message();
        if (ctx->recv_id)
          resume<int>(ec.value(), ctx->recv_id);

        if (ctx->send_id)
          resume<int>(ec.value(), ctx->send_id);

        promise.setValue();
        break;
      }

      poll_completion(res, ctx);
    }
  };

  on_response(ctx).via(&client.get_executor()).start([](auto&&) {
  });

  auto close_lz = [&cq_fd]() -> async_simple::coro::Lazy<int> {
    std::error_code ignore;
    cq_fd.cancel(ignore);
    cq_fd.close(ignore);
    co_return 0;
  };

  std::string msg(conf.data_len, 'A');
  for (size_t i = 0; i < conf.max_request_count; i++) {
    // send request to server
    auto [rr, sr] = co_await async_simple::coro::collectAll(
        post_receive_coro(ctx.get()), post_send_coro(ctx.get(), msg, true));
    if (rr.value() || sr.value()) {
      ELOG_ERROR << "rdma send recv error";
      break;
    }
  }

  std::error_code ignore;
  cq_fd.cancel(ignore);
  cq_fd.close(ignore);

  co_await promise.getFuture();

  ibv_destroy_qp(ctx->qp);
  free(ctx->mr->addr);
  ibv_dereg_mr(ctx->mr);
  resources_destroy(res);

  co_return;
  /*----------------rdma---------------*/
}
/*send multi request with same socket in the same time*/
Lazy<void> connection_reuse() {
  coro_rpc_client client;
  [[maybe_unused]] auto ec = co_await client.connect("127.0.0.1", "8801");
  assert(!ec);
  std::vector<Lazy<async_rpc_result<int>>> handlers;
  for (int i = 0; i < 10; ++i) {
    /* send_request is thread-safe, so you can call it in different thread with
     * same client*/
    handlers.push_back(co_await client.send_request<add>(i, i + 1));
  }
  std::vector<async_simple::Try<async_rpc_result<int>>> results =
      co_await collectAll(std::move(handlers));
  for (int i = 0; i < 10; ++i) {
    std::cout << results[i].value()->result() << std::endl;
    assert(results[i].value()->result() == 2 * i + 1);
  }
  co_return;
}

int main(int argc, char** argv) {
  cmdline::parser parser;

  parser.add<std::string>("host", 'h', "server ip address", false, "127.0.0.1");
  parser.add<std::string>("port", 'p', "server port", false, "8801");

  parser.add<size_t>("data_len", 'l', "data length", false, 16);
  parser.add<uint32_t>("client_concurrency", 'c',
                       "total number of http clients", false, 1);
  parser.add<size_t>("max_request_count", 'm', "max request count", false,
                     100000);

  parser.parse_check(argc, argv);

  bench_config conf{};
  conf.host = parser.get<std::string>("host");
  conf.port = parser.get<std::string>("port");
  conf.client_concurrency = parser.get<uint32_t>("client_concurrency");
  conf.data_len = parser.get<size_t>("data_len");
  conf.max_request_count = parser.get<size_t>("max_request_count");

  ELOG_INFO << "ip: " << conf.host << ", " << "port: " << conf.port << ", "
            << "client concurrency: " << conf.client_concurrency << ", "
            << "data_len: " << conf.data_len << ", "
            << "max_request_count: " << conf.max_request_count;

  try {
    syncAwait(show_rpc_call(conf));
    // syncAwait(connection_reuse());
    std::cout << "Done!" << std::endl;
  } catch (const std::exception& e) {
    std::cout << "Error:" << e.what() << std::endl;
  }
  return 0;
}