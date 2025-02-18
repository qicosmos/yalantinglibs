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
#include "ylt/coro_io/load_blancer.hpp"

#include <async_simple/coro/Collect.h>
#include <async_simple/coro/Lazy.h>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <memory>
#include <system_error>
#include <thread>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "cmdline.h"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/easylog.hpp"
using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
std::string echo(std::string_view sv);
std::atomic<uint64_t> qps = 0;

std::atomic<uint64_t> working_echo = 0;

size_t request_cnt = 10000;
std::string g_str;
std::string_view g_send_data;

Lazy<std::vector<std::chrono::microseconds>> call_echo(
    std::shared_ptr<coro_io::load_blancer<coro_rpc_client>> load_blancer) {
  std::vector<std::chrono::microseconds> result;
  result.reserve(request_cnt);
  auto tp = std::chrono::steady_clock::now();
  ++working_echo;
  for (size_t i = 0; i < request_cnt; ++i) {
    auto res = co_await load_blancer->send_request(
        [](coro_rpc_client &client, std::string_view hostname) -> Lazy<void> {
          auto res = co_await client.call<echo>(g_send_data);
          if (!res.has_value()) {
            ELOG_ERROR << "coro_rpc err: \n" << res.error().msg;
            co_return;
          }
        });
    if (!res) {
      ELOG_ERROR << "client pool err: connect failed.\n";
      break;
    }
    ++qps;
    auto old_tp = tp;
    tp = std::chrono::steady_clock::now();
    result.push_back(
        std::chrono::duration_cast<std::chrono::microseconds>(tp - old_tp));
  }
  co_return std::move(result);
}

Lazy<void> qps_watcher() {
  using namespace std::chrono_literals;
  auto &clients = coro_io::g_clients_pool<coro_rpc_client>();
  while (working_echo > 0) {
    co_await coro_io::sleep_for(1s);
    uint64_t cnt = qps.exchange(0);
    std::cout << "QPS:" << cnt << " working echo:" << working_echo << "\n";
    cnt = 0;
  }
}

std::vector<std::chrono::microseconds> result;
void latency_watcher() {
  std::sort(result.begin(), result.end());
  auto arr = {0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 0.99, 0.999, 0.9999, 0.99999, 1.0};
  for (auto e : arr) {
    std::cout
        << (e * 100) << "% request finished in:"
        << result[std::max<std::size_t>(0, result.size() * e - 1)].count() /
               1000.0
        << "ms" << std::endl;
  }
}

int main(int argc, char **argv) {
  easylog::set_min_severity(easylog::Severity::WARNING);
  cmdline::parser parser;

  parser.add<std::string>("url", 'u', "url", false, "0.0.0.0:8090");
  parser.add<size_t>("data_len", 'l', "data length", false, 0);
  parser.add<uint32_t>("client_concurrency", 'c',
                       "total number of http clients", false, 0);
  parser.add<size_t>("max_request_count", 'm', "max request count", false,
                     10000000);

  parser.parse_check(argc, argv);

  std::string url = parser.get<std::string>("url");
  auto data_len = parser.get<size_t>("data_len");
  uint32_t worker_cnt = parser.get<uint32_t>("client_concurrency");
  request_cnt = parser.get<size_t>("max_request_count");

  if (data_len == 0) {
    std::cout << parser.usage() << std::endl;
    return 1;
  }

  g_str = std::string(data_len, 'A');
  g_send_data = g_str;

  std::cout << "url: " << url << ", "
            << "client concurrency: " << worker_cnt << ", "
            << "data_len: " << data_len << ", "
            << "max_request_count: " << request_cnt << ", " << std::endl;

  auto hosts = std::vector<std::string_view>{url};
  auto chan = coro_io::load_blancer<coro_rpc_client>::create(
      hosts, {.pool_config{.max_connection = worker_cnt}});
  auto chan_ptr = std::make_shared<decltype(chan)>(std::move(chan));
  auto executor = coro_io::get_global_block_executor();
  for (int i = 0; i < worker_cnt; ++i) {
    call_echo(chan_ptr).start([=](auto &&res) {
      executor->schedule([res = std::move(res.value())]() mutable {
        result.insert(result.end(), res.begin(), res.end());
        --working_echo;
      });
    });
  }
  syncAwait(qps_watcher());
  latency_watcher();
  std::cout << "Done!" << std::endl;
  return 0;
}