#pragma once

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "duct/duct.h"
#include "duct/status.h"

namespace duct::pubsub {

// A topic-filtered message hub. A publisher calls publish(topic, message); each
// subscriber registers a topic filter and receives every matching message via
// its callback. The broker owns no transport; it is a pure in-process dispatch
// layer that can be driven by any Pipe on either side.
//
// This intentionally lives outside the core transport (see design audit: broadcast
// is a separate dispatch-layer problem) and does not assume SPSC rings.
class Broker {
 public:
  // Subscriber handle. Destroying it unsubscribes.
  class Subscription {
   public:
    Subscription(Broker* broker, std::uint64_t id) : broker_(broker), id_(id) {}
    ~Subscription() { if (broker_) broker_->unsubscribe(id_); }
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;
    std::uint64_t id() const { return id_; }
   private:
    Broker* broker_;
    std::uint64_t id_;
  };

  // Subscribe to messages whose topic matches `filter` (prefix match, so "" and
  // "foo" both match "foo/bar"). The callback runs on the publisher's thread.
  std::unique_ptr<Subscription> subscribe(std::string filter,
                                          std::function<void(std::string, Message)> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    const std::uint64_t id = next_id_++;
    subs_.push_back({id, std::move(filter), std::move(cb)});
    return std::make_unique<Subscription>(this, id);
  }

  // Publish a message to all subscribers whose filter is a prefix of `topic`.
  void publish(const std::string& topic, const Message& msg) {
    std::vector<std::function<void(std::string, Message)>> cbs;
    {
      std::lock_guard<std::mutex> lock(mu_);
      for (const auto& s : subs_) {
        if (topic.compare(0, s.filter.size(), s.filter) == 0) {
          cbs.push_back(s.cb);
        }
      }
    }
    for (auto& cb : cbs) cb(topic, msg);
  }

 private:
  struct Sub {
    std::uint64_t id;
    std::string filter;
    std::function<void(std::string, Message)> cb;
  };

  void unsubscribe(std::uint64_t id) {
    std::lock_guard<std::mutex> lock(mu_);
    subs_.erase(std::remove_if(subs_.begin(), subs_.end(),
                               [&](const Sub& s) { return s.id == id; }),
                subs_.end());
  }

  std::mutex mu_;
  std::vector<Sub> subs_;
  std::uint64_t next_id_ = 1;
};

}  // namespace duct::pubsub
