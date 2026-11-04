#pragma once

#include <algorithm>
#include <atomic>
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
  struct Sub {
    std::uint64_t id;
    std::string filter;
    std::function<void(std::string, Message)> cb;
  };

  // Shared, reference-counted state. Both the Broker and every Subscription
  // hold a reference, so the mutex/subs live as long as any Subscription does
  // and a destructor racing the broker cannot touch freed memory.
  struct State {
    std::mutex mu;
    std::vector<Sub> subs;
    std::uint64_t next_id = 1;
    std::atomic<bool> alive{true};
  };

  ~Broker() {
    // Invalidate subscriptions before destroying internal state so a
    // Subscription outliving the Broker cannot call into freed memory.
    state_->alive.store(false, std::memory_order_release);
  }

  // Subscriber handle. Destroying it unsubscribes.
  class Subscription {
   public:
    Subscription(std::shared_ptr<State> state, std::uint64_t id)
        : state_(std::move(state)), id_(id) {}
    ~Subscription() {
      // Check-then-act: the broker may be concurrently destroyed. The shared
      // State outlives the broker (we hold a reference), so the unsubscribe
      // below locks a live mutex even if ~Broker already ran.
      if (state_->alive.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->subs.erase(
            std::remove_if(state_->subs.begin(), state_->subs.end(),
                           [&](const Sub& s) { return s.id == id_; }),
            state_->subs.end());
      }
    }
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;
    std::uint64_t id() const { return id_; }
   private:
    std::shared_ptr<State> state_;
    std::uint64_t id_;
  };

  // Subscribe to messages whose topic matches `filter` (prefix match, so "" and
  // "foo" both match "foo/bar"). The callback runs on the publisher's thread.
  std::unique_ptr<Subscription> subscribe(std::string filter,
                                          std::function<void(std::string, Message)> cb) {
    std::lock_guard<std::mutex> lock(state_->mu);
    const std::uint64_t id = state_->next_id++;
    state_->subs.push_back({id, std::move(filter), std::move(cb)});
    return std::make_unique<Subscription>(state_, id);
  }

  // Publish a message to all subscribers whose filter is a prefix of `topic`.
  void publish(const std::string& topic, const Message& msg) {
    std::vector<std::function<void(std::string, Message)>> cbs;
    {
      std::lock_guard<std::mutex> lock(state_->mu);
      for (const auto& s : state_->subs) {
        if (topic.compare(0, s.filter.size(), s.filter) == 0) {
          cbs.push_back(s.cb);
        }
      }
    }
    for (auto& cb : cbs) cb(topic, msg);
  }

 private:
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

}  // namespace duct::pubsub
