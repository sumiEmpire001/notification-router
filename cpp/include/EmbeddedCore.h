#pragma once
#include <stdint.h>
#include <stddef.h>
namespace lord_sumi {
enum class GateDecision { Allow, Quiet, Disabled, Invalid, Fault };
struct GateConfig { uint16_t quiet_start_min,quiet_end_min;bool enabled; };
class NotificationGate {
 public:
  explicit NotificationGate(GateConfig c):config_(c),last_minute_(0),seen_(false),fault_(c.quiet_start_min>=1440||c.quiet_end_min>=1440){}
  GateDecision decide(uint64_t utc_minute,uint16_t local_minute) {
    if(fault_)return GateDecision::Fault;
    if(local_minute>=1440||(seen_&&utc_minute<last_minute_)){fault_=true;return GateDecision::Invalid;}
    last_minute_=utc_minute;seen_=true;
    if(!config_.enabled)return GateDecision::Disabled;
    const uint16_t a=config_.quiet_start_min,b=config_.quiet_end_min;
    if(a==b)return GateDecision::Allow;
    bool quiet=a<b?(local_minute>=a&&local_minute<b):(local_minute>=a||local_minute<b);
    return quiet?GateDecision::Quiet:GateDecision::Allow;
  }
  bool fault()const{return fault_;}
  void reset(){last_minute_=0;seen_=false;fault_=config_.quiet_start_min>=1440||config_.quiet_end_min>=1440;}
 private:GateConfig config_;uint64_t last_minute_;bool seen_,fault_;
};
}