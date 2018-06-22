/* 
 * File:   slotdistribution.h
 * Author: whycold
 *
 * Created on 2014年9月22日, 下午3:57
 */
#include "slotdistribution.h"
#include <stdio.h>
#include <sstream>

uint16_t crc16(const char *buf, int len);

static const string NullAddress;


SlotDistribution::SlotDistribution() {
}

void SlotDistribution::RefreshSlotDistribution(const shared_ptr<map<SlotRange, string> >& slot_dist) {
    slot_distribution_ = slot_dist;
};

const string& SlotDistribution::GetRedisAddress(const string& key) {
    if (slot_distribution_.get() == 0) {
        return NullAddress;
    }
    uint16_t slot = crc16(key.c_str(), key.size()) % 16384;
    SlotRange slot_range;
    slot_range.begin = slot;
    slot_range.end = slot;
    map<SlotRange, string>::iterator iter = slot_distribution_->find(slot_range);
    if (iter != slot_distribution_->end()) {
        return iter->second;
    } else {
        printf("SlotDistribution::GetRedisAddress no redis response slot(%u)", slot);
        return NullAddress;
    }
} 

string SlotDistribution::GenerateRedisID(const string& ip, int port)
{
    std::stringstream id;
    id<<ip<<":"<<port;
    return id.str();
} 
