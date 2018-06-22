/* 
 * File:   slotdistribution.h
 * Author: whycold
 *
 * Created on 2014年9月22日, 下午3:57
 */
#ifndef SLOTDISTRIBUTION_H_
#define	SLOTDISTRIBUTION_H_

#include <stdint.h>
#include <map>
#include <string>
#include <boost/shared_ptr.hpp>

using ::std::map;
using ::std::string;
using ::boost::shared_ptr;
 
class SlotRange {
public:
    uint16_t begin;
    uint16_t end;  
    
    bool operator< (const SlotRange& r) const {
        if (this->end < r.begin) {
            return true;
        } else {               
            return false;
        }
    };    
};     
    
    
class SlotDistribution {  
public:
    SlotDistribution();
    
    void RefreshSlotDistribution(const ::boost::shared_ptr<map<SlotRange, string> >& slot_dist);
    
    const string& GetRedisAddress(const string& key); 
    
    string GenerateRedisID(const string& ip, int port);  
    
private:
    ::boost::shared_ptr<map<SlotRange, string> > slot_distribution_;    
    
};

#endif	/* SLOTDISTRIBUTION_H_ */

