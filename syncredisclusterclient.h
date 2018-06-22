/* 
 * File:   syncredisclusterclient.h
 * Author: whycold
 *
 * Created on 2014年9月22日, 下午3:57
 */

#ifndef SYNCREDISCLUSTERCLIENT_H_
#define	SYNCREDISCLUSTERCLIENT_H_

#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include <boost/shared_ptr.hpp>
#include "slotdistribution.h"


class AppSyncRedisClient;
struct redisReply;

using ::string;
using ::std::map;
using ::std::vector;
using ::boost::shared_ptr;

class SyncRedisClusterClient {
public:
  SyncRedisClusterClient(const string& seed_node_ip, int seed_node_port, int time_out);
   
  // keys
  int Del(const string & key, int64_t & result);
  int Expire(const string& key, uint32_t time );  
  int Exists( const string & key, int64_t& result);
  int Rename( const string & key, string& newkey);
  int Ttl( const string & key, int64_t& result);
  int Type( const string & key, string & result);  
  
  // strings
  int Decr(const string& key, int64_t& result);
  int DecrBy(const string& key, int64_t dec_value, int64_t& result);
  int Incr(const string& key, int64_t& result);
  int IncrBy(const string& key, int64_t value, int64_t & result );   
  int Get(const string& key, string& result);
  int GetSet(const string& key, const string& value, string& result);
  int Set(const string& key, const string& value);
  int SetEx(const string& key, const string& value, int32_t seconds);
  int SetPx(const string& key, const string& value, int64_t milliseconds);  
  int SetNx(const string& key, const string& value, bool& have_exist);
  int SetXx(const string& key, const string& value, bool& have_exis);  
  int Scan(uint64_t cursor, uint32_t count, uint64_t& ret_cursor, ::std::vector< ::string >& result);

  //sets
  int SAdd(const string& key, const string& value, int64_t & result );
  int SCard( const string& key, int64_t& result );
  int SIsMember( const string& key, const string& value, bool& is_member );
  int SRem( const string & key, const string& value, int64_t& result );
  int SMembers(const string& key, vector< string >& results);  
  
  // hashs
  int HSet(const string& key, const string& field, const string& value, int64_t& result);  
  int HSetnx( const string& key, const string& field, const string& value, int64_t& result);
  int HDel(const string& key, const string& field, int64_t & result);
  int HExists(const string& key, const string& field, int64_t & result);
  int HGet(const string& key, const string& field, string& result);
  int HGetAll(const string& key, map<string, string>& results);
  int HLen(const string& key, int64_t& result);
  int HIncrby(const string & key, string& field, int64_t incr_value, int64_t& result);
  int HKeys(const string& key, vector<string>& fields);
  int HVals( const string& key, vector<string>& results);  
  
  //  sorted set
  int ZAdd(const string & key, int64_t score, const string& member, int64_t& result);
  int MZAdd(const string & key, const map<string, int64_t>& values, int64_t& result);
  int ZCount( const string & key, int64_t min, int64_t max, int64_t& result);
  int ZIncrBy(const string & key, int64_t incr_value, const string& member, int64_t& result);
  int ZRank( const string & key, const string& member, int64_t& result);
  int ZRem( const string & key, const string& member, int64_t& result);
  int MZRem( const string & key, const vector<string>& members, int64_t& result);
  int ZScore( const string & key, const string& member, string& result);  
  
  //list
  int LPush(const string& key, const vector<string>& values, int64_t& result);
  int RPush(const string& key, const vector<string>& values, int64_t& result);
  int LPop(const string& key, string& result);
  int RPop(const string& key, string& result);
  int LLen(const string& key, int64_t& result);  
  int LRem(const string& key, int64_t count, const string& value, int64_t& result);
  int LIndex(const string& key, int64_t index, string& result);
  int LRange(const string& key, int64_t start, int64_t stop, vector<string>& result);
  
  //eval
  int Eval(const string& key, const string& script, uint32_t numkeys, vector<string> &keys, int64_t & result);
  
//private:
  redisReply* RedisCommand(const string& partition_key, const string & cmd);

private:
  shared_ptr<AppSyncRedisClient> GetHealthSyncRedisClient(string& _address);

  bool GetClusterSlotInfo();

  void RefreshSlotDistribution(const string & bulk);

  bool IsConnected(shared_ptr<AppSyncRedisClient> client);

private:
  string seed_address_;
  shared_ptr<AppSyncRedisClient> seed_sync_redis_client_;
  map<string, shared_ptr<AppSyncRedisClient> > syn_redis_client_map_;
  SlotDistribution slot_distribution_;
  bool cluster_ready_;
  int  time_out_;
};


#endif	/* SYNCREDISCLUSTERCLIENT_H_ */

