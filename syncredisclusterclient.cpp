/* 
 * File:   syncredisclusterclient.cpp
 * Author: whycold
 * 
 * Created on 2014年9月22日, 下午3:57
 */

#include "syncredisclusterclient.h"
#include <stdio.h>
#include <string.h>
#include "appsyncredisclient.h"
#include "rediscmd.h"


void explode(char c, const string& input, map<uint32_t, string>& _output){
    uint32_t i = 0;
    char *last_pos = (char*)input.c_str();

    while(1){
        char *p_n = strchr(last_pos, c);
        if(p_n){
            int n = p_n - last_pos;
            _output[i++].assign(last_pos, n);
            last_pos = p_n + 1;
        }else{
            _output[i++] = last_pos;
            break;
        }
    }
}

SyncRedisClusterClient::SyncRedisClusterClient(const string& seed_node_ip, int seed_node_port, int time_out)
: cluster_ready_(false), time_out_(time_out) {
    seed_address_ = slot_distribution_.GenerateRedisID(seed_node_ip, seed_node_port);
    seed_sync_redis_client_.reset(new AppSyncRedisClient(seed_node_ip, seed_node_port, time_out));

}

int SyncRedisClusterClient::Del(const string & key, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::delete_key(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        result = reply->integer;
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Del err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Del err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::Expire(const string& key, uint32_t time) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::expire(key, time));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 1) {
            ret = 0;
        } else {
            ret = 1;
        }
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Expire err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Expire err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::Exists( const string & key, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::exists(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Exists err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Exists err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}

int SyncRedisClusterClient::Rename( const string & key, string& newkey) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::rename(key, newkey));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STATUS && strncmp(reply->str, "OK", 2) == 0) {
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Rename err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Rename err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}

int SyncRedisClusterClient::Ttl( const string & key, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::ttl(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Ttl err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Ttl err:can't connect, key:%s", key.c_str());
    }
    return ret;        
}

int SyncRedisClusterClient::Type( const string & key, string & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::type(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Type err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Type err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}


int SyncRedisClusterClient::Decr(const string& key, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_decr(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Decr err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Decr err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::DecrBy(const string& key, int64_t dec_value, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_decrby(key, dec_value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::DecrBy err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::DecrBy err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::Incr(const string& key, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_incr(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Incr err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Incr err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::IncrBy(const string& key, int64_t value, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_incrby(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::IncrBy err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::IncrBy err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::Get(const string& key, string& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_get(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Get err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Get err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::GetSet(const string& key, const string& value, string& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_getset(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::GetSet err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::GetSet err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::Set(const string& key, const string& value) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_set(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STATUS && strncmp(reply->str, "OK", 2) == 0) {
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Set err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Set err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SetEx(const string& key, const string& value, int32_t seconds) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_setex(key, seconds, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STATUS && strncmp(reply->str, "OK", 2) == 0) {
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SetEx err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SetEx err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SetPx(const string& key, const string& value, int64_t milliseconds) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_setpx(key, value, milliseconds));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STATUS && strncmp(reply->str, "OK", 2) == 0) {
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SetPx err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SetPx err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SetNx(const string& key, const string& value, bool& have_exist) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_setnx(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_NIL) {
        have_exist = true;
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_STATUS && strncmp(reply->str, "OK", 2) == 0) {
        have_exist = false;
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SetNx err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SetNx err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SetXx(const string& key, const string& value, bool& have_exist) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::string_setxx(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_NIL) {
        have_exist = false;
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_STATUS && strncmp(reply->str, "OK", 2) == 0) {
        have_exist = true;
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SetXx err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SetXx err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SAdd(const string& key, const string& value, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::set_add(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SAdd err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SAdd err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SCard(const string& key, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::set_count(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SCard err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SCard err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SIsMember(const string& key, const string& value, bool& is_member) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::set_ismember(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        if (reply->integer > 0) {
            is_member = true;
        } else {
            is_member = false;
        }
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SIsMember err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SIsMember err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SRem(const std::string & key, const std::string& value, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::set_remove(key, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SRem err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SRem err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::SMembers(const string& key, vector< string >& results) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::set_members(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        ret = 0;
        int element_count = reply->elements;
        redisReply **element = reply->element;

        for (int i = 0; i < element_count; i++) {
            results.push_back(element[i]->str);
        }
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::SMembers err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::SMembers err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

// hashs
int SyncRedisClusterClient::HSet(const string& key, const string& field, const string& value, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_set(key, field, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HSet err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HSet err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::HSetnx( const string& key, const string& field, const string& value, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_setnx(key, field, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HSetnx err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HSetnx err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}


int SyncRedisClusterClient::HDel(const string& key, const string& field, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_del(key, field));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HDel err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HDel err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::HExists(const string& key, const string& field, int64_t & result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_exists(key, field));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HExists err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HExists err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}

int SyncRedisClusterClient::HGet(const string& key, const string& field, string& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_get(key, field));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HGet err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HGet err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::HGetAll(const string& key, map<string, string>& results) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_getall(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        int element_count = reply->elements;
        redisReply **element = reply->element;
        int i = 0;
        int i_next = i + 1;
        while (i_next < element_count) {
            results[(element[i]->str)] = element[i_next]->str;
            i = i + 2;
            i_next = i + 1;
        }
        ret = 0;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HGetAll err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HGetAll err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::HLen(const string& key, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_len(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HLen err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HLen err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::HIncrby(const string & key, string& field, int64_t incr_value, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_incrby(key, field, incr_value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HIncrby err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HIncrby err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}

int SyncRedisClusterClient::HKeys(const string& key, vector<string>& fields) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_keys(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        ret = 0;
        int element_count = reply->elements;
        redisReply **element = reply->element;
        for (int i = 0; i < element_count; i++) {
            fields.push_back(element[i]->str);
        }
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HKeys err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HKeys err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}
int SyncRedisClusterClient::HVals( const string& key, vector<string>& results) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::hash_vals(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        ret = 0;
        int element_count = reply->elements;
        redisReply **element = reply->element;
        for (int i = 0; i < element_count; i++) {
            results.push_back(element[i]->str);
        }
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::HVals err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::HVals err:can't connect, key:%s", key.c_str());
    }
    return ret;      
}

int SyncRedisClusterClient::ZAdd(const string & key, int64_t score, const string& member, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_add(key, score, member));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::ZAdd err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::ZAdd err:can't connect, key:%s", key.c_str());
    }
    return ret;       
}

int SyncRedisClusterClient::MZAdd(const string & key, const map<string, int64_t>& values, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_madd(key, values));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::MZAdd err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::MZAdd err:can't connect, key:%s", key.c_str());
    }
    return ret;       
}

int SyncRedisClusterClient::ZCount( const string & key, int64_t min, int64_t max, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_count(key, min, max));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::ZCount err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::ZCount err:can't connect, key:%s", key.c_str());
    }
    return ret;     
}
int SyncRedisClusterClient::ZIncrBy(const string & key, int64_t incr_value, const string& member, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_incrby(key, incr_value, member));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::ZIncrBy err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::ZIncrBy err:can't connect, key:%s", key.c_str());
    }
    return ret;     
}

int SyncRedisClusterClient::ZRank( const string & key, const string& member, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_rank(key, member));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::ZRank err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::ZRank err:can't connect, key:%s", key.c_str());
    }
    return ret;      
}

int SyncRedisClusterClient::ZRem( const string & key, const string& member, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_rem(key, member));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::ZRem err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::ZRem err:can't connect, key:%s", key.c_str());
    }
    return ret;      
}

int SyncRedisClusterClient::MZRem( const string & key, const vector<string>& members, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_mrem(key, members));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::MZRem err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::MZRem err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}

int SyncRedisClusterClient::ZScore( const string & key, const string& member, string& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::zset_score(key, member));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::ZScore err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::ZScore err:can't connect, key:%s", key.c_str());
    }
    return ret;    
}

int SyncRedisClusterClient::LPush(const string& key, const vector<string>& values, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_lpush(key, values));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::LPush err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::LPush err:can't connect, key:%s", key.c_str());
    }
    return ret;       
}
int SyncRedisClusterClient::RPush(const string& key, const vector<string>& values, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_rpush(key, values));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::RPush err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::RPush err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

int SyncRedisClusterClient::LPop(const string& key, string& result){
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_lpop(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::LPop err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::LPop err:can't connect, key:%s", key.c_str());
    }
    return ret;     
}

int SyncRedisClusterClient::RPop(const string& key, string& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_rpop(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::RPop err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::RPop err:can't connect, key:%s", key.c_str());
    }
    return ret;        
}
int SyncRedisClusterClient::LLen(const string& key, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_len(key));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::LLen err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::LLen err:can't connect, key:%s", key.c_str());
    }
    return ret;      
} 

int SyncRedisClusterClient::LRem(const string& key, int64_t count, const string& value, int64_t& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_lrem(key, count, value));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::LRem err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::LRem err:can't connect, key:%s", key.c_str());
    }
    return ret;   
}

int SyncRedisClusterClient::LIndex(const string& key, int64_t index, string& result) {
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_lindex(key, index));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        ret = 0;
        result = reply->str;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        ret = 1;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::LIndex err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::LIndex err:can't connect, key:%s", key.c_str());
    }
    return ret;            
}

int SyncRedisClusterClient::LRange(const string& key, int64_t start, int64_t stop, vector<string>& result){
    int ret = -1;
    string cmd;
    cmd.assign(rediscmd::list_range(key, start, stop));
    redisReply* reply = this->RedisCommand(key, cmd);
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        ret = 0;
        int element_count = reply->elements;
        redisReply **element = reply->element;
        for (int i = 0; i < element_count; i++) {
            result.push_back(element[i]->str);
        }
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::LRange err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::LRange err:can't connect, key:%s", key.c_str());
    }
    return ret;       
}

//eval
//NOTE: keys -> KEYS & ARGV
int SyncRedisClusterClient::Eval(const string& key, const string& script, uint32_t numkeys, vector<string> &keys, int64_t & result) {
    string cmd;
    cmd.assign(rediscmd::eval(script, numkeys, keys));
    redisReply* reply = this->RedisCommand(key, cmd);
    int ret = -1;
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        ret = 0;
        result = reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_ERROR) {
        printf("SyncRedisClusterClient::Eval err:%s, key:%s", reply->str, key.c_str());
    } else {
        printf("SyncRedisClusterClient::Eval err:can't connect, key:%s", key.c_str());
    }
    return ret;
}

redisReply* SyncRedisClusterClient::RedisCommand(const string& partition_key, const string & cmd) {
    if (cluster_ready_ == false) {
        if (GetClusterSlotInfo() == true) {
            cluster_ready_ = true;
        } else {
            printf("SyncRedisClusterClient::RedisCommand failed to get cluste slots");
        }
    }

    shared_ptr<AppSyncRedisClient> response_client;
    if (cluster_ready_) {
        string redis_address = slot_distribution_.GetRedisAddress(partition_key);
        map<string, shared_ptr<AppSyncRedisClient> >::iterator find = syn_redis_client_map_.find(redis_address);
        if (find != syn_redis_client_map_.end()) {
            response_client = find->second;
        }
        if (response_client.get() == 0) {
            cluster_ready_ = false;
        }
    } else {
        printf("[+]SyncRedisClusterClient::RedisCommand cluster not ready!");
    }

    redisReply* reply = NULL;
    if (response_client.get() != 0) {
        reply = response_client->exeRedisCommand(cmd);
        bool needgetclusterslotinfo = false;
        if (NULL == reply) {
            needgetclusterslotinfo = true;
        } else if (reply->type == REDIS_REPLY_ERROR) {
            string error(reply->str, reply->len);
            map<uint32_t, string> error_array;
            explode(' ', error, error_array);
            if (error_array[0] == "MOVED") {
                needgetclusterslotinfo = true;
            }
        }

        if (needgetclusterslotinfo) {
            if (GetClusterSlotInfo() == false) {
                cluster_ready_ = false;
                printf("SyncRedisClusterClient::RedisCommand failed to get cluste slots");
            }
        }
    }
    return reply;
}

shared_ptr<AppSyncRedisClient> SyncRedisClusterClient::GetHealthSyncRedisClient(string& _address) {
    for (map<string, shared_ptr<AppSyncRedisClient> >::iterator iter = syn_redis_client_map_.begin(); iter != syn_redis_client_map_.end(); iter++) {
        if (this->IsConnected(iter->second) == true) {
            _address = iter->first;
            return iter->second;
        }
    }
    return shared_ptr<AppSyncRedisClient>();
}

bool SyncRedisClusterClient::GetClusterSlotInfo() {
    bool ret = false;
    if (seed_sync_redis_client_.get() == 0 || (seed_sync_redis_client_.get() != 0 && this->IsConnected(seed_sync_redis_client_) == false)) {
        string address;
        shared_ptr<AppSyncRedisClient> healh_client = GetHealthSyncRedisClient(address);
        if (healh_client.get() != 0) {
            seed_sync_redis_client_ = healh_client;
            seed_address_ = address;
        }
    }
    if (seed_sync_redis_client_.get() != 0) {
        redisReply* reply = seed_sync_redis_client_->exeRedisCommand(rediscmd::cluster_nodes());
        if (reply && reply->type == REDIS_REPLY_STRING) {
            string cluster_nodes(reply->str, reply->len);
            RefreshSlotDistribution(cluster_nodes);
            ret = true;
        }
    }
    return ret;
}

void SyncRedisClusterClient::RefreshSlotDistribution(const string& bulk) {
    map<uint32_t, string> node_infos;
    explode('\n', bulk, node_infos);

    syn_redis_client_map_.clear();
    shared_ptr<map<SlotRange, string> > slot_dist(new map<SlotRange, string>());
    map<uint32_t, string> bulk_array;
    map<uint32_t, string> slot_array;
    map<uint32_t, string> address_array;
    for (map<uint32_t, string>::iterator iter = node_infos.begin(); iter != node_infos.end(); iter++) {
        bulk_array.clear();
        explode(' ', iter->second, bulk_array);
        if (bulk_array[2] == "master") {
            string address = bulk_array[1];
            for (size_t i = 8; i < bulk_array.size(); i++) {
                slot_array.clear();
                explode('-', bulk_array[i], slot_array);
                SlotRange slot_range;
                slot_range.begin = atoi(slot_array[0].c_str());
                if (slot_array.size() == 2) {
                    slot_range.end = atoi(slot_array[1].c_str());
                } else {
                    slot_range.end = slot_range.begin;
                }
                printf("SyncRedisClusterClient::RefreshSlotDistribution slot(%u-%u)", slot_range.begin, slot_range.end);
                slot_dist->insert(make_pair(slot_range, address));
            }

            address_array.clear();
            explode(':', address, address_array);
            string ip = address_array[0];
            int port = atoi(address_array[1].c_str());
            printf("SyncRedisClusterClient::RefreshSlotDistribution %s -> %s %u", address.c_str(), ip.c_str(), port);
            shared_ptr<AppSyncRedisClient> local_client(new AppSyncRedisClient(ip, port, time_out_));
            syn_redis_client_map_.insert(make_pair(address, local_client));
        }
        if (bulk_array[2] == "myself,master") {
            for (size_t i = 8; i < bulk_array.size(); i++) {
                slot_array.clear();
                explode('-', bulk_array[i], slot_array);
                SlotRange slot_range;
                slot_range.begin = atoi(slot_array[0].c_str());
                if (slot_array.size() == 2) {
                    slot_range.end = atoi(slot_array[1].c_str());
                } else {
                    slot_range.end = slot_range.begin;
                }
                printf("SyncRedisClusterClient::RefreshSlotDistribution slot(%u-%u)", slot_range.begin, slot_range.end);
                slot_dist->insert(make_pair(slot_range, seed_address_));
            }

            printf("SyncRedisClusterClient::RefreshSlotDistribution myself %s", seed_address_.c_str());
            syn_redis_client_map_.insert(make_pair(seed_address_, seed_sync_redis_client_));
        }
    }

    slot_distribution_.RefreshSlotDistribution(slot_dist);
}

bool SyncRedisClusterClient::IsConnected(shared_ptr<AppSyncRedisClient> client) {
    bool ret = false;
    if (client.get() == 0) {
        printf("[-]SyncRedisClusterClient::IsConnected client is null, return false");
        return ret;
    }

    // 因为只能发包才会创建连接，所以通过发包来判断是否连接上
    redisReply *reply = client->exeRedisCommand(rediscmd::ping());
    if (reply && reply->type == REDIS_REPLY_STATUS) {
        ret = true;
    }
    return ret;
}

