#ifndef REDIS_CMD_H_
#define REDIS_CMD_H_

#include <stdio.h>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <map>

// NOTE: redis command utility for RedisClient
// USAGE: std::string cmd(cmdstream("SET") << key << value);
class cmdstream
{
public:
    explicit cmdstream(const std::string & cmdName)
    {
        append(cmdName);
    }

    inline cmdstream & operator << (const std::string & key)
    {
        append(key);
        return *this;
    }
    inline cmdstream & operator << (const std::vector<std::string> & keys)
    {
        m_lines.insert(m_lines.end(), keys.begin(), keys.end());
        return *this;
    }
    inline cmdstream & operator << (int32_t ikey)
    {
        char buf[32];
        ::snprintf(buf, sizeof(buf), "%d", ikey);
        append(buf);
        return *this;
    }
    inline cmdstream & operator << (uint32_t uikey)
    {
        char buf[32];
        ::snprintf(buf, sizeof(buf), "%u", uikey);
        append(buf);
        return *this;
    }
    inline cmdstream & operator << (int64_t ikey)
    {
        char buf[64];
        ::snprintf(buf, sizeof(buf), "%ld", ikey);
        append(buf);
        return *this;
    }
    inline cmdstream & operator << (uint64_t uikey)
    {
        char buf[64];
        ::snprintf(buf, sizeof(buf), "%lu", uikey);
        append(buf);
        return *this;
    }

    operator std::string () const
    {
        const std::string endLine("\r\n");

        std::ostringstream oss;
        oss << '*' << m_lines.size() << endLine;
        for (std::vector<std::string>::const_iterator it = m_lines.begin();
                it != m_lines.end(); ++it)
        {
            oss << '$' << it->size() << endLine << *it << endLine;
        }

        return oss.str();
    }

    void reset(const std::string & cmdName)
    {
        m_lines.clear();
        append(cmdName);
    }

private:
    cmdstream(const cmdstream & s);
    cmdstream & operator = (const cmdstream & s);

    void append(const std::string & key)
    {
        m_lines.push_back(key);
    }

    std::vector<std::string> m_lines;
};

struct fmtkey
{
public:
    explicit fmtkey() : m_key() { }
    explicit fmtkey(const std::string & s) : m_key(s) { }

    inline fmtkey & operator << (const std::string & key)
    {
        append(key);
        return *this;
    }
    inline fmtkey & operator << (int32_t ikey)
    {
        char buf[32];
        ::snprintf(buf, sizeof(buf), "%d", ikey);
        append(buf);
        return *this;
    }
    inline fmtkey & operator << (uint32_t uikey)
    {
        char buf[32];
        ::snprintf(buf, sizeof(buf), "%u", uikey);
        append(buf);
        return *this;
    }
    inline fmtkey & operator << (int64_t ikey)
    {
        char buf[64];
        ::snprintf(buf, sizeof(buf), "%ld", ikey);
        append(buf);
        return *this;
    }
    inline fmtkey & operator << (uint64_t uikey)
    {
        char buf[64];
        ::snprintf(buf, sizeof(buf), "%lu", uikey);
        append(buf);
        return *this;
    }

    operator std::string () const
    {
        return m_key;
    }
    void clear()
    {
        m_key.clear();
    }

private:
    fmtkey(const fmtkey & s);
    fmtkey & operator = (const fmtkey & s);

    void append(const std::string & param)
    {
        m_key += param;
    }

    std::string m_key;
};

// common commands
class rediscmd
{
public:
    // global operations
    static std::string ping()
    {
        return cmdstream("PING");
    }
    static std::string cluster_nodes()
    {
        return cmdstream("CLUSTER") << "NODES";
    }   
    static std::string cluster_slots()
    {
        return cmdstream("CLUSTER") << "SLOTS";
    } 
    static std::string cluster_info()
    {
        return cmdstream("CLUSTER") << "INFO";
    } 	
    static std::string key_count()
    {
        return cmdstream("DBSIZE");
    }
    static std::string select_db(int dbNum)
    {
        return cmdstream("SELECT") << dbNum;
    }
    static std::string flush_all()
    {
        return cmdstream("FLUSHALL");
    }
    static std::string flushdb()
    {
        return cmdstream("FLUSHDB");
    }
    static std::string delete_key(const std::string & key)
    {
        return cmdstream("DEL") << key;
    }
    static std::string delete_mkey(const std::vector<std::string> & keys)
    {
        return cmdstream("DEL") << keys;
    }
    static std::string expire(const std::string &key, uint32_t timeout)
    {
        return cmdstream("EXPIRE") << key << timeout;
    }
    static std::string expireat(const std::string &key, uint32_t timeout)
    {
        return cmdstream("EXPIREAT") << key << timeout;
    }
    static std::string exists(const std::string &key)
    {
        return cmdstream("EXISTS") << key;
    }
    static std::string keys(const std::string &key)
    {
        return cmdstream("KEYS") << key;
    }
    static std::string ttl(const std::string &key)
    {
        return cmdstream("TTL") << key;
    }
    static std::string rename(const std::string& key, const std::string& newkey)
    {
        return cmdstream("RENAME") << key << newkey;
    }
    static std::string type(const std::string &key)
    {
        return cmdstream("TYPE") << key;
    }
    // Strings
    static std::string string_decrby(const std::string & key, const int64_t  val)
    {
        return cmdstream("DECRBY") << key << val;
    }
    static std::string string_set(const std::string & key, const std::string & val)
    {
        return cmdstream("SET") << key << val;
    }
    static std::string string_get(const std::string & key)
    {
        return cmdstream("GET") << key;
    }
    static std::string string_getset(const std::string & key, const std::string & val)
    {
        return cmdstream("GETSET") << key << val;
    }
    static std::string string_incr(const std::string & key)
    {
        return cmdstream("INCR") << key;
    }
    static std::string string_incrby(const std::string & key, const int64_t  val)
    {
        return cmdstream("INCRBY") << key << val;
    }
    static std::string string_decr(const std::string & key)
    {
        return cmdstream("DECR") << key;
    }
    static std::string string_mget(const std::vector<std::string> & keys)
    {
        return cmdstream("MGET") << keys;
    }
    static std::string string_mset(const std::map<std::string, std::string> & keys)
    {
        cmdstream cmd("MSET");
        for (std::map<std::string, std::string>::const_iterator iter = keys.begin(); iter != keys.end(); iter++) {
          cmd << iter->first << iter->second;
        }
        return cmd;
    }
    static std::string string_setex(const std::string & key, uint32_t seconds, const std::string & val)
    {
        return cmdstream("SET") << key << val << "EX" << seconds;
    }
    static std::string string_setpx(const std::string & key, const std::string & val, int64_t milliseconds)
    {
        return cmdstream("SET") << key << val << "PX" << milliseconds;
    }
    static std::string string_setnx(const std::string & key, const std::string & val)
    {
        return cmdstream("SET") << key << val << "NX";
    }
    static std::string string_setxx(const std::string & key, const std::string & val)
    {
        return cmdstream("SET") << key << val << "XX";
    }
    // Sets
    static std::string set_add(const std::string & key, const std::string & val)
    {
        return cmdstream("SADD") << key << val;
    }
    static std::string set_multi_add(const std::string & key, const std::vector<std::string>& values)
    {
        return cmdstream("SADD") << key << values;
    }
    static std::string set_remove(const std::string & key, const std::string & val)
    {
        return cmdstream("SREM") << key << val;
    }
    static std::string set_srem(const std::string& key, const std::vector<std::string>& values)
    {
        return cmdstream("SREM")<< key << values;
    }
    static std::string set_ismember(const std::string & key, const std::string & val)
    {
        return cmdstream("SISMEMBER") << key << val;
    }
    static std::string set_members(const std::string & key)
    {
        return cmdstream("SMEMBERS") << key;
    }
    static std::string set_count(const std::string & key)
    {
        return cmdstream("SCARD") << key;
    }
    static std::string set_randmembers(const std::string & key,const int32_t count)
    {
        return cmdstream("SRANDMEMBER")<< key << count;
    }
    static std::string set_pop(const std::string & key)
    {
        return cmdstream("SPOP") << key;
    }
		static std::string set_inter(const std::vector<std::string> & keys)
    {
        return cmdstream("SINTER") << keys;
    }

    // SORTED SET
    static std::string zset_add(const std::string & key, int64_t score, const std::string & val)
    {
        return cmdstream("ZADD") << key << score << val;
    }
    static std::string zset_madd(const std::string & key, const std::map<std::string, int64_t>& values)
    {
        cmdstream cmd("ZADD");
        cmd << key;
        for (std::map<std::string, int64_t>::const_iterator iter = values.begin(); iter != values.end(); iter++) {
          cmd << iter->second << iter->first;
        }
        return cmd;
    }
    static std::string zset_card(const std::string & key)
    {
        return cmdstream("ZCARD") << key;
    }
    static std::string zset_count(const std::string & key, int64_t min, int64_t max)
    {
        return cmdstream("ZCOUNT") << key << min << max;
    }
    static std::string zset_incrby(const std::string & key, int64_t score, const std::string & val)
    {
        return cmdstream("ZINCRBY") << key << score << val;
    }
    static std::string zset_rank(const std::string & key, const std::string & val)
    {
        return cmdstream("ZRANK") << key << val;
    }
    static std::string zset_rem(const std::string & key, const std::string & val)
    {
        return cmdstream("ZREM") << key << val;
    }
    static std::string zset_mrem(const std::string & key, const std::vector<std::string> & val)
    {
        return cmdstream("ZREM") << key << val;
    }
    static std::string zset_revrange(const std::string & key, int64_t start, int64_t stop)
    {
        return cmdstream("ZREVRANGE") << key << start << stop;
    }
    static std::string zset_revrange_withscore(const std::string & key, int64_t start, int64_t stop)
    {
        return cmdstream("ZREVRANGE") << key << start << stop << "WITHSCORES";
    }
    static std::string zset_range_withscore(const std::string & key, int64_t start, int64_t stop)
    {
        return cmdstream("ZRANGE") << key << start << stop << "WITHSCORES";
    }
    static std::string zset_score(const std::string & key, const std::string & val)
    {
        return cmdstream("ZSCORE") << key << val;
    }

    static std::string zset_revrank(const std::string & key, const std::string & val)
    {
        return cmdstream("ZREVRANK") << key << val;
    }
    // Lists
    static std::string list_rpush(const std::string & key, const std::vector<std::string> & val)
    {
        return cmdstream("RPUSH") << key << val;
    }
    static std::string list_lpush(const std::string & key, const std::vector<std::string> & val)
    {
        return cmdstream("LPUSH") << key << val;
    }
    static std::string list_rpop(const std::string & key)
    {
        return cmdstream("RPOP") << key;
    }
    static std::string list_lpop(const std::string & key)
    {
        return cmdstream("LPOP") << key;
    }
    static std::string list_lrem(const std::string & key, const uint32_t count, const std::string& val)
    {
        return cmdstream("LREM") << key << count << val;
    }
    static std::string list_len(const std::string & key)
    {
        return cmdstream("LLEN") << key;
    }
    static std::string list_range(const std::string & key, int start, int stop)
    {
        return cmdstream("LRANGE") << key << start << stop;
    }
    static std::string list_multi_lpush(const std::string & key, const std::vector<std::string>& values)
    {
        return cmdstream("LPUSH") << key << values;
    }
    static std::string list_lindex(const std::string & key, const uint32_t index)
    {
        return cmdstream("LINDEX") << key << index;
    }

    // Hashes
    static std::string hash_set(const std::string & key, const std::string & field, const std::string & val)
    {
        return cmdstream("HSET") << key << field << val;
    }
    static std::string hash_get(const std::string & key, const std::string & field)
    {
        return cmdstream("HGET") << key << field;
    }
    static std::string hash_del(const std::string & key, const std::string & field)
    {
        return cmdstream("HDEL") << key << field;
    }

    static std::string hash_mdel(const std::string & key, const std::vector<std::string> & fields)
    {
        return cmdstream("HDEL") << key << fields;
    }
    static std::string hash_exists(const std::string & key, const std::string & field)
    {
        return cmdstream("HEXISTS") << key << field;
    }
    static std::string hash_keys(const std::string & key)
    {
        return cmdstream("HKEYS") << key;
    }
    static std::string hash_getall(const std::string & key)
    {
        return cmdstream("HGETALL") << key;
    }
    static std::string hash_len(const std::string & key)
    {
        return cmdstream("HLEN") << key;
    }
    static std::string hash_mset(const std::string& key, const std::map<std::string, std::string>& values)
    {
        cmdstream cmd("HMSET");
        cmd << key;
        for (std::map<std::string, std::string>::const_iterator iter = values.begin(); iter != values.end(); iter++) {
          cmd << iter->first << iter->second;
        }
        return cmd;
    }
    static std::string hash_mget(const std::string & key, const std::vector<std::string> & field)
    {
      return cmdstream("HMGET") << key << field;
    }
    static std::string hash_incrby(const std::string & key, const std::string & field, const int64_t incr)
    {
      return cmdstream("HINCRBY") << key << field << incr;
    }
    static std::string hash_setnx(const std::string & key, const std::string & field, const std::string & value)
    {
      return cmdstream("HSETNX") << key << field << value;
    }
    static std::string hash_vals(const std::string & key)
    {
        return cmdstream("HVALS") << key;
    }
    // Pub/Sub
    static std::string publish(const std::string & ch, const std::string & msg)
    {
        return cmdstream("PUBLISH") << ch << msg;
    }
    static std::string subscribe(const std::string & ch)
    {
        return cmdstream("SUBSCRIBE") << ch;
    }
    static std::string unsubscribe(const std::string & ch)
    {
        return cmdstream("UNSUBSCRIBE") << ch;
    }
    static std::string pattern_subscribe(const std::string & ch)
    {
        return cmdstream("PSUBSCRIBE") << ch;
    }
    static std::string pattern_unsubscribe(const std::string & ch)
    {
        return cmdstream("PUNSUBSCRIBE") << ch;
    }

    // Lua script
    // NOTE: keys -> KEYS & ARGV
    template<typename T>
    static std::string eval(const std::string & script, uint32_t numkeys,
            std::vector<T> & keys)
    {
        cmdstream cs("EVAL");
        cs << script << numkeys;

        for (typename std::vector<T>::iterator it = keys.begin();
                it != keys.end(); ++it) {
            cs << *it;
        }
        return cs;
    }

    // SCANs
    // SCAN
    static std::string scan(uint64_t cursor, uint32_t count)
    {
        return cmdstream("SCAN") << cursor << "COUNT" << count;
    }
    // SSCAN
    static std::string sscan(const std::string & key, uint64_t cursor, uint32_t count)
    {
        return cmdstream("SSCAN") << key << cursor << "COUNT" << count;
    }
    // HSCAN
    static std::string hscan(const std::string & key, uint64_t cursor, uint32_t count)
    {
        return cmdstream("HSCAN") << key << cursor << "COUNT" << count;
    }
    // ZSCAN
    static std::string zscan(const std::string & key, uint64_t cursor, uint32_t count)
    {
        return cmdstream("ZSCAN") << key << cursor << "COUNT" << count;
    }
};


#endif
