#ifndef APPSYNCREDISCLIENT_H_
#define APPSYNCREDISCLIENT_H_

#include "hiredis/hiredis.h"
#include <stdint.h>
#include <string>
#include <vector>

class AppSyncRedisClient
{
public:
    // redis ip:port && connect/read/write timeout (default 2000 millisecond) 
    AppSyncRedisClient(std::string ip, int port, int timeout = 2000);
    ~AppSyncRedisClient();

    // execute redis 'formatted' cmd, please use 'rediscmd' to make redis cmd string
    // return NULL if redis is not connected, app need not free the returned redisReply
    redisReply * exeRedisCommand(const std::string & cmd);

    // set read/write timeout, 1 usec(millisecond) = 1/1000 second
    bool setTimeout(int usec);

    // clear backups & reset current redis ip:port
    void resetRedisIpPort(std::string ip, int port);

    // connect to redis ip:port
    bool connectRedis();
    // get if redis is connected
    bool isConnected();
    // if there is any err
    std::string getLastErr();

    
private:
    void collectLastErr();
    int _exeRedisCommand(const std::string & cmd);
    std::string getIpByHostName(const char * hostname);
    std::string getIpByHostName_r(const char * hostname);

private:
    std::string     m_strErr;
    std::string     m_ip;
    int             m_port;
    int             m_timeout;
    bool            m_connected;
    redisContext *      m_pRedisCtx;
    redisReply   *      m_pRedisReply;

};


#endif
