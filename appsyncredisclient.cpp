#include "appsyncredisclient.h"
#include <stdio.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h> 
#include <stdlib.h>
#include <string.h>
#include "rediscmd.h"

AppSyncRedisClient::AppSyncRedisClient(std::string ip, int port, int timeout)
: m_ip(ip), m_port(port), m_timeout(timeout), m_connected(false),
m_pRedisCtx(NULL), m_pRedisReply(NULL) {
}

AppSyncRedisClient::~AppSyncRedisClient() {
    if (NULL != m_pRedisCtx) {
        ::redisFree(m_pRedisCtx);
        m_pRedisCtx = NULL;
    }
    if (NULL != m_pRedisReply) {
        ::freeReplyObject(m_pRedisReply);
        m_pRedisReply = NULL;
    }
}

bool AppSyncRedisClient::connectRedis() {
    if (NULL != m_pRedisCtx) {
        ::redisFree(m_pRedisCtx);
        m_pRedisCtx = NULL;
    }

    struct timeval tv;
    tv.tv_sec = m_timeout / 1000;
    tv.tv_usec = (m_timeout % 1000) * 1000;

    std::string strIp = getIpByHostName_r(m_ip.c_str());
    if (strIp.empty()) {
        return false;
    }
    m_pRedisCtx = ::redisConnectWithTimeout(strIp.c_str(), m_port, tv);
    if (NULL == m_pRedisCtx) return false;

    m_connected = false;
    if (m_pRedisCtx->err) {
        m_strErr = m_pRedisCtx->errstr;
    } 

    return m_connected;
}


void AppSyncRedisClient::resetRedisIpPort(std::string ip, int port) {
    m_ip = ip;
    m_port = port;
}

bool AppSyncRedisClient::isConnected() {
    return m_connected;
}

bool AppSyncRedisClient::setTimeout(int usec) {
    m_timeout = usec;

    if (NULL == m_pRedisCtx) return false;

    struct timeval tv;
    tv.tv_sec = m_timeout / 1000;
    tv.tv_usec = (m_timeout % 1000) * 1000;

    int ret = ::redisSetTimeout(m_pRedisCtx, tv);
    return (REDIS_OK == ret);
}

std::string AppSyncRedisClient::getLastErr() {
    return m_strErr;
}

void AppSyncRedisClient::collectLastErr() {
    m_strErr = "redis-err: no err detected";

    char buf[256];

    if (NULL == m_pRedisCtx) {
        ::snprintf(buf, sizeof (buf), "redis-err: not connected. ip:%s port:%u", m_ip.c_str(), m_port);
        m_strErr = buf;
        return;
    }

    if (m_pRedisCtx->err) {
        // important
        m_connected = false;

        ::snprintf(buf, sizeof (buf), "redis-err: context err:%d %s ip:%s port:%u",
                m_pRedisCtx->err, m_pRedisCtx->errstr, m_ip.c_str(), m_port);
        m_strErr = buf;
    } else {
        if ((NULL != m_pRedisReply) && (REDIS_REPLY_ERROR == m_pRedisReply->type)) {
            ::snprintf(buf, sizeof (buf), "redis-err: reply err:%d %s ip:%s port:%u",
                    m_pRedisReply->type, m_pRedisReply->str, m_ip.c_str(), m_port);
            m_strErr = buf;
        }
    }
}

redisReply * AppSyncRedisClient::exeRedisCommand(const std::string & cmd) {
    if (!isConnected()) {
        connectRedis();
    }

    if (-1 == _exeRedisCommand(cmd)) {
        return NULL;
    }

    return m_pRedisReply;
}

int AppSyncRedisClient::_exeRedisCommand(const std::string & cmd) {
    if (NULL != m_pRedisReply) {
        ::freeReplyObject(m_pRedisReply);
        m_pRedisReply = NULL;
    }

    if (!isConnected()) {
        collectLastErr();
        return -1;
    }

    void * pReply = NULL;
    ::redisAppendFormattedCommand(m_pRedisCtx, cmd.data(), cmd.length());
    ::redisGetReply(m_pRedisCtx, &pReply);

    m_pRedisReply = static_cast<redisReply *> (pReply);

    collectLastErr();
    return 0;
}

std::string AppSyncRedisClient::getIpByHostName(const char * hostname) {
    struct hostent *host = ::gethostbyname(hostname);
    if (host == NULL) {
        return "";
    }

    in_addr* addr;
    addr = (in_addr *) host->h_addr;

    return ::inet_ntoa(*addr);
}

std::string AppSyncRedisClient::getIpByHostName_r(const char * hostname) {
    if (hostname == NULL) {
        return "";
    }

    struct hostent *host, hostinfo;
    char dns_buff[8192];
    int rc;
    if (0 == ::gethostbyname_r(hostname, &hostinfo, dns_buff, 8192, &host, &rc)) {
        in_addr* addr;
        addr = (in_addr *) hostinfo.h_addr;
        if (addr != NULL) {
            return ::inet_ntoa(*addr);
        } else {
            return "";
        }
    } else {
        return "";
    }
}
