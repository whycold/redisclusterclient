#include "syncredisclusterclient.h"
#include <string>
#include "rediscmd.h"
#include "hiredis/hiredis.h"

using namespace ::std;

string  gen_VIEW__test__test__key(uint64_t key){
    char buf[4096];
    char* p = buf;
    string res;

    *(uint64_t*)p = htobe64(key);
    p += 8;
    res.assign(buf, p - buf);
    return res;
}


class TestSyncRedisClusterClient
{
public:
    TestSyncRedisClusterClient()
    {
        client_ = NULL;
    };
    
    bool Init(const string &ip, int port, int timeout) {
        if (client_ == NULL) {
            client_ = new SyncRedisClusterClient(ip, port, timeout);
        }
        return true;
    }
    
    void TestHelloWorld()
    {

        string cmd;  
        string key = gen_VIEW__test__test__key(random()%16);
        cmd.assign(rediscmd::string_set(key, "hello world"));
        redisReply* reply = client_->RedisCommand(key, cmd);
        if (reply == NULL ||(reply && reply->type == REDIS_REPLY_ERROR)) {
            std::cout<<"TestAsyncRedisClusterClient return err"<<std::endl;
        }
	if (reply && reply->type == REDIS_REPLY_STATUS) {
            std::cout<<"TestAsyncRedisClusterClient status:"<<reply->str<<std::endl;
        }
        return;
    };
    
    
    SyncRedisClusterClient* client_;    
};

int main( int argc, char *argv[] )
{ 
    TestSyncRedisClusterClient sync_client;
    sync_client.Init("127.0.0.1", 6381, 2000);
    sync_client.TestHelloWorld();

}
