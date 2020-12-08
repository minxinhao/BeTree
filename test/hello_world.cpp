#include "../src/betree.hpp"
#include <unistd.h>
#include <string>
#include <sys/types.h>

int main(){
    betree<uint64_t,std::string> b;
    b.insert(0,"hello");
    b.insert(1,"world");
    b.dump_messages();
    assert(b.query(0)=="hello");
    assert(b.query(1)=="world");

    b.erase(0);
    b.erase(1);
    b.dump_messages();


    betree<uint64_t,std::string>::iterator iter = b.begin();

    while(iter != b.end()){
        std::cout<<iter.first<<" "<<iter.second<<std::endl;
    }
}