#include "../src/betree.hpp"
#include <unistd.h>
#include <string>
#include <sys/types.h>

int main(){
    betree<uint64_t,std::string> b;
    std::string src_str("hello world");
    for(int site = 0;site!=src_str.length();site++){
        b.insert(site,std::string(1,src_str[site]));
    }
    // b.dump_messages();
    for(int site = 0;site<src_str.length();site++){
        assert(b.query(site)==std::string(1,src_str[site])) ; 
    }
    for(int site = 0;site<src_str.length();site+=2){
        b.erase(site);
    }
    
    // b.dump_messages();


    betree<uint64_t,std::string>::iterator iter = b.begin();

    while(iter != b.end()){
        std::cout<<iter.first<<" "<<iter.second<<std::endl;
        ++iter;
    }


    betree<uint64_t,std::string>::iterator iter_2 = b.lower_bound(0);

    while(iter_2 != b.end()){
        std::cout<<iter_2.first<<" "<<iter_2.second<<std::endl;
        ++iter_2;
    }
}