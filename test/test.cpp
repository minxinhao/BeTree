// This test program performs a series of inserts, deletes, updates,
// and queries to a betree.  It performs the same sequence of
// operatons on a std::map.  It checks that it always gets the same
// result from both data structures.

// The program takes 1 command-line parameter -- the number of
// distinct keys it can use in the test.

#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include "../src/betree.hpp"

void timer_start(uint64_t &timer)
{
    struct timeval t;
    assert(!gettimeofday(&t, NULL));
    timer -= 1000000 * t.tv_sec + t.tv_usec;
}

void timer_stop(uint64_t &timer)
{
    struct timeval t;
    assert(!gettimeofday(&t, NULL));
    timer += 1000000 * t.tv_sec + t.tv_usec;
}

template <class Key, class Value>
void do_scan(typename betree<Key, Value>::iterator &betit,
             typename std::map<Key, Value>::iterator &refit,
             betree<Key, Value> &b,
             typename std::map<Key, Value> &reference)
{
    while (refit != reference.end())
    {
        assert(betit != b.end());
        assert(betit.first == refit->first);
        assert(betit.second == refit->second);
        ++refit;
        if (refit == reference.end())
        {
            debug(std::cout << "Almost done" << std::endl);
        }
        ++betit;
    }
    assert(betit == b.end());
}

#define DEFAULT_TEST_MAX_NODE_SIZE (1ULL << 6)
#define DEFAULT_TEST_MIN_FLUSH_SIZE (DEFAULT_TEST_MAX_NODE_SIZE / 4)
#define DEFAULT_TEST_CACHE_SIZE (4)
#define DEFAULT_TEST_NDISTINCT_KEYS (1ULL << 10)
#define DEFAULT_TEST_NOPS (1ULL << 12)

void usage(char *name)
{
    std::cout
        << "Usage: " << name << " [OPTIONS]" << std::endl
        << "Tests the betree implementation" << std::endl
        << std::endl
        << "Options are" << std::endl
        << "  Required:" << std::endl
        << "    -m  <mode>  (test or benchmark-<mode>)          [ default: none, parameter required ]" << std::endl
        << "        benchmark modes:" << std::endl
        << "          upserts    " << std::endl
        << "          queries    " << std::endl
        << "  Betree tuning parameters:" << std::endl
        << "    -N <max_node_size>            (in elements)     [ default: " << DEFAULT_TEST_MAX_NODE_SIZE << " ]" << std::endl
        << "    -f <min_flush_size>           (in elements)     [ default: " << DEFAULT_TEST_MIN_FLUSH_SIZE << " ]" << std::endl
        << "    -C <max_cache_size>           (in betree nodes) [ default: " << DEFAULT_TEST_CACHE_SIZE << " ]" << std::endl
        << "  Options for both tests and benchmarks" << std::endl
        << "    -k <number_of_distinct_keys>                    [ default: " << DEFAULT_TEST_NDISTINCT_KEYS << " ]" << std::endl
        << "    -t <number_of_operations>                       [ default: " << DEFAULT_TEST_NOPS << " ]" << std::endl
        << "    -s <random_seed>                                [ default: random ]" << std::endl
        << std::endl;
}

int test(betree<uint64_t, std::string> &b,
         uint64_t nops,
         uint64_t number_of_distinct_keys)
{
    std::map<uint64_t, std::string> reference;

    for (unsigned int i = 0; i < nops; i++)
    {
        int op;
        uint64_t t;

        op = rand() % 6;
        t = rand() % number_of_distinct_keys;

        switch (op)
        {
        case 0: // insert
            b.insert(t, std::to_string(t) + ":");
            reference[t] = std::to_string(t) + ":";
            break;
        case 1: // update
            b.update(t, std::to_string(t) + ":");
            if (reference.count(t) > 0)
                reference[t] += std::to_string(t) + ":";
            else
                reference[t] = std::to_string(t) + ":";
            break;
        case 2: // delete
            b.erase(t);
            reference.erase(t);
            break;
        case 3: // query
            try
            {
                std::string bval = b.query(t);
                assert(reference.count(t) > 0);
                std::string rval = reference[t];
                assert(bval == rval);
            }
            catch (std::out_of_range e)
            {
                assert(reference.count(t) == 0);
            }
            break;
        case 4: // full scan
        {
            auto betit = b.begin();
            auto refit = reference.begin();
            do_scan(betit, refit, b, reference);
        }
        break;
        case 5: // lower-bound scan
        {
            auto betit = b.lower_bound(t);
            auto refit = reference.lower_bound(t);
            do_scan(betit, refit, b, reference);
        }
        break;
        default:
            abort();
        }
    }

    std::cout << "Test PASSED" << std::endl;

    return 0;
}

void benchmark_upserts(betree<uint64_t, std::string> &b,
                       uint64_t nops,
                       uint64_t number_of_distinct_keys,
                       uint64_t random_seed)
{
    uint64_t overall_timer = 0;
    for (uint64_t j = 0; j < 100; j++)
    {
        uint64_t timer = 0;
        timer_start(timer);
        for (uint64_t i = 0; i < nops / 100; i++)
        {
            uint64_t t = rand() % number_of_distinct_keys;
            b.update(t, std::to_string(t) + ":");
        }
        timer_stop(timer);
        printf("%ld %ld %ld\n", j, nops / 100, timer);
        overall_timer += timer;
    }
    printf("# overall: %ld %ld\n", 100 * (nops / 100), overall_timer);
}

void benchmark_queries(betree<uint64_t, std::string> &b,
                       uint64_t nops,
                       uint64_t number_of_distinct_keys,
                       uint64_t random_seed)
{

    // Pre-load the tree with data
    srand(random_seed);
    for (uint64_t i = 0; i < nops; i++)
    {
        uint64_t t = rand() % number_of_distinct_keys;
        b.update(t, std::to_string(t) + ":");
    }

    // Now go back and query it
    srand(random_seed);
    uint64_t overall_timer = 0;
    timer_start(overall_timer);
    for (uint64_t i = 0; i < nops; i++)
    {
        uint64_t t = rand() % number_of_distinct_keys;
        b.query(t);
    }
    timer_stop(overall_timer);
    printf("# overall: %ld %ld\n", nops, overall_timer);
}

int main(int argc, char **argv)
{
    char *mode = NULL;
    uint64_t max_node_size = DEFAULT_TEST_MAX_NODE_SIZE;
    uint64_t min_flush_size = DEFAULT_TEST_MIN_FLUSH_SIZE;
    uint64_t cache_size = DEFAULT_TEST_CACHE_SIZE;
    char *backing_store_dir = NULL;
    uint64_t number_of_distinct_keys = DEFAULT_TEST_NDISTINCT_KEYS;
    uint64_t nops = DEFAULT_TEST_NOPS;
    unsigned int random_seed = time(NULL) * getpid();

    int opt;
    char *term;

    //////////////////////
    // Argument parsing //
    //////////////////////

    while ((opt = getopt(argc, argv, "m:N:f:C:k:t:s:")) != -1)
    {
        switch (opt)
        {
        case 'm':
            mode = optarg;
            break;
        case 'N':
            max_node_size = strtoull(optarg, &term, 10);
            if (*term)
            {
                std::cerr << "Argument to -N must be an integer" << std::endl;
                usage(argv[0]);
                exit(1);
            }
            break;
        case 'f':
            min_flush_size = strtoull(optarg, &term, 10);
            if (*term)
            {
                std::cerr << "Argument to -f must be an integer" << std::endl;
                usage(argv[0]);
                exit(1);
            }
            break;
        case 'C':
            cache_size = strtoull(optarg, &term, 10);
            if (*term)
            {
                std::cerr << "Argument to -C must be an integer" << std::endl;
                usage(argv[0]);
                exit(1);
            }
            break;
        case 'k':
            number_of_distinct_keys = strtoull(optarg, &term, 10);
            if (*term)
            {
                std::cerr << "Argument to -k must be an integer" << std::endl;
                usage(argv[0]);
                exit(1);
            }
            break;
        case 't':
            nops = strtoull(optarg, &term, 10);
            if (*term)
            {
                std::cerr << "Argument to -t must be an integer" << std::endl;
                usage(argv[0]);
                exit(1);
            }
            break;
        case 's':
            random_seed = strtoull(optarg, &term, 10);
            if (*term)
            {
                std::cerr << "Argument to -s must be an integer" << std::endl;
                usage(argv[0]);
                exit(1);
            }
            break;
        default:
            std::cerr << "Unknown option '" << (char)opt << "'" << std::endl;
            usage(argv[0]);
            exit(1);
        }
    }

    if (mode == NULL ||
        (strcmp(mode, "test") != 0 && strcmp(mode, "benchmark-upserts") != 0 && strcmp(mode, "benchmark-queries") != 0))
    {
        std::cerr << "Must specify a mode of \"test\" or \"benchmark\"" << std::endl;
        usage(argv[0]);
        exit(1);
    }

    if (strncmp(mode, "benchmark", strlen("benchmark")) == 0)
    {
        std::cerr << "Cannot specify an input script in benchmark mode" << std::endl;
        usage(argv[0]);
        exit(1);
    
    }

    srand(random_seed);

    if (backing_store_dir == NULL)
    {
        std::cerr << "-d <backing_store_directory> is required" << std::endl;
        usage(argv[0]);
        exit(1);
    }

    ////////////////////////////////////////////////////////
    // Construct a betree and run the tests or benchmarks //
    ////////////////////////////////////////////////////////

  
    betree<uint64_t, std::string> b(max_node_size, max_node_size/4,min_flush_size);

    if (strcmp(mode, "test") == 0)
        test(b, nops, number_of_distinct_keys);
    else if (strcmp(mode, "benchmark-upserts") == 0)
        benchmark_upserts(b, nops, number_of_distinct_keys, random_seed);
    else if (strcmp(mode, "benchmark-queries") == 0)
        benchmark_queries(b, nops, number_of_distinct_keys, random_seed);
    return 0;
}
