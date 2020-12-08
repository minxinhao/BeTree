// A in-memory edition of B^e-tree Implementation.Transplanted 
// oscarlab's BeTree implementation and the backing_store and 
// swap_space designs were removed.

// Keys must be comparable (via operator< and operator==).
// No other restrictions such as seriablizable and addable in key/value 
// class as long as they are compatible with std::map.
// See hello_world.cpp for example usage.

// Remove MessageKey type as I think that timestamp field has no sense 
// actually and the support for concurrency is not currently considered.

#include <map>
#include <vector>
#include <cassert>
#include <cstdint>
#include <unordered_map>
#include <set>
#include <sstream>
#include <functional>
#include <cstddef>
#include <iostream>
#include <memory>
#include "debug.hpp"

// The three types of upsert.  An UPDATE specifies a value, v, that
// will be added (using operator+) to the old value associated to some
// key in the tree.  If there is no old value associated with the key,
// then it will add v to the result of a Value obtained using the
// default zero-argument constructor.
#define INSERT (0)
#define DELETE (1)
#define UPDATE (2)

template<class Value>
class Message {
public:
  Message(void) :
    opcode(INSERT),
    val()
  {}

  Message(int opc, const Value &v) :
    opcode(opc),
    val(v)
  {}

  int opcode;
  Value val;
};

template <class Value>
bool operator==(const Message<Value> &a, const Message<Value> &b) {
  return a.opcode == b.opcode && a.val == b.val;
}

// Measured in messages.
#define DEFAULT_MAX_NODE_SIZE (1ULL<<18)

// TODO:Edit logic in MIN_FLUSH_SIZE
// The minimum number of messages that we will flush to an out-of-cache node.
// Note: we will flush even a single element to a child that is already dirty.
// Note: we will flush MIN_FLUSH_SIZE/2 items to a clean in-memory child.
#define DEFAULT_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 16ULL)

template<class Key, class Value> class betree {
private:
    class node;
    typedef typename std::shared_ptr<node> node_pointer;

    uint64_t min_flush_size;
    uint64_t max_node_size;
    uint64_t min_node_size;
    node_pointer root;
    Value default_value;

    class child_info {
    public:
    child_info(void)
      : child(),
	    child_size(0)
    {}
    
    child_info(node_pointer& child, uint64_t child_size)
      : child(child.get()),
	child_size(child_size)
    {}

    const child_info& operator =(const child_info& b){
        child.reset(b.child.get());
        child_size = b.child_size;
        return *this;
    }

    child_info(const child_info& b){
        child.reset(b.child.get());
        child_size = b.child_size;
    }

    node_pointer child;
    uint64_t child_size;
  };

    typedef typename std::map<Key, child_info> pivot_map;
    typedef typename std::map<Key, Message<Value> > message_map;
    
    class node {
    public:
        pivot_map pivots;
        message_map elements;

        bool is_leaf(void) const{
            return pivots.empty();
        }

        // Return OUT iterator of mp which points 
        // to the subtree contains this key.
        template<class OUT, class IN>
        static OUT get_pivot(IN & mp, const Key & k) {
            assert(mp.size() > 0);
            auto it = mp.lower_bound(k);
            if (it == mp.begin() && k < it->first)
                throw std::out_of_range("Key does not exist "
                        "(it is smaller than any key in DB)");
            if (it == mp.end() || k < it->first)
            // Why not just return it if k < it->first
            // In my comprehension,return it-- when k < it->first implies 
            // that every pivot owns the smallest key in the sub-tree 
            // pointed by pointer in childinfo.
                --it;
                return it;      
        }

        // Instantiate the above template for const and non-const
        // calls. (template inference doesn't seem to work on this code)
        typename pivot_map::const_iterator get_pivot(const Key & k) const {
        return get_pivot<typename pivot_map::const_iterator,
                const pivot_map>(pivots, k);
        }
        typename pivot_map::iterator
        get_pivot(const Key & k) {
            return get_pivot<typename pivot_map::iterator, pivot_map>(pivots, k);
        }

        // Return iterator pointing to the first element with mk >= k.
        // (Same const/non-const templating trick as above)
        template<class OUT, class IN>
        static OUT get_element_begin(IN & elts, const Key &k) {
        return elts.lower_bound(k);
        }

        typename message_map::iterator get_element_begin(const Key &k) {
            return get_element_begin<typename message_map::iterator,
                    message_map>(elements, k);
        }

        typename message_map::const_iterator get_element_begin(const Key &k) const {
            return get_element_begin<typename message_map::const_iterator,
            const message_map>(elements, k);
        }

        // Return iterator pointing to the first element that goes to
        // child indicated by it
        typename message_map::iterator
        get_element_begin(const typename pivot_map::iterator it) {
            return it == pivots.end() ? elements.end() : get_element_begin(it->first);
        }

        // Apply a message to ourself.
        // I have no idea in addable value, so i remove
        // paramater default_value and value addition in applying 
        // updates.
        void apply(const Key &mkey, const Message<Value> &elt) {
            switch (elt.opcode) {
            case INSERT:
                //There is no timestamp anymore , so there is no need 
                // to erase (elements.lower_bound(mkey.range_start()),
                // elements.upper_bound(mkey.range_end()))
                elements[mkey] = elt;
                break;

            case DELETE:
                if (!is_leaf())
                    elements[mkey] = elt;
                else
                    elements.erase(mkey);
                break;

            case UPDATE:
            {
                auto iter = elements.upper_bound(mkey);
                if (iter != elements.begin()){
                // max element with key <= mkey 
                    iter--;
                }
                if (iter == elements.end() || iter->first != mkey){
                    // No key equals to mkey.key in this node
                    if (is_leaf()) {
                        apply(mkey, Message<Value>(INSERT, elt.val));
                    } else {
                        elements[mkey] = elt;
                    }
                }
                else {
                    assert(iter != elements.end() && iter->first == mkey); // There's Message with m.key in elements
                    if (iter->second.opcode == INSERT) {
                        apply(mkey, Message<Value>(INSERT, elt.val));	  
                    } else {
                        elements[mkey] = elt;	      
                    }
                }
            }
            break;

            default:
                assert(0);
            }
        }

        // Requires: there are less than MIN_FLUSH_SIZE things in elements
        //           destined for each child in pivots);
        pivot_map split(betree &bet) {
            assert(pivots.size() + elements.size() >= bet.max_node_size);
        // This size split does a good job of causing the resulting
        // nodes to have size between 0.4 * MAX_NODE_SIZE and 0.6 * MAX_NODE_SIZE.
            int num_new_leaves =
                            (pivots.size() + elements.size())  / (10 * bet.max_node_size / 24);
            int things_per_new_leaf =
                            (pivots.size() + elements.size() + num_new_leaves - 1) / num_new_leaves; // Rounded up by adding num_new_leaves-1

            pivot_map result;
            auto pivot_idx = pivots.begin();
            auto elt_idx = elements.begin();
            int things_moved = 0;
            for (int i = 0; i < num_new_leaves; i++) {
                if (pivot_idx == pivots.end() && elt_idx == elements.end())
                    break;
                node_pointer new_node(new node);
                result[pivot_idx != pivots.end() ? pivot_idx->first : elt_idx->first] = child_info(new_node,
                                new_node->elements.size() + new_node->pivots.size());
                while(things_moved < (i+1) * things_per_new_leaf &&
                    (pivot_idx != pivots.end() || elt_idx != elements.end())) {
                    if (pivot_idx != pivots.end()) {
                        new_node->pivots[pivot_idx->first] = pivot_idx->second;
                        ++pivot_idx;                                // (*)
                        things_moved++;
                        auto elt_end = get_element_begin(pivot_idx);//Variable pivot_idx has beened added  one at (*)
                                                                    //If pivot_idx==pivots.end(),get_element_begin will return elements.end(),so all elements in inter-node will never be splitted into one new node without old pivot
                        while (elt_idx != elt_end) {                //(**)
                            new_node->elements[elt_idx->first] = elt_idx->second;
                            ++elt_idx;
                            things_moved++;
                        }
                    } else {
                        // Must be a leaf
                        // It holds becuase while in (**)
                        assert(pivots.size() == 0); 
                        new_node->elements[elt_idx->first] = elt_idx->second;
                        ++elt_idx;
                        things_moved++;	    
                    }
                }
            }
            
            for (auto it = result.begin(); it != result.end(); ++it)
                it->second.child_size = it->second.child->elements.size() + it->second.child->pivots.size();
            
            assert(pivot_idx == pivots.end());
            assert(elt_idx == elements.end());
            pivots.clear();
            elements.clear();
            return result;
        }

        node_pointer merge(betree &bet,
		       typename pivot_map::iterator begin,
		       typename pivot_map::iterator end) {
            node_pointer new_node(new node);
            for (auto it = begin; it != end; ++it) {
                new_node->elements.insert(it->second.child->elements.begin(),
                        it->second.child->elements.end());
                new_node->pivots.insert(it->second.child->pivots.begin(),
                        it->second.child->pivots.end());
            }
            return new_node;
        }

        void merge_small_children(betree &bet) {
            if (is_leaf())
                return;

            for (auto beginit = pivots.begin(); beginit != pivots.end(); ++beginit) {
                uint64_t total_size = 0;
                auto endit = beginit;
                while (endit != pivots.end()) {
                    if (total_size + beginit->second.child_size > 6 * bet.max_node_size / 10)
                        break;
                    total_size += beginit->second.child_size;
                    ++endit;
                }
                if (endit != beginit) {
                    node_pointer merged_node = merge(bet, beginit, endit);
                    for (auto tmp = beginit; tmp != endit; ++tmp) {
                        tmp->second.child->elements.clear();
                        tmp->second.child->pivots.clear();
                    }
                    Key key = beginit->first;
                    pivots.erase(beginit, endit);
                    pivots[key] = child_info(merged_node, merged_node->pivots.size() + merged_node->elements.size());
                    beginit = pivots.lower_bound(key);
                }
            }
        }

        void flush_max_message_set(betree &bet,
            typename pivot_map::iterator& first_pivot_idx){
            while (elements.size() + pivots.size() >= bet.max_node_size) {
                // Find the child with the largest set of messages in our buffer
                unsigned int max_size = 0;
                auto child_pivot = pivots.begin();
                auto next_pivot = pivots.begin();
                for (auto it = pivots.begin(); it != pivots.end(); ++it) {
                    auto it2 = next(it);
                    auto elt_it = get_element_begin(it); 
                    auto elt_it2 = get_element_begin(it2); 
                    unsigned int dist = distance(elt_it, elt_it2);
                    if (dist > max_size) {
                        child_pivot = it;
                        next_pivot = it2;
                        max_size = dist;
                    }
                }
                if (max_size <= bet.min_flush_size)
                    break; // Requires for splits hold.
                auto elt_child_it = get_element_begin(child_pivot);
                auto elt_next_it = get_element_begin(next_pivot);
                message_map child_elts(elt_child_it, elt_next_it);
                pivot_map new_children = child_pivot->second.child->flush(bet, child_elts);
                elements.erase(elt_child_it, elt_next_it);
                if (!new_children.empty()) {
                    pivots.erase(child_pivot);
                    pivots.insert(new_children.begin(), new_children.end());
                } else {
                    first_pivot_idx->second.child_size =
                    child_pivot->second.child->pivots.size() +
                    child_pivot->second.child->elements.size();
                } 
            }   
        }

        pivot_map flush(betree &bet, message_map &elts){  
            debug(std::cout << "Flushing " << this << std::endl);
            pivot_map result;

            if (elts.size() == 0) {
                debug(std::cout << "Done (empty input)" << std::endl);
                return result;
            }

            if (is_leaf()) {
                for (auto it = elts.begin(); it != elts.end(); ++it)
                    apply(it->first, it->second);
                if (elements.size() + pivots.size() >= bet.max_node_size)
                    result = split(bet);
                return result;
            }	

            ////////////// Non-leaf
            
            // Update the key of the first child, if necessary
            Key oldmin = pivots.begin()->first;
            Key newmin = elts.begin()->first;
            if (newmin < oldmin) {
                pivots[newmin] = pivots[oldmin];
                pivots.erase(oldmin);
            }

            // I remove logic for that If everything is going to a single dirty child, go ahead
            // and put it there.

            for (auto it = elts.begin(); it != elts.end(); ++it)
                apply(it->first, it->second);

            // Now flush children as necessary
            auto first_pivot_idx = get_pivot(elts.begin()->first);
            flush_max_message_set(bet,first_pivot_idx);
            

            // We have too many pivots to efficiently flush stuff down, so split
            if (elements.size() + pivots.size() > bet.max_node_size) {
                result = split(bet);
            }

            //merge_small_children(bet);
            
            debug(std::cout << "Done flushing " << this << std::endl);
            return result;
        }

        Value query(const betree & bet, const Key k) const{
            debug(std::cout << "Querying " << this << std::endl);
            if (is_leaf()) {
                auto it = elements.lower_bound(k);
                if (it != elements.end() && it->first == k) {
                    assert(it->second.opcode == INSERT);
                    return it->second.val;
                } else {
                    throw std::out_of_range("Key does not exist");
                }
            }

            ///////////// Non-leaf
            
            auto message_iter = get_element_begin(k);
            Value v = bet.default_value;

            if (message_iter == elements.end() || k < message_iter->first)
                // If we don't have any messages for this key, just search
                // further down the tree.
                v = get_pivot(k)->second.child->query(bet, k);
            else if (message_iter->second.opcode == UPDATE) {
                // Notes::I remove logic of original UPDATA processing as 
                // I think it's useless or at least uncomprehensive.
                v = message_iter->second.val;
            } else if (message_iter->second.opcode == DELETE) {
                // We have a delete message, so we don't need to look further
                // down the tree.  
                throw std::out_of_range("Key does not exist");
            } else if (message_iter->second.opcode == INSERT) {
                v = message_iter->second.val;
            }

            return v;
        }

        std::pair<Key, Message<Value> >
        get_next_message_from_children(const Key *mkey) const {
            auto it = (mkey && pivots.begin()->first < *mkey)
            ? get_pivot(*mkey): pivots.begin();
            while (it != pivots.end()) {
                try {
                    return it->second.child->get_next_message(mkey);
                } catch (std::out_of_range e) {}
                ++it;
            }
            throw std::out_of_range("No more messages in any children");
        }

        std::pair<Key, Message<Value> >
        get_next_message(const Key *mkey) const {
            auto it = mkey ? elements.upper_bound(*mkey) : elements.begin();

            if (is_leaf()) {
                if (it == elements.end())
                    throw std::out_of_range("No more messages in sub-tree");
                return std::make_pair(it->first, it->second);
            }

            if (it == elements.end())
                return get_next_message_from_children(mkey);
        
            try {
                auto kids = get_next_message_from_children(mkey);
                if (kids.first < it->first)
                    return kids;
                else 
                    return std::make_pair(it->first, it->second);
            }catch (std::out_of_range e) {
                return std::make_pair(it->first, it->second);	
            }
        }
    };

public:
    betree(uint64_t maxnodesize = DEFAULT_MAX_NODE_SIZE,
	    uint64_t minnodesize = DEFAULT_MAX_NODE_SIZE / 4,
	    uint64_t minflushsize = DEFAULT_MIN_FLUSH_SIZE) :
    min_flush_size(minflushsize),
    max_node_size(maxnodesize),
    min_node_size(minnodesize)
  {
    root.reset(new node);
  }

    ~betree(){
    }

    // Insert the specified message and handle a split of the root if it
    // occurs.
    void upsert(int opcode, Key k, Value v){
        message_map tmp;
        tmp[k] = Message<Value>(opcode, v);
        pivot_map new_nodes = root->flush(*this, tmp);
        if (new_nodes.size() > 0) {
            root.reset(new node);
            root->pivots = new_nodes;
        }
    }

    void insert(Key k, Value v){
        upsert(INSERT, k, v);
    }

    void update(Key k, Value v){
        upsert(UPDATE, k, v);
    }

    void erase(Key k){
        upsert(DELETE, k, default_value);
    }
    
    Value query(Key k){
        Value v = root->query(*this, k);
        return v;
    }

    void dump_messages(void) {
        std::pair<Key, Message<Value> > current;
        std::cout << "############### BEGIN DUMP ##############" << std::endl;
        
        try {
            current = root->get_next_message(NULL);
            do { 
                std::cout << current.first     << " "
                    << current.second.opcode   << " "
                    << current.second.val      << std::endl;
                current = root->get_next_message(&current.first);
            } while (1);
        } catch (std::out_of_range e) {}
    }

    class iterator {
        const betree &bet;
        std::pair<Key, Message<Value> > position;
        bool is_valid;
        bool pos_is_valid;
    public:
        Key first;
        Value second;

        iterator(const betree &bet)
        : bet(bet),
            position(),
            is_valid(false),
            pos_is_valid(false),
            first(),
            second()
        {}

        iterator(const betree &bet, const Key *mkey)
        : bet(bet),
            position(),	
            is_valid(false),
            pos_is_valid(false),
            first(),
            second()
        {
            try {
                position = bet.root->get_next_message(mkey);
                pos_is_valid = true;
                setup_next_element();
            } catch (std::out_of_range e) {}
        }

        void apply(const Key &msgkey, const Message<Value> &msg) {
            switch (msg.opcode) {
                case INSERT:
                first = msgkey;
                second = msg.val;
                is_valid = true;
                break;
                case UPDATE:
                first = msgkey;
                if (is_valid == false)
                    second = bet.default_value;
                second = second + msg.val;
                is_valid = true;
                break;
                case DELETE:
                is_valid = false;
                break;
                default:
                abort();
                break;
            }
        }

        void setup_next_element(void) {
            is_valid = false;
            while (pos_is_valid && (!is_valid || position.first == first)) {
                apply(position.first, position.second);
                try {
                    position = bet.root->get_next_message(&position.first);
                } catch (std::exception e) {
                    pos_is_valid = false;
                }
            }
        }

        bool operator==(const iterator &other) {
            return &bet == &other.bet &&
            is_valid == other.is_valid &&
            pos_is_valid == other.pos_is_valid &&
            (!pos_is_valid || position == other.position) &&
            (!is_valid || (first == other.first && second == other.second));
        }

        bool operator!=(const iterator &other) {
            return !operator==(other);
        }

        iterator &operator++(void) {
            setup_next_element();
            return *this;
        }
    };

    iterator begin(void) const {
        return iterator(*this, NULL);
    }

    iterator lower_bound(Key key) const {
        return iterator(*this, key);
    }

    
    iterator end(void) const {
        return iterator(*this);
    }
};