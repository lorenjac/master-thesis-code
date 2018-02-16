#ifndef LIST_HPP
#define LIST_HPP

#include <stdexcept> // std::out_of_range
#include <cstddef>   // std::size_t
#include <utility>   // std::swap

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/p.hpp>

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

template <class T>
class list
{
// ############################################################################
// TYPES
// ############################################################################

public:
    using elem_type = T;
    using size_type = std::size_t;
    using this_type = list<elem_type>;

    class iterator;

private:
    /**
     * A list node for a doubly-linked list.
     * Contains a value and pointers for a
     * predecessor and a successor node.
     */
    struct node
    {
        /**
         * Creates a list node.
          */
        node()
            : mNext{}
            , mPrev{}
            , mValue{}
        {
        }

        pmdk::persistent_ptr<node> mNext;
        pmdk::persistent_ptr<node> mPrev;

        // In order to make value persistent, elem_type should be of type
        // p<Y> or persistent_ptr<Y>
        //
        // This is the only way to make both persistent properties and
        // persistent pointers usable in a convenient manner.
        elem_type mValue;
    };

// ############################################################################
// MEMBER VARIABLES
// ############################################################################

private:
    pmdk::persistent_ptr<node> mHead;
    pmdk::persistent_ptr<node> mTail;
    pmdk::p<size_type> mSize;

// ############################################################################
// PUBLIC API
// ############################################################################

public:
    list()
        : mHead{}
        , mTail{}
        , mSize{}
    {}

    list(const this_type& other) = delete;

    /**
     * Construct a list from an rvalue reference
     */
    list(this_type&& other)
        : mHead{other.mHead}
        , mTail{other.mTail}
        , mSize{other.mSize}
    {
        other.mHead = nullptr;
        other.mTail = nullptr;
        other.mSize.get_rw() = 0;
    }

    /**
     * Destroys this list and all its nodes.
     */
    ~list()
    {
        // Removes all nodes from this list
        // Requires no transaction because dtors are always
        // executed transactionally with delete_persistent()
	    clear();
    }

    this_type& operator=(const this_type& other) = delete;

    /**
     * Move assignment.
     *
     * Use only INSIDE active transactions.
     */
    this_type& operator=(this_type&& other)
    {
        std::swap(mHead, other.mHead);
        std::swap(mTail, other.mTail);
        std::swap(mSize, other.mSize);
        return *this;
    }

    /**
     * Initialize this list. This function MUST be called before any
     * other member function IF this instance was created as a root
     * object of an object pool.
     */
    template <class pool_type>
    void init(pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [this](){
            mHead = nullptr;
            mTail = nullptr;
            mSize.get_rw() = 0;
        });
    }

    /**
     * Add an element at the back of this list.
     */
    template <class pool_type>
    void push_back(const elem_type& elem, pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            auto new_node = pmdk::make_persistent<node>();
            new_node->mValue = elem;
            push_back(new_node);
        });
    }

    /**
     * Add an element at the back of this list.
     */
    template <class pool_type>
    void push_front(const elem_type& elem, pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            auto new_node = pmdk::make_persistent<node>();
            new_node->mValue = elem;
            push_front(new_node);
        });
    }

    /**
     * Inserts an element at the specified position.
     *
     * If there is an element at the given position, the new element will take
     * its position and the former element will be pushed to the right.
     *
     * Pos may be equal to the size of the list. In this case, the item is
     * added to the back of the list.
     */
    template <class pool_type>
    void insert(size_type pos, const elem_type& elem, pmdk::pool<pool_type>& pool)
    {
        // Fail if index is invalid
        if (pos > size())
            throw std::out_of_range("index is out of range!");

        // Create and move iterator to specified position
        auto it = begin();
        for (size_type i=0; i<pos; ++i)
            ++it;

        insert(it, elem, pool);
    }

    /**
     * Inserts an element at the specified position.
     *
     * If there is an element at the given position, the new element will take
     * its position and the former element will be pushed to the right.
     *
     * Pos may be the end iterator. In this case, the item is added to the back
     * of the list.
     */
    template <class pool_type>
    void insert(iterator pos, const elem_type& elem, pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            auto new_node = pmdk::make_persistent<node>();
            new_node->mValue = elem;
            insert(pos, new_node);
        });
    }

    /**
     * Steal an element from another list and add it to the back of this list.
     *
     * Does not involve any allocations or deallocations. The indexed node
     * is simply unlinked from the other list and appended to this list.
     *
     * Fails, if the specified index is invalid in the other list.
     */
    template <class pool_type>
    void push_back_from(this_type& other, const size_type pos,
            pmdk::pool<pool_type>& pool)
    {
        // Fail if index is invalid
        if (pos >= other.size())
            throw std::out_of_range("index is out of range!");

        // Create and move iterator to specified position
        auto it = other.begin();
        for (size_type i = 0; i < pos; ++i)
            ++it;

        // Unlink specified node from other list and append it to this list
        pmdk::transaction::exec_tx(pool, [&,this](){
            auto unlinked_node = other.remove(it);
            push_back(unlinked_node);
        });
    }

    /**
     * Steal an element from another list and add it to the front of this list.
     *
     * Does not involve any allocations or deallocations. The indexed node
     * is simply unlinked from the other list and appended to this list.
     *
     * Fails, if the specified index is invalid in the other list.
     */
    template <class pool_type>
    void push_front_from(this_type& other, const size_type pos,
            pmdk::pool<pool_type>& pool)
    {
        // Fail if index is invalid
        if (pos >= other.size())
            throw std::out_of_range("index is out of range!");

        // Create and move iterator to specified position
        auto it = other.begin();
        for (size_type i = 0; i < pos; ++i)
            ++it;

        // Unlink specified node from other list and append it to this list
        pmdk::transaction::exec_tx(pool, [&,this](){
            auto unlinked_node = other.remove(it);
            push_front(unlinked_node);
        });
    }

    /**
     * Returns element at the i-th position in the list.
     * Throws std::out_of_range if pos is invalid.
     */
    elem_type& get(const size_type pos)
    {
        // Fail if index is invalid
        if (pos >= size())
            throw std::out_of_range("index is out of range!");

        // Go to i-th position (index is guaranteed to be valid)
        auto curr = mHead;
        for (size_type i = 0; i < pos; ++i)
            curr = curr->mNext;

        return curr->mValue;
    }

    /**
     * Returns element at the i-th position in the list.
     * Throws std::out_of_range if pos is invalid.
     */
    const elem_type& get(const size_type pos) const
    {
        // Fail if index is invalid
        if (pos >= size())
            throw std::out_of_range("index is out of range!");

        // Go to i-th position (index is guaranteed to be valid)
        auto curr = mHead;
        for (size_type i = 0; i < pos; ++i)
            curr = curr->mNext;

        return curr->mValue;
    }

    /**
     * Remove an element at the i-th position in the list.
     * Throws std::out_of_range if pos is invalid.
     */
    template <class pool_type>
    void erase(const size_type pos, pmdk::pool<pool_type>& pool)
    {
        // Fail if index is invalid
        if (pos >= size())
            throw std::out_of_range("index is out of range!");

        // Create and move iterator to specified position
        auto it = begin();
        for (size_type i=0; i<pos; ++i)
            ++it;

        // Remove element via iterator
        erase(it, pool);
    }

    /**
     * Remove an element at the given position in the list.
     * Throws std::out_of_range if pos is invalid.
     */
    template <class pool_type>
    iterator erase(iterator pos, pmdk::pool<pool_type>& pool)
    {
        // Fail if index is invalid
        if (pos == end())
            throw std::out_of_range("iterator is out of range!");

        pmdk::transaction::exec_tx(pool, [&,this](){
            auto node_to_remove = remove(pos++);
            pmdk::delete_persistent<node>(node_to_remove);
        });
        return pos; // return iterator to next element or end
    }

    /**
     * Removes all elements in this list.
     */
    template <class pool_type>
    void clear(pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            // same is achieved through move assignment: *this = this_type{};
            clear(); // Remove all nodes
            init(pool); // Reset all member variables
        });
    }

    /**
     * Returns number of elements in the list.
     */
    size_t size() const { return mSize.get_ro(); }

    /**
     * Returns true if the list has no elements, false otherwise.
     */
    bool empty() const { return mSize.get_ro() == 0; }

// ############################################################################
// ITERATORS
// ############################################################################

    /**
     * Non-const iterator
     */
    class iterator
    {
        friend this_type;

    private:
        node* curr;

    public:
        explicit iterator(node* curr = nullptr)
            : curr(curr)
        {}

        elem_type& operator*() { return curr->mValue; }
        elem_type* operator->() { return &curr->mValue; }

        bool operator==(const iterator& other) { return curr == other.curr; }
        bool operator!=(const iterator& other) { return curr != other.curr; }

        iterator operator++(int) {
            auto old = *this;
            curr = curr->mNext.get();
            return old;
        }

        iterator operator++() {
            curr = curr->mNext.get();
            return *this;
        }
    };

    iterator begin() { return iterator{mHead.get()}; }
    iterator end() { return iterator{}; }

// ############################################################################
// PRIVATE API
// ############################################################################

private:
    /**
     * Append an existing node to the back of the list.
     *
     * This function is agnostic to whether the given
     * node was newly created or stolen from another
     * list. Performs no allocations.
     *
     * Also increases the item count.
     */
    void push_back(pmdk::persistent_ptr<node> node)
    {
        if (mHead) {
            mTail->mNext = node;
            mTail->mNext->mPrev = mTail;
            mTail = mTail->mNext;
        }
        else {
            mHead = node;
            mTail = mHead;
        }
        ++mSize.get_rw();
    }

    void push_front(pmdk::persistent_ptr<node> node)
    {
        if (mHead) {
            node->mNext = mHead; // head becomes successor of new node
            mHead->mPrev = node; // new node becomes predecessor of head
        }
        else {
            mTail = node;
        }
        mHead = node;
        ++mSize.get_rw();
    }

    void insert(iterator pos, pmdk::persistent_ptr<node> node)
    {
        if (pos.curr == nullptr) {
            // Insert node BEFORE nullptr [= end()], that is, after tail. There
            // is no special case for pos.curr = tail because inserting before
            // the tail does not result in a new tail (as opposed to inserting
            // at the front).
            push_back(node);
        }
        else if (pos.curr == mHead.get()) {
            // Insert node BEFORE frontmost element (= head)
            push_front(node);
        }
        else {
            // Since pos != front() and pos != end() we know that head != tail
            // and node will be head < node < tail. Therefore, we know that pos
            // must have a predecessor and a successor, so we can proceed with
            // ease.

            auto pred = pos.curr->mPrev;
            auto curr = pred->mNext;

            // Make new node aware of its neighbors
            node->mPrev = pred;
            node->mNext = curr;

            // Make surrounding nodes aware of new node
            pred->mNext = node;
            curr->mPrev = node;
            ++mSize.get_rw();
        }
    }

    pmdk::persistent_ptr<node> remove(iterator pos)
    {
        // Stores a ptr to a node that will be deleted once it is unlinked
        pmdk::persistent_ptr<node> unlinked;

        // Decide whether the node in question is front, back, or in between
        if (pos.curr == mHead.get()) {
            auto tmp = mHead;

            // Unset backward link of successor (if such a node exists)
            if (tmp->mNext)
                tmp->mNext->mPrev = nullptr;

            // New head points to successor or is a nullptr
            mHead = tmp->mNext;

            // Invalidate tail if list has become empty (head=null)
            // Not strictly required (mTail is never read until it is
            // overwritten when re-populated in push_back().
            if (!mHead)
                mTail = nullptr;

            // Mark former head for destruction
            unlinked = tmp;
        }
        else if (pos.curr == mTail.get()) {
            auto tmp = mTail;

            // Unset forward link of predecessor.
            // No need to check if prev exists because otherwise
            // the list would have size=1 which means that an
            // index would either be 0 (see above if branch)
            // or out of bounds (see top of function).
            mTail->mPrev->mNext = nullptr;

            // New tail points to predecessor (must exist)
            mTail = mTail->mPrev;

            // Mark former tail for destruction
            unlinked = tmp;
        }
        else {
            // Retrieve persistent_ptr to current iterator position
            auto curr = pos.curr->mPrev->mNext;

            // Retrieve predecessor and successor of the current node.
            // These must exist because we handled or corner cases before.
            auto prev = curr->mPrev;
            auto next = curr->mNext;

            // Unset all links to curr by bypassing the current node
            prev->mNext = next;
            next->mPrev = prev;

            // Mark current node for destruction
            unlinked = curr;
        }
        --mSize.get_rw();
        unlinked->mNext = nullptr;
        unlinked->mPrev = nullptr;
        return unlinked;
    }

    void clear()
    {
        // Starting with first node, remove all nodes
        auto curr = mHead;
        while (curr) {
            auto node_to_remove = curr;
            curr = curr->mNext;
            pmdk::delete_persistent<node>(node_to_remove);
        }
    }

}; // end class list

} // end namespace detail
} // end namespace midas

#endif
