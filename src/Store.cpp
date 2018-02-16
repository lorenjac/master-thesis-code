#include "Store.hpp"

#include <experimental/filesystem>  // std::exists
#include <memory> // std::make_shared

namespace midas {
namespace detail {

// ############################################################################
// PUBLIC API
// ############################################################################

Store::Store(pool_type& pop)
    : pop{pop}
    , index{}
    , tx_tab{}
    , timestampCounter{TS_START}
    , idCounter{ID_START}
{
    init();
}

transaction::ptr Store::begin()
{
    // Create new transaction with current timestamp
    auto tx = std::make_shared<transaction>(
        idCounter.fetch_add(TS_DELTA),
        timestampCounter.fetch_add(TS_DELTA)
    );

    if (!tx)
        return nullptr;

    // Add new transaction to list of running transactions
    tx_tab.insert(tx->getId(), tx);

    // std::cout << "Store::begin(): spawned new transaction {";
    // std::cout << "id=" << tx->getId() << ", begin=" << tx->getBegin() << "}\n";
    // std::cout << "Store::begin(): number of transactions is " << tx_tab.size() << '\n';

    return tx;
}

int Store::abort(transaction::ptr tx, int reason)
{
    // std::cout << "Store::abort(tx{id=" << tx->getId() << "}";
    // std::cout << ", reason=" << reason << "):" << '\n';

    // Reject invalid or inactive transactions.
    if (!isValidTransaction(tx))
        return INVALID_TX;

    // Mark this transaction as aborted/failed.
    // This must be done atomically because operations of concurrent
    // transactions might be querying the state of tx (if they
    // found its id in a version they want to read or write).
    tx->getStatus().store(transaction::FAILED);

    // Undo all changes carried out by tx
    rollback(tx);

    tx_tab.erase(tx->getId());

    // return the specified error code (supplied by the caller)
    return reason;
}

int Store::commit(transaction::ptr tx)
{
    // std::cout << "Store::commit(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!isValidTransaction(tx))
        return INVALID_TX;

    // Set tx end timestamp
    tx->setEnd(timestampCounter.fetch_add(TS_DELTA));

    // Note: This is where validation would take place (not required for snapshot isolation)
    // if (!validate(tx))
    //     return abort(tx, 0xDEADBEEF);

    if (!persist(tx))
        return abort(tx, WRITE_CONFLICT);

    // Mark tx as committed.
    // This must be done atomically because operations of concurrent
    // transactions might be querying the state of tx (e.g. if they
    // found its id in a version they want to read or write).
    tx->getStatus().store(transaction::COMMITTED);

    // Propagate end timestamp of tx to end/begin fields of original/new versions
    finalize(tx);

    // Now that all its modifications have become persistent, we can safely
    // remove this transaction from our list
    tx_tab.erase(tx->getId());

    return OK;
}

int Store::read(transaction::ptr tx, const key_type& key, mapped_type& result)
{
    // std::cout << "Store::read(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!isValidTransaction(tx))
        return INVALID_TX;

    // Look up data item. Abort if key does not exist.
    index_mutex.lock();
    history::ptr history;
    auto status = index->get(key, history);
    index_mutex.unlock();
    if (!status) {
        return abort(tx, VALUE_NOT_FOUND);
    }

    // Scan history for latest committed version which is older than tx.
    history->mutex.lock();
    auto candidate = getReadableSnapshot(history, tx);
    history->mutex.unlock();

    // If no candidate was found then no version is visible and tx must fail
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

    // std::cout << "Store::read(): version found for key '" << key << "': {";
    // std::cout << "begin=" << candidate->begin;
    // std::cout << ", end=" << candidate->end;
    // std::cout << ", data=" << candidate->data << "}\n";

    // Retrieve data from selected version
    result = candidate->data.to_std_string();
    return OK;
}

int Store::write(transaction::ptr tx, const key_type& key, const mapped_type& value)
{
    // std::cout << "Store::write(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!isValidTransaction(tx))
        return INVALID_TX;

    // Check if item was written before in the transaction
    auto& changeSet = tx->getChangeSet();
    auto changeIter = changeSet.find(key);
    if (changeIter != changeSet.end()) {
        auto& mod = changeIter->second;

        // Update the delta on the change set
        mod.delta = value;

        // The version affected by this change was 'removed' earlier in this
        // transaction so we change the modification from removal to update.
        // Updates remain updates, as do inserts.
        if (mod.code == transaction::Mod::Kind::Remove) {
            mod.code = transaction::Mod::Kind::Update;
        }
        return OK;
    }

    // std::cout << "write(): item not in change set" << std::endl;

    index_mutex.lock();
    history::ptr history;
    index->get(key, history);
    index_mutex.unlock();

    if (!history)
        return insert(tx, key, value);

    history->mutex.lock();
    version::ptr candidate = getWritableSnapshot(history, tx);
    if (!candidate) {
        auto hasValidVersions = hasValidSnapshots(history);
        history->mutex.unlock();

        if (!hasValidVersions)
            return insert(tx, key, value);

        return abort(tx, VALUE_NOT_FOUND);
    }

    // Mark version as temporary-invalid
    pmdk::transaction::exec_tx(pop, [&,this](){
        candidate->end.store(tx->getId());
    });

    // Let others enter the history
    history->mutex.unlock();

    // Update changeset of tx
    tx->getChangeSet().emplace(key, transaction::Mod{
        transaction::Mod::Kind::Update,
        candidate,
        value,
        nullptr
    });
    return OK;
}

int Store::drop(transaction::ptr tx, const key_type& key)
{
    // std::cout << "Store::drop(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!isValidTransaction(tx))
        return INVALID_TX;

    // Check if item was written before in the transaction
    auto& changeSet = tx->getChangeSet();
    auto changeIter = changeSet.find(key);
    if (changeIter != changeSet.end()) {
        auto& mod = changeIter->second;
        if (mod.code == transaction::Mod::Kind::Update) {
            // The version affected by this change was 'updated' earlier in this
            // transaction so we change the modification from update to removal.
            mod.code = transaction::Mod::Kind::Remove;
        }
        else if (mod.code == transaction::Mod::Kind::Insert) {
            // The version affected by this change was 'inserted' earlier in
            // this transaction so we simply discard the change altogether.
            changeSet.erase(changeIter);
            pmdk::transaction::exec_tx(pop, [&,this](){
                // Revalidate the temporarily invalidated version. This
                // operation is not synchronized with regard to is history.
                // However, the version already carries our id so we have full
                // ownership. Releasing it can cause no damage.
                mod.v_origin->end.store(TS_INFINITY);
            });
        }
        else if (mod.code == transaction::Mod::Kind::Remove) {
            // The version affected by this change was 'removed' earlier in
            // this transaction already, so we have fail here
            return VALUE_NOT_FOUND;
        }
        return OK;
    }

    // Look up history of data item. Abort if key does not exist.
    history::ptr history;
    index_mutex.lock();
    auto status = index->get(key, history);
    index_mutex.unlock();
    if (!status)
        return abort(tx, VALUE_NOT_FOUND);

    // In order to ensure a consistent view on the history, we need to
    // make sure that no one else can modify it.
    history->mutex.lock();
    auto candidate = getWritableSnapshot(history, tx);
    if (!candidate) {
        history->mutex.unlock();
        return abort(tx, VALUE_NOT_FOUND);
    }

    // Tentatively invalidate V with our tx id
    pmdk::transaction::exec_tx(pop, [&,this](){
        candidate->end.store(tx->getId());
    });
    history->mutex.unlock();

    tx->getChangeSet().emplace(key, transaction::Mod{
        transaction::Mod::Kind::Remove,
        candidate,
        "",
        nullptr
    });
    return OK;
}

void Store::print()
{
    const auto end = index->end();
    std::cout << "--" << std::endl;
    std::cout << "buckets: " << index->buckets() << std::endl;
    std::cout << "size: " << index->size() << std::endl;
    std::cout << "--" << std::endl;
    for (auto it = index->begin(); it != end; ++it) {
        std::cout << "key: "  << (*it)->key.get_ro().to_std_string() << std::endl;

        size_type i = 0;
        auto history = (*it)->value;
        for (auto v : history->chain) {
            if (i++ != 0)
                std::cout << "  --" << std::endl;
            std::cout << "  data : " << v->data.to_std_string() << std::endl;
            std::cout << "  began: " << v->begin << std::endl;
            std::cout << "  ended: " << v->end<< std::endl;
        }
        if (i != 0)
            std::cout << std::endl;
    }
}

// ############################################################################
// PRIVATE API
// ############################################################################

void Store::init()
{
    // Retrieve volatile pointer to index. This is done to avoid expensive calls
    // to the overloaded dereference operators in pmdk::persistent_ptr<T>.
    index = pop.get_root()->index.get();

    // Collapse the all histories. There is no point in keeping more than
    // one version of an item across restarts. The reason is that all
    // subsequent transactions in this newly initialized session are newer
    // than the latest version and therefore should only see the latest
    // version. Therefore we remove all other versions.
    //
    // Also, we must handle timestamps from the previous session. Unless we hold
    // the timestamp counter in persistent memory, which would make it even
    // more costly than its atomic operations, we need to make all versions
    // look like they had been committed before the first transaction of
    // this session. To do that, we simply set the begin timestamps in all
    // remaining versions to the initial value of the timestamp counter.
    // However, versions that have been invalidated must be deleted.
    // After that, we increase the timestamp counter.
    //
    // Last but not least, we should unlock all history mutexes, as they
    // may have become persistent in a previous session.
    const auto end = index->end();
    for (auto it = index->begin(); it != end; ) {
        auto& hist = (*it)->value;
        pmdk::transaction::exec_tx(pop, [&,this](){
            purgeHistory(hist);
            if (hist->chain.empty()) {
                // Purge left history empty, so we should remove it from
                // the index and deallocate it
                it = index->erase(it, pop);
                pmdk::delete_persistent<history>(hist);
            }
            else {
                // Release the lock on the current history, as it may have become
                // persistent in a previous session
                hist->mutex.unlock();
                ++it;
            }
        });
    }

    // Increase timestamp counter. This way, all subsequent transactions
    // of this session have higher timestamps than all the versions that
    // were reset above.
    timestampCounter.fetch_add(TS_DELTA);
}

void Store::purgeHistory(history::ptr& history)
{
    const auto first_stamp = timestampCounter.load();
    auto& chain = history->chain;
    auto end = chain.end();
    for (auto it = chain.begin(); it != end; ) {
        auto& v = *it;
        if (isTransactionId(v->begin)) {
            // V was created before restart but the associated tx
            // never committed or failed to finalize timestamps.
            // Therefore, we have to delete V.
            it = chain.erase(it, pop);
            pmdk::delete_persistent<version>(v);
        }
        else if (v->end == TS_INFINITY) {
            // V was valid before restart. Make it look like it was
            // created during this session.
            v->begin = first_stamp;
            ++it;
        }
        else if (isTransactionId(v->end)) {
            // V was invalidated but the associated transaction
            // never committed, so it is valid. Now that V is valid
            // again, make it look like it was created during this
            // session.
            v->begin = first_stamp;
            v->end = TS_INFINITY;
            ++it;
        }
        else {
            // V was invalidated and the associated transaction
            // has committed so we do not need V anymore.
            it = chain.erase(it, pop);
            pmdk::delete_persistent<version>(v);
        }
    }
}

int Store::insert(transaction::ptr tx, const key_type& key, const mapped_type& value)
{
    tx->getChangeSet().emplace(key, transaction::Mod{
        transaction::Mod::Kind::Insert,
        nullptr,
        value,
        nullptr
    });
    return OK;
}

version::ptr Store::getWritableSnapshot(history::ptr& history, transaction::ptr tx)
{
    // std::cout << "Store::getWritableSnapshot(tx{id=" << tx->getId() << "}):" << '\n';

    for (auto& v : history->chain) {
        if (isWritable(v, tx))
            return v;
    }
    return nullptr;
}

version::ptr Store::getReadableSnapshot(history::ptr& history, transaction::ptr tx)
{
    // std::cout << "Store::getReadableSnapshot(tx{id=" << tx->getId() << "}):" << '\n';

    for (auto& v : history->chain) {
        if (isReadable(v, tx))
            return v;
    }
    return nullptr;
}

bool Store::isReadable(version::ptr& v, transaction::ptr tx)
{
    // Read begin/end fields
    auto v_begin = v->begin;
    auto v_end = v->end.load();

    // Check if begin-field contains a transaction id.
    // If so, then V might still be dirty so we have to
    // look up its associated transaction and determine whether
    // (1) it committed and (2) whether that happened before tx started.
    // In the absence of a tx id, V is clearly committed but we have to
    // check if that happened before tx started.
    if (isTransactionId(v_begin)) {
        // Lookup the specified transaction
        transaction::ptr other_tx;
        tx_tab.find(v_begin, other_tx);

        // V (written by other_tx) is only visible to tx if other_tx
        // has committed before tx started.
        if (other_tx->getStatus().load() != transaction::COMMITTED ||
                other_tx->getEnd() > tx->getBegin())
            return false;
    }
    else {
        // V is only visible to tx if it was committed before tx started.
        if (v_begin >= tx->getBegin())
            return false;
    }

    // Inspect end field
    // If it contains a transaction id then we have to check that transaction.
    // Otherwise we have to check if V was not invalidated before tx started.
    if (isTransactionId(v_end)) {

        // Lookup the specified transaction
        transaction::ptr other_tx;
        tx_tab.find(v_end, other_tx);

        // V (possibly invalidated by other_tx) is only visible to tx
        // if other_tx is active, has aborted or has committed after
        // tx started. If other_tx committed before tx then V was
        // invalid before tx started and is thus invisible.
        if (other_tx->getStatus().load() == transaction::COMMITTED &&
                other_tx->getEnd() < tx->getBegin())
            return false;
    }
    else {
        // V is only visible to tx if it is not been invalidated prior to tx.
        //
        // Note: This constraint is less restrictive than its counterpart
        // in write(). Writing forbids any invalidation even if V is
        // still valid when tx started.
        if (v_end < tx->getBegin())
            return false;
    }

    // If we reach this point all constraints were met so
    // we found a potential candidate version.
    return true;
}

bool Store::isWritable(version::ptr& v, transaction::ptr tx)
{
    auto v_begin = v->begin;
    auto v_end = v->end.load();

    // Check if begin-field contains a transaction id.
    // If so, then V might still be dirty so we have to
    // look up its associated transaction and determine whether
    // (1) it committed and (2) whether that happened before tx started.
    // In the absence of a tx id, V is clearly committed but we have to
    // check if that happened before tx started.
    if (isTransactionId(v_begin)) {
        // Lookup the specified transaction
        transaction::ptr other_tx;
        tx_tab.find(v_begin, other_tx);

        // V (written by other_tx) is only visible to tx if other_tx
        // has committed before tx started.
        //
        // Note: This is the same assertion as is used for reading.
        if (other_tx->getStatus().load() != transaction::COMMITTED ||
                other_tx->getEnd() > tx->getBegin())
            return false;
    }
    else if (v_begin >= tx->getBegin()) {
        // V is only visible to tx if it was committed before tx started.
        //
        // Note: This is the same assertion as is used for reading.
        return false;
    }

    // Check if end-field contains a transaction id.
    // If so, then V might be outdated so we have to look up
    // its associated transaction and determine whether it aborted.
    // If it aborted then there may be a newer version of V but that
    // is invisible to tx as it was not committed. Hence V is visible to tx.
    // If it did not abort then it is still committed or active which means
    // that either V is now invalid or a write-write conflict would occur.
    // In the absence of a tx id, V is clearly committed but may be outdated.
    // In that case we have to check its timestamp for invalidation.
    if (isTransactionId(v_end)) {
        // Lookup the specified transaction
        transaction::ptr other_tx;
        tx_tab.find(v_end, other_tx);

        // V is only visible to tx if other_tx has aborted.
        if (other_tx->getStatus().load() != transaction::FAILED)
            return false;
    }
    else if (v_end != TS_INFINITY) {
        // V is only visible to tx if it is not been invalidated (no matter when).
        //
        // Note: This constraint is more restrictive than its
        // counterpart in read(). Reading allows invalidation
        // provided V was still valid when tx started.
        return false;
    }

    // If we reach this point all constraints were met so
    // we found a potential candidate version.
    return true;
}

bool Store::persist(transaction::ptr tx)
{
    // std::cout << "Store::persist(tx{id=" << tx->getId() << "}):" << '\n';

    bool success = true;
    const auto tid = tx->getId();
    pmdk::transaction::exec_tx(pop, [&,this](){
        for (auto& [key, change] : tx->getChangeSet()) {
            // Do nothing for removals
            if (change.code == transaction::Mod::Kind::Remove)
                continue;

            // Create new version
            auto new_version = pmdk::make_persistent<version>();
            new_version->begin = tid;
            new_version->data = change.delta;
            new_version->end = TS_INFINITY;

            // Register new version with change set
            change.v_new = new_version;

            // Get history of version (create if needed)
            history::ptr history;
            if (change.code == transaction::Mod::Kind::Update) {
                index_mutex.lock();
                index->get(key, history);
                index_mutex.unlock();
            }
            else if (change.code == transaction::Mod::Kind::Insert) {

                history::ptr exist_hist;

                // Handle ww-conflict when installing insertions. If another
                // transaction managed to insert a history for the same key
                // before us, then we clearly have a write/write conflict in
                // which case we must rollback all our installed versions and
                // histories
                index_mutex.lock();
                if (index->get(key, exist_hist)) {
                    exist_hist->mutex.lock();
                    auto hasValidEntries = hasValidSnapshots(exist_hist);
                    exist_hist->mutex.unlock();
                    if (!hasValidEntries) {
                        history = exist_hist;
                    }
                    else {
                        std::cout << "persist(): write/write conflict!\n";
                        success = false;
                    }
                }
                else {
                    history = pmdk::make_persistent<detail::history>();
                    bool insertSuccess = index->put(key, history, pop);
                    if (!insertSuccess) {
                        std::cout << "persist(): write/write conflict!\n";
                        pmdk::delete_persistent<detail::history>(history);
                        success = false;
                    }
                }
                index_mutex.unlock();
            }

            if (!success) {
                pmdk::delete_persistent<version>(new_version);
                change.v_new = nullptr;
                return;
            }

            // Add new version to item history
            history->mutex.lock();
            history->chain.push_front(new_version, pop);
            history->mutex.unlock();
        }
    });
    return success;
}

void Store::finalize(transaction::ptr tx)
{
    // std::cout << "Store::finalize(tx{id=" << tx->getId() << "}):" << '\n';

    const auto tx_end_stamp = tx->getEnd();
    pmdk::transaction::exec_tx(pop, [&,this](){
        // Finalize timestamps on all old and new versions
        for (auto& [key, change] : tx->getChangeSet()) {
            // Suppress unused variable warning
            (void)key;

            // Access to versions/histories is not synchronized here.
            // However, at this point, tx must have committed already and all
            // other transaction can see that. So outdated versions are clearly
            // marked as such, as are new versions. Therefore, these changes
            // are neutral and all other transactions will always see valid
            // data (timestamps or TIDs).
            switch (change.code) {
            case transaction::Mod::Kind::Insert:
                change.v_new->begin = tx_end_stamp;
                break;

            case transaction::Mod::Kind::Update:
                change.v_new->begin = tx_end_stamp;

                // Note: No test-and-set is required here because, as opposed
                // to rollbacks, no one is going to try and acquire ownership
                // on this version because it is actually outdated. During
                // rollbacks, versions are known to have been touched by a
                // failed transaction, so they are writable already when
                // a rollback starts. When finalizing a commit, versions
                // are not writable anymore, so no care must be taken.
                change.v_origin->end.store(tx_end_stamp);
                break;

            case transaction::Mod::Kind::Remove:
                // See note above.
                change.v_origin->end.store(tx_end_stamp);
                break;
            }
        }
    });
}

void Store::rollback(transaction::ptr tx)
{
    // std::cout << "Store::rollback(tx{id=" << tx->getId() << "}):" << '\n';

    auto tid = tx->getId();
    pmdk::transaction::exec_tx(pop, [&,this](){
        // Revalidate updated or removed versions and _invalidate_ new versions.
        // There may not be an new version for every insert/update if it was
        // version installment that led to this rollback.
        for (auto& [key, change] : tx->getChangeSet()) {
            // Suppress unused variable warning
            (void)key;

            switch (change.code) {
            case transaction::Mod::Kind::Insert:
                // Access to version/history is not synchronized here.
                // As a result, other transactions scanning this version may
                // see inconsistent timestamps. However, our tx has not
                // committed so none of these transactions should be able to
                // see this version anyway. Therefore, we need not worry here.
                if (change.v_new) {
                    change.v_new->begin = TS_ZERO;
                    change.v_new->end = TS_ZERO;
                }
                break;

            case transaction::Mod::Kind::Update:
                // Access to version/history is not synchronized here.
                // As a result, other transactions scanning this version may
                // see inconsistent timestamps. However, our tx has not
                // committed so none of these transactions should be able to
                // see this version anyway. Therefore, we need not worry here.
                if (change.v_new) {
                    change.v_new->begin = TS_ZERO;
                    change.v_new->end = TS_ZERO;
                }

                // Access to version/history is not synchronized here.
                // As a result, other transactions (seeing our tx has failed)
                // could try to acquire ownership for the current version.
                // Therefore, we need to test if our transaction id is still
                // there and reset it, otherwise we simply fail because some
                // other transaction already correctly owns this version.
                // Since all updaters register themselves atomically, they
                // will either insert their TID first (in which case we fail
                // to reset it) or they will find a perfectly TS_INFINITY timestamp
                // which they can overwrite with their TID without problems.
                change.v_origin->end.compare_exchange_strong(tid, TS_INFINITY);
                tid = tx->getId(); // recover from side effect of CAS above
                break;

            case transaction::Mod::Kind::Remove:
                // Access to version/history is not synchronized here.
                // As a result, other transactions (seeing our tx has failed)
                // could try to acquire ownership for the current version.
                // Therefore, we need to test if our transaction id is still
                // there and reset it, otherwise we simply fail because some
                // other transaction already correctly owns this version.
                // Since all updaters register themselves atomically, they
                // will either insert their TID first (in which case we fail
                // to reset it) or they will find a perfectly TS_INFINITY timestamp
                // which they can overwrite with their TID without problems.
                change.v_origin->end.compare_exchange_strong(tid, TS_INFINITY);
                tid = tx->getId(); // recover from side effect of CAS above
                break;
            }
        }
    });
} // end function rollback

bool Store::isValidTransaction(const transaction::ptr tx)
{
    return (tx && tx_tab.contains(tx->getId()) &&
            tx->getStatus().load() == transaction::ACTIVE);
}

bool Store::hasValidSnapshots(const history::ptr& hist)
{
    for (auto& v : hist->chain) {
        auto v_end = v->end.load();
        if (v_end == TS_INFINITY || isTransactionId(v_end))
            return true;
    }
    return false;
}

bool Store::isTransactionId(const stamp_type data)
{
    return data & 1;
}

bool init(Store::pool_type& pop, std::string file, size_type pool_size)
{
    namespace filesystem = std::experimental::filesystem::v1;
    using pool_type = Store::pool_type;
    using index_type = Store::index_type;

    const std::string layout{"midas"};
    if (filesystem::exists(file)) {
        if (pool_type::check(file, layout) != 1) {
            std::cout << "File seems to be corrupt! Aborting..." << std::endl;
            return false;
        }
        pop = pool_type::open(file, layout);
    }
    else {
        pop = pool_type::create(file, layout, pool_size);
        auto root = pop.get_root();
        pmdk::transaction::exec_tx(pop, [&](){
            root->index = pmdk::make_persistent<index_type>();
        });
    }
    return true;
}

} // end namespace detail
} // end namespace midas
