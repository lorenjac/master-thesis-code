#include "store.hpp"

#include <memory>

namespace midas {

// Infinity timestamp. Only used for end timestamps of valid versions.
static const stamp_type INF = std::numeric_limits<stamp_type>::max() - 1;

inline bool is_tx_id(const stamp_type data)
{
    return data & 1;
}

inline bool is_valid_tx(const transaction::ptr tx)
{
    return (tx && tx->getStatus().load() == transaction::ACTIVE);
}

store::store(pool_type& pop)
    : pop{pop}
    , index{}
    , tx_tab{}
{
    init();
}

void store::init()
{
    std::cout << "store::init()" << std::endl;

    // Retrieve volatile pointer to index. This is done to avoid expensive calls
    // to the overloaded dereference operators in pmdk::persistent_ptr<T>.
    index = pop.get_root()->index.get();

    std::cout << "store::init(): resetting histories..." << std::endl;

    // for (auto& [key, hist] : *index) { // crashes GCC !!!

    // Collapse the all histories. There is no point in keeping more than
    // one version of an item across restarts. The reason is that all
    // subsequent transactions in this newly initialized session are newer
    // than the latest version and therefore should only see the latest
    // version. Therefore we remove all other versions.
    //
    // Also, must handle timestamps from the previous session. Unless we hold
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
    for (auto iter = index->begin(); iter != end; ++iter) {

        // const auto& key = (*iter)->key.get_ro();
        // std::cout << "store::init(): resetting history of '" << key << "'\n";

        auto& hist = (*iter)->value;
        auto& chain = hist->chain;
        if (chain.size()) {
            pmdk::transaction::exec_tx(pop, [&,this](){
                while (chain.size() > 1) {
                    pmdk::delete_persistent<detail::version>(chain.get(0));
                    chain.remove(0, pop);
                }

                // std::cout << "store::init(): resetting '" << chain.get(0)->data;
                // std::cout << "'" << std::endl;

                auto& latest = chain.get(0);
                if (is_tx_id(latest->end)) {
                    // On recent session a transaction invalidated the latest
                    // item but failed to commit, so we have to revalidate it.
                    latest->begin = next_ts;
                    latest->end = INF;
                }
                else if (latest->end == INF) {
                    // Recent session ended normally, but begin timestamps
                    // of valid items are no longer valid, so we have to reset
                    // them
                    latest->begin = next_ts;
                }
                else {
                    // Recent session ended normally, but properly invalidated
                    // items are still there, so we should delete them
                    // altogether
                    pmdk::delete_persistent<detail::version>(latest);
                    chain.remove(0, pop);
                }
            });
        }

        // Release the lock on the current history, as it may have become
        // persistent in a previous session
        hist->mutex.unlock();
    }

    // TODO init(): remove empty/purged histories
    // index->erase_if([](auto& key, auto& hist){
    //     return hist->chain.empty();
    // });

    // Increase timestamp counter. This way, all subsequent transactions
    // of this session have higher timestamps than all the versions that
    // were reset above.
    next_ts.fetch_add(2);
}

transaction::ptr store::begin()
{
    std::cout << "store::begin()" << std::endl;

    // Create new transaction with current timestamp
    auto tx = std::make_shared<transaction>(
        next_id.fetch_add(2),
        next_ts.fetch_add(2)
    );

    if (!tx)
        return nullptr;

    // Add new transaction to list of running transactions
    tx_tab.insert(tx->getId(), tx);

    std::cout << "store::begin(): spawned new transaction {";
    std::cout << "id=" << tx->getId() << ", begin=" << tx->getBegin() << "}\n";
    std::cout << "store::begin(): number of transactions is " << tx_tab.size() << '\n';

    return tx;
}

int store::abort(transaction::ptr tx, int reason)
{
    std::cout << "store::abort(tx{id=" << tx->getId() << "}";
    std::cout << ", reason=" << reason << "):" << '\n';

    // Reject invalid or inactive transactions.
    if (!is_valid_tx(tx))
        return INVALID_TX;

    // Mark this transaction as aborted/failed.
    // This must be done atomically because operations of concurrent
    // transactions might be querying the state of tx (if they
    // found its id in a version they want to read or write).
    tx->getStatus().store(transaction::FAILED);

    // Undo all changes carried out by tx
    rollback(tx);

    // return the specified error code (supplied by the caller)
    return reason;
}

int store::commit(transaction::ptr tx)
{
    std::cout << "store::commit(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!is_valid_tx(tx))
        return INVALID_TX;

    // Set tx end timestamp
    tx->setEnd(next_ts.fetch_add(2));

    // Note: This is where validation would take place (not required for snapshot isolation)
    // if (!validate(tx))
    //     return abort(tx, 0xDEADBEEF);

    if (!installVersions(tx))
        return abort(tx, 0xDEADBEEF);

    // Mark tx as committed.
    // This must be done atomically because operations of concurrent
    // transactions might be querying the state of tx (e.g. if they
    // found its id in a version they want to read or write).
    tx->getStatus().store(transaction::COMMITTED);

    // Propagate end timestamp of tx to end/begin fields of original/new versions
    installEndStamps(tx);

    std::cout << "store::commit(): committed transaction {";
    std::cout << "id=" << tx->getId();
    std::cout << ", begin=" << tx->getBegin();
    std::cout << ", end=" << tx->getEnd() << "}\n";

    // Now that all its modifications have become persistent, we can safely
    // remove this transaction from our list
    tx_tab.erase(tx->getId());

    std::cout << "store::commit(): number of transactions is " << tx_tab.size() << '\n';
    return OK;
}

int store::read(transaction::ptr tx, const key_type& key, mapped_type& result)
{
    std::cout << "store::read(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!is_valid_tx(tx))
        return INVALID_TX;

    // Look up data item. Abort if key does not exist.
    index_mutex.lock();
    detail::history::ptr history;
    auto status = index->get(key, history);
    index_mutex.unlock();
    if (!status) {
        return abort(tx, VALUE_NOT_FOUND);
    }

    // Scan history for latest committed version which is older than tx.
    history->mutex.lock();
    auto candidate = getVersionR(history, tx);
    history->mutex.unlock();

    // If no candidate was found then no version is visible and tx must fail
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

    std::cout << "store::read(): version found for key '" << key << "': {";
    std::cout << "begin=" << candidate->begin;
    std::cout << ", end=" << candidate->end;
    std::cout << ", data=" << candidate->data << "}\n";

    // Retrieve data from selected version
    result = candidate->data.to_std_string();
    return OK;
}

int store::write(transaction::ptr tx, const key_type& key, const mapped_type& value)
{
    std::cout << "store::write(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!is_valid_tx(tx))
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

    // TODO write(): make sure empty histories can be updated
    //  (A) remove history as soon as it becomes empty (neutral to update/insert)
    //  (B) keep empty histories and enable insertion with _insert() (spares heap ops)

    int status = OK;
    detail::history::ptr history;

    index_mutex.lock();
    if (index->get(key, history)) {
        // We no longer need the index so unlock it
        index_mutex.unlock();
        status = _update(tx, key, value, history);
    }
    else {
        // We still need the index for insertion so keep it locked
        status = _insert(tx, key, value);
        index_mutex.unlock();
    }
    return status;
}

int store::_update(transaction::ptr tx, const key_type& key, const mapped_type& value, detail::history::ptr history)
{
    std::cout << "store::_update(tx{id=" << tx->getId() << "}):" << '\n';

    history->mutex.lock();
    detail::version::ptr candidate = getVersionW(history, tx);
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

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
        value
    });
    return OK;
}

int store::_insert(transaction::ptr tx, const key_type& key, const mapped_type& value)
{
    std::cout << "store::_insert(tx{id=" << tx->getId() << "}):" << '\n';

    tx->getChangeSet().emplace(key, transaction::Mod{
        transaction::Mod::Kind::Insert,
        nullptr,
        value
    });
    return OK;
}

int store::drop(transaction::ptr tx, const key_type& key)
{
    std::cout << "store::drop(tx{id=" << tx->getId() << "}):" << '\n';

    // Reject invalid or inactive transactions.
    if (!is_valid_tx(tx))
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
                mod.version->end.store(INF);
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
    detail::history::ptr history;
    index_mutex.lock();
    auto status = index->get(key, history);
    index_mutex.unlock();
    if (!status)
        return abort(tx, VALUE_NOT_FOUND);

    // In order to ensure a consistent view on the history, we need to
    // make sure that no one else can modify it.
    history->mutex.lock();
    auto candidate = getVersionW(history, tx);
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

    // Tentatively invalidate V with our tx id
    pmdk::transaction::exec_tx(pop, [&,this](){
        candidate->end.store(tx->getId());
    });
    history->mutex.unlock();

    tx->getChangeSet().emplace(key, transaction::Mod{
        transaction::Mod::Kind::Remove,
        candidate,
        ""
    });
    return OK;
}

detail::version::ptr store::getVersionW(detail::history::ptr& history, transaction::ptr tx)
{
    std::cout << "store::getVersionW(tx{id=" << tx->getId() << "}):" << '\n';
    for (auto& v : history->chain) {
        if (isWritable(v, tx))
            return v;
    }
    return nullptr;
}

detail::version::ptr store::getVersionR(detail::history::ptr& history, transaction::ptr tx)
{
    std::cout << "store::getVersionR(tx{id=" << tx->getId() << "}):" << '\n';
    for (auto& v : history->chain) {
        if (isReadable(v, tx))
            return v;
    }
    return nullptr;
}

bool store::isReadable(detail::version::ptr& v, transaction::ptr tx)
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
    if (is_tx_id(v_begin)) {
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
    if (is_tx_id(v_end)) {

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

bool store::isWritable(detail::version::ptr& v, transaction::ptr tx)
{
    auto v_begin = v->begin;
    auto v_end = v->end.load();

    // Check if begin-field contains a transaction id.
    // If so, then V might still be dirty so we have to
    // look up its associated transaction and determine whether
    // (1) it committed and (2) whether that happened before tx started.
    // In the absence of a tx id, V is clearly committed but we have to
    // check if that happened before tx started.
    if (is_tx_id(v_begin)) {
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
    if (is_tx_id(v_end)) {
        // Lookup the specified transaction
        transaction::ptr other_tx;
        tx_tab.find(v_end, other_tx);

        // V is only visible to tx if other_tx has aborted.
        if (other_tx->getStatus().load() != transaction::FAILED)
            return false;
    }
    else if (v_end != INF) {
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

bool store::installVersions(transaction::ptr tx)
{
    std::cout << "store::installVersions(tx{id=" << tx->getId() << "}):" << '\n';

    const auto tx_end_stamp = tx->getEnd();
    pmdk::transaction::exec_tx(pop, [&,this](){
        for (auto [key, change] : tx->getChangeSet()) {
            // Do nothing for removals
            if (change.code == transaction::Mod::Kind::Remove)
                continue;

            // Create new version
            auto new_version = pmdk::make_persistent<detail::version>();
            // FIXME placing end time stamp here is wrong (tx has not committed)
            new_version->begin = tx_end_stamp;
            new_version->data = change.delta;
            new_version->end = INF;

            // Get history of version (create if needed)
            detail::history::ptr history;
            if (change.code == transaction::Mod::Kind::Update) {
                index_mutex.lock();
                index->get(key, history);
                index_mutex.unlock();
            }
            else if (change.code == transaction::Mod::Kind::Insert) {
                history = pmdk::make_persistent<detail::history>();
                index_mutex.lock();
                bool insertSuccess = index->put(key, history, pop);
                if (!insertSuccess) {
                    // FIXME handle ww-conflict when installing insertions
                    // If another transaction managed to insert a history for
                    // the same key before us, then we clearly have a
                    // write/write conflict in which case we must rollback
                    // all our installed versions / histories

                    // Should be fine if history is empty or has no valid versions
                }
                index_mutex.unlock();
            }

            // Add new version to item history
            history->mutex.lock();
            // TODO make scans faster by inserting at the front
            history->chain.append(new_version, pop);
            history->mutex.unlock();
        }
    });
    return true;
}

void store::installEndStamps(transaction::ptr tx)
{
    std::cout << "store::installEndStamps(tx{id=" << tx->getId() << "}):" << '\n';

    const auto tx_end_stamp = tx->getEnd();
    pmdk::transaction::exec_tx(pop, [&,this](){
        // Finalize timestamps on all outdated versions
        // All new versions are already created with their final timestamp
        for (auto [key, change] : tx->getChangeSet()) {
            (void)key;
            if (change.code == transaction::Mod::Kind::Update ||
                    change.code == transaction::Mod::Kind::Remove) {
                change.version->end.store(tx_end_stamp);
                // TODO make new versions valid here
            }
        }
    });
}

void store::rollback(transaction::ptr tx)
{
    std::cout << "store::rollback(tx{id=" << tx->getId() << "}):" << '\n';

    pmdk::transaction::exec_tx(pop, [&,this](){
        // revalidate updated or removed versions (set end = INF)
        // newly inserted items are never created so no rollback needed
        for (auto [key, change] : tx->getChangeSet()) {
            (void)key;
            if (change.code == transaction::Mod::Kind::Update ||
                    change.code == transaction::Mod::Kind::Remove) {
                change.version->end.store(INF);
            }
        }
    });
}

} // end namespace midas
