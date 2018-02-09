#include "store.hpp"

#include <memory>

namespace midas {

// Infinity timestamp. Only used for end timestamps of valid versions.
static const stamp_type INF = std::numeric_limits<stamp_type>::max() - 1;

inline bool is_tx_id(const stamp_type data)
{
    return data & 1;
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
                if (latest->end == INF) {
                    latest->begin = next_ts;
                }
                else {
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
    std::cout << "store::abort(" << reason << ")" << std::endl;

    // Reject invalid or inactive transactions.
    if (!tx || tx->getStatus().load() != transaction::ACTIVE)
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
    std::cout << "store::commit()" << std::endl;

    // Reject invalid or inactive transactions.
    if (!tx || tx->getStatus().load() != transaction::ACTIVE)
        return INVALID_TX;

    // Set tx end timestamp
    tx->setEnd(next_ts.fetch_add(2));

    // Note: This is where validation would take place (not required for snapshot isolation)
    // TODO commit(): add validation

    // Mark tx as committed.
    // This must be done atomically because operations of concurrent
    // transactions might be querying the state of tx (e.g. if they
    // found its id in a version they want to read or write).
    tx->getStatus().store(transaction::COMMITTED);

    // Propagate end timestamp of tx to end/begin fields of original/new versions
    persist(tx);

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
    // Reject invalid or inactive transactions.
    if (!tx || tx->getStatus().load() != transaction::ACTIVE)
        return INVALID_TX;

    // In order to prevent iterator invalidation by concurrent inserts or
    // garbage collection we need to make sure that no one else can access
    // the index.
    index_mutex.lock();

    // Look up data item. Abort if key does not exist.
    std::cout << "store::read(): looking up '" << key << "'" << std::endl;
    detail::history::ptr history;
    if (!index->get(key, history)) {
        std::cout << "store::read(): key '" << key << "' does not exist" << std::endl;
        index_mutex.unlock();
        return abort(tx, VALUE_NOT_FOUND);
    }

    // Now that we retrieved the history, we can safely unlock the index again.
    index_mutex.unlock();

    // In orde to prevent concurrent modification, we need to
    // make sure that no one else can access the history.
    history->mutex.lock();

    // Abort if no version of the queried data item exists
    if (history->chain.empty())
        return abort(tx, VALUE_NOT_FOUND);

    // Scan history for latest committed version which is older than tx.
    // Search direction is reversed to find later items first.
    detail::version::ptr candidate;
    // for (auto it = history.versions().rbegin(); it != history.versions().rend(); ++it) {
    // The current version
    // auto v = *it;

    // TODO read(): check if non-reversed for-loop is correct
    // TODO read(): start scan with latest version
    for (auto v : history->chain) {

        // Read begin/end fields
        // auto v_begin = v->begin.load();
        // auto v_end = v->end.load();
        auto v_begin = v->begin;
        auto v_end = v->end.load();

        // Check if begin-field contains a transaction id.
        // If so, then V might still be dirty so we have to
        // look up its associated transaction and determine whether
        // (1) it committed and (2) whether that happened before tx started.
        // In the absence of a tx id, V is clearly committed but we have to
        // check if that happened before tx started.
        if (is_tx_id(v_begin)) {
            // If V was created by tx then it is visible to tx
            if (v_begin == tx->getId()) {
                candidate = v;
                break;
            }

            // Lookup the specified transaction
            // tx_table_mutex.lock();
            transaction::ptr other_tx;
            tx_tab.find(v_begin, other_tx);
            // tx_table_mutex.unlock();

            // V (written by other_tx) is only visible to tx if other_tx
            // has committed before tx started.
            if (other_tx->getStatus().load() != transaction::COMMITTED ||
                    other_tx->getEnd() > tx->getBegin())
                continue;
        }
        else {
            // V is only visible to tx if it was committed before tx started.
            if (v_begin >= tx->getBegin())
                continue;
        }

        // Inspect end field
        // If it contains a transaction id then we have to check that transaction.
        // Otherwise we have to check if V was not invalidated before tx started.
        if (is_tx_id(v_end)) {
            // Lookup the specified transaction
            // tx_table_mutex.lock();
            transaction::ptr other_tx;
            tx_tab.find(v_end, other_tx);
            // tx_table_mutex.unlock();

            // V (possibly invalidated by other_tx) is only visible to tx
            // if other_tx is active, has aborted or has committed after
            // tx started. If other_tx committed before tx then V was
            // invalid before tx started and is thus invisible.
            if (other_tx->getStatus().load() == transaction::COMMITTED &&
                    other_tx->getEnd() < tx->getBegin())
                continue;
        }
        else {
            // V is only visible to tx if it is not been invalidated prior to tx.
            //
            // Note: This constraint is less restrictive than its counterpart
            // in write(). Writing forbids any invalidation even if V is
            // still valid when tx started.
            if (v_end < tx->getBegin())
                continue;
        }

        // If we reach this point all constraints were met so
        // we found a potential candidate version.
        candidate = v;
        break;
    }
    // history.unlock();
    history->mutex.unlock();

    // If no candidate was found then no version is visible and tx must fail
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

    // If we reach this point, we're good to go! :-)

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
    std::cout << "store::write()" << std::endl;

    // Reject invalid or inactive transactions.
    if (!tx || tx->getStatus().load() != transaction::ACTIVE)
        return INVALID_TX;

    // In order to prevent iterator invalidation by concurrent inserts or
    // garbage collection we need to make sure that no one else can acceess
    // the index.
    index_mutex.lock();

    // TODO write(): make sure empty histories can be updated
    //  (A) remove history as soon as it becomes empty (neutral to update/insert)
    //  (B) keep empty histories and enable insertion with _insert() (spares heap ops)

    int status = OK;
    std::cout << "store::write(): looking up '" << key << "'" << std::endl;
    detail::history::ptr history;
    if (index->get(key, history)) {

        // We no longer need the index so unlock it
        index_mutex.unlock();
        status = _update(tx, key, value, history);
    }
    else {
        std::cout << "store::write(): key '" << key << "' does not exist" << std::endl;

        // We still need the index for insertion so keep it locked
        status = _insert(tx, key, value);
        index_mutex.unlock();
    }
    return status;
}

int store::_update(transaction::ptr tx, const key_type& key, const mapped_type& value, detail::history::ptr history)
{
    std::cout << "store::_update()" << std::endl;

    // In order to prevent concurrent modification, we need to make sure that
    // no one else can access the history.
    history->mutex.lock();

    // Abort if no version of the queried data item exists
    if (history->chain.empty())
        return abort(tx, VALUE_NOT_FOUND);

    // The latest committed version suitable for writing
    detail::version::ptr candidate;

    // End timestamp of candidate version. We will need it later when
    // determining whether another tx grabbed our candidate before us.
    stamp_type v_end;

    // Scan history for latest committed version which is older than tx.
    // Search direction is reversed to find later items first.
    // for (auto it = history.versions().rbegin(); it != history.versions().rend(); ++it) {
    //     auto v = *it;

    // TODO _update(): check if non-reversed for-loop is correct
    for (auto v : history->chain) {

        // Read begin/end fields
        // auto v_begin = v->begin.load();
        // v_end = v->end.load();
        auto v_begin = v->begin;
        v_end = v->end;

        // Check if begin-field contains a transaction id.
        // If so, then V might still be dirty so we have to
        // look up its associated transaction and determine whether
        // (1) it committed and (2) whether that happened before tx started.
        // In the absence of a tx id, V is clearly committed but we have to
        // check if that happened before tx started.
        if (is_tx_id(v_begin)) {
            // If V was created by tx then it is visible to tx
            if (v_begin == tx->getId()) {
                // Apply changes and return
                v->data = value;
                return OK;
            }

            // Lookup the specified transaction
            // tx_table_mutex.lock();
            transaction::ptr other_tx;
            tx_tab.find(v_begin, other_tx);
            // tx_table_mutex.unlock();

            // V (written by other_tx) is only visible to tx if other_tx
            // has committed before tx started.
            //
            // Note: This is the same assertion as is used for reading.
            if (other_tx->getStatus().load() != transaction::COMMITTED ||
                    other_tx->getEnd() > tx->getBegin())
                continue;
        }
        else {
            // V is only visible to tx if it was committed before tx started.
            //
            // Note: This is the same assertion as is used for reading.
            if (v_begin >= tx->getBegin())
                continue;
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
            // tx_table_mutex.lock();
            transaction::ptr other_tx;
            tx_tab.find(v_end, other_tx);
            // tx_table_mutex.unlock();

            // V is only visible to tx if other_tx has aborted.
            if (other_tx->getStatus().load() != transaction::FAILED)
                continue;
        }
        else {
            // V is only visible to tx if it is not been invalidated (no matter when).
            //
            // Note: This constraint is more restrictive than its
            // counterpart in read(). Reading allows invalidation
            // provided V was still valid when tx started.
            if (v_end != INF)
                continue;
        }

        // If we reach this point all constraints were met so
        // we found a potential candidate version.
        candidate = v;
        break;
    }
    // history.unlock();
    history->mutex.unlock();

    // If no candidate was found then no version is visible and tx must fail
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

    // Atomically set id of tx as end timestamp of version to be updated.
    // If this CAS succeeds this transaction has exclusive write access
    // to the candidate while all other contesters will fail. Likewise,
    // if another transaction precedes tx then tx must fail. We compare
    // with v_end because that is the timestamp/tid we saw during visibility
    // check. If someone preceded us then v->end will be different now.
    if (!candidate->end.compare_exchange_strong(v_end, tx->getId()))
        return abort(tx, WRITE_CONFLICT);

    // If we reach this point, we're good to go! :-)

    // Create new version
    // auto new_version = std::make_shared<version>();
    // new_version->begin = tx->getId();
    // new_version->data = data;
    // new_version->end = INF;
    detail::version::ptr new_version;
    pmdk::transaction::exec_tx(pop, [&,this](){
        new_version = pmdk::make_persistent<detail::version>();
        new_version->begin = tx->getId();
        new_version->data = value;
        new_version->end = INF;
    });

    // Update write_set with a before-after pair of this write operation
    // tx->write_set.emplace_back(candidate, new_version);
    tx->getWriteSet().emplace_back(candidate, new_version);

    // Add new version to history
    history->mutex.lock();
    history->chain.append(new_version, pop);
    history->mutex.unlock();
    return OK;
}

int store::_insert(transaction::ptr tx, const key_type& key, const mapped_type& value)
{
    std::cout << "store::_insert()" << std::endl;

    detail::version::ptr new_version;
    pmdk::transaction::exec_tx(pop, [&,this](){
        // Create new version
        new_version = pmdk::make_persistent<detail::version>();
        new_version->begin = tx->getId();
        new_version->data = value;
        new_version->end = INF;

        std::cout << "store::_insert(): created new version {";
        std::cout << "data=" << new_version->data;
        std::cout << ", begin=" << new_version->begin;
        std::cout << ", end=" << new_version->end << "}\n";

        // Create new history with initial version and add it to the index.
        // We could do that lazily on commit but then write-write conlicts
        // would be possible during commit (currently they are only bound
        // to happen when updating an existing item). Also its consistent
        // with updating items, where versions are also inserted into
        // their histories immediately.
        auto new_history = pmdk::make_persistent<detail::history>();
        new_history->chain.append(new_version, pop);

        // FIXME _insert(): can insertion of new history fail? what if so?
        index->put(key, new_history, pop);
    });

    // Register this modification with its associated transaction, so that
    // its timestamps can be finalized on commit.
    tx->getCreateSet().emplace_back(key, new_version);
    return OK;
}

int store::drop(transaction::ptr tx, const key_type& key)
{
    std::cout << "store::drop()" << std::endl;

    // Reject invalid or inactive transactions.
    if (!tx || tx->getStatus().load() != transaction::ACTIVE)
        return INVALID_TX;

    // Look up data item. Abort if key does not exist.
    pmdk::persistent_ptr<detail::history> history;
    index_mutex.lock();
    auto status = index->get(key, history);
    index_mutex.unlock();
    if (!status)
        return abort(tx, VALUE_NOT_FOUND);

    std::cout << "store::drop(): history exists" << std::endl;

    // In order to ensure a consistent view on the history, we need to
    // make sure that no one else can modify it.
    history->mutex.lock();

    // Abort if no version of the queried data item exists
    if (history->chain.empty())
        return abort(tx, VALUE_NOT_FOUND);

    std::cout << "store::drop(): history is non-empty" << std::endl;

    // The latest committed version suitable for writing
    detail::version::ptr candidate;

    // End timestamp of candidate version. We will need this later when
    // determining whether another tx grabbed our candidate before us.
    stamp_type v_end;

    // Scan history for latest committed version which is older than tx.
    // Search direction is reversed to find later items first.
    // for (auto it = history.versions().rbegin(); it != history.versions().rend(); ++it) {
    //     auto v = *it;
    // TODO drop(): check if non-reversed for-loop is correct
    for (auto& v : history->chain) {

        // atomically read begin/end fields
        auto v_begin = v->begin;
        v_end = v->end.load();

        // Check if begin-field contains a transaction id.
        // If so, then V might still be dirty so we have to
        // look up its associated transaction and determine whether
        // (1) it committed and (2) whether that happened before tx started.
        // In the absence of a tx id, V is clearly committed but we have to
        // check if that happened before tx started.
        if (is_tx_id(v_begin)) {
            // If V was created by tx then it is visible to tx
            if (v_begin == tx->getId()) {
                candidate = v;
                break;
            }

            // Lookup the specified transaction
            transaction::ptr other_tx;
            tx_tab.find(v_begin, other_tx);

            // V (written by other_tx) is only visible to tx if other_tx
            // has committed before tx started.
            //
            // Note: This is the same assertion as is used for reading.
            if (other_tx->getStatus().load() != transaction::COMMITTED ||
                    other_tx->getEnd() > tx->getBegin())
                continue;
        }
        else {
            // V is only visible to tx if it was committed before tx started.
            //
            // Note: This is the same assertion as is used for reading.
            if (v_begin >= tx->getBegin())
                continue;
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
                continue;
        }
        else {
            // V is only visible to tx if it is not been invalidated (no matter when).
            //
            // Note: This constraint is more restrictive than its
            // counterpart in read(). Reading allows invalidation
            // provided V was still valid when tx started.
            if (v_end != INF)
                continue;
        }

        // If we reach this point all constraints were met so
        // we found a potential candidate version.
        candidate = v;
        break;
    }
    history->mutex.unlock();

    // If no candidate was found then no version is visible and tx must fail
    if (!candidate)
        return abort(tx, VALUE_NOT_FOUND);

    std::cout << "store::drop(): version found" << std::endl;

    // Atomically set id of tx as end timestamp of version to be updated.
    // If this CAS succeeds then this transaction has exclusive write access
    // to the candidate while all other contesters fail. Likewise, if another
    // transaction precedes tx then tx fails. We compare with v_end because
    // that is the timestamp/tid we saw during visibility check. If someone
    // preceded us then v->end will be different now in which case we fail.
    if (!candidate->end.compare_exchange_strong(v_end, tx->getId()))
        return abort(tx, WRITE_CONFLICT);

    // Placing tx's id in the end field of the candidate version V was actually
    // all we really had to do here. This invalidates V as soon as tx commits.
    // We do not delete V because tx could still fail in which case we need to
    // be able to rollback. Likewise, if V is the currently the only version in
    // its history, then we cannot delete that history. This can be done on
    // successful commit or later (e.g. GC). For now, we only add V to our
    // remove set, so that we can rollback if need be.
    tx->getRemoveSet().emplace_back(key, candidate);
    return OK;
}

void store::persist(transaction::ptr tx)
{
    std::cout << "store::persist(tx{id=" << tx->getId() << "}):" << '\n';

    pmdk::transaction::exec_tx(pop, [&,this](){

        for (auto& delta : tx->getWriteSet()) {
            // Update end field of old version.
            delta.first->end.store(tx->getEnd());

            // Update begin field of new version.
            delta.second->begin = tx->getEnd();
        }

        // Same as above for created items
        for (auto& created : tx->getCreateSet()) {
            created.second->begin = tx->getEnd();
        }

        // Same as above for removed items
        for (auto& removed : tx->getRemoveSet()) {
            removed.second->end.store(tx->getEnd());
        }
    });
}

void store::rollback(transaction::ptr tx)
{
    std::cout << "store::rollback(tx{id=" << tx->getId() << "}):" << '\n';

    pmdk::transaction::exec_tx(pop, [&,this](){

        // Undo all writes
        for (auto& delta : tx->getWriteSet()) {
            // Re-validate old version by setting end timestamp (back) to infinity.
            // When starting the initial write operation, the version must have been writeable.
            // This means that its end timestamp was either an id of a failed transaction or a
            // timestamp of infinity. The former would be changed to infinity in the same manner.
            delta.first->end.store(INF);

            // invalidate the new version (will be collected by GC later)
            delta.second->begin = 0;
            delta.second->end.store(0);
        }

        // Undo all creations
        for (auto& created : tx->getCreateSet()) {
            // invalidate the new version (will be collected by GC later)
            created.second->begin = 0;
        }

        // Undo all deletions
        for (auto& removed : tx->getRemoveSet()) {
            // Re-validate old version by setting end timestamp (back) to infinity.
            // Same mechanics as for undoing writes.
            removed.second->end.store(INF);
        }
    });
}

} // end namespace midas
