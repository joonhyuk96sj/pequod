#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH
#include "local_vector.hh"
#include "hashtable.hh"
#include "pqsource.hh"
#include "pqsink.hh"
#include "time.hh"
#include <iterator>
class Json;

namespace pq {
namespace bi = boost::intrusive;

class Table : public Datum {
  public:
    typedef ServerStore store_type;

    Table(Str name, Table* parent, Server* server);
    ~Table();
    static Table empty_table;

    typedef Str key_type;
    typedef Str key_const_reference;
    inline Str name() const;
    inline Str hashkey() const;

    typedef ServerStore::iterator local_iterator;
    inline local_iterator lbegin();
    inline local_iterator lend();
    inline local_iterator lfind(Str key);

    class iterator;
    inline iterator begin();
    inline iterator end();
    inline iterator lower_bound_hint(Str key);
    inline iterator lower_bound(Str key);
    inline size_t size() const;
    inline const Datum& operator[](Str key) const;
    inline size_t count(Str key) const;

    iterator validate(Str first, Str last, uint64_t now);
    inline iterator validate(Str key, uint64_t now);
    inline void invalidate_dependents(Str key);
    inline void invalidate_dependents(Str first, Str last);

    void add_source(SourceRange* r);
    inline void unlink_source(SourceRange* r);
    void remove_source(Str first, Str last, SinkRange* sink, Str context);
    void add_join(Str first, Str last, Join* j, ErrorHandler* errh);

    local_iterator insert(Table& t);
    void insert(Str key, String value);
    template <typename F>
    inline void modify(Str key, const SinkRange* sink, const F& func);
    void erase(Str key);
    inline iterator erase(iterator it);
    inline void invalidate_erase(Datum* d);
    inline iterator erase_invalid(iterator it);

    inline bool flush_for_pull(uint64_t now);

    void add_stats(Json& j) const;
    void print_sources(std::ostream& stream) const;

    uint64_t ninsert_;
    uint64_t nmodify_;
    uint64_t nmodify_nohint_;
    uint64_t nerase_;
    uint64_t nvalidate_;

  private:
    store_type store_;
    interval_tree<SourceRange> source_ranges_;
    interval_tree<JoinRange> join_ranges_;
    unsigned njoins_;
    uint64_t flush_at_;
    bool all_pull_;
    Server* server_;
    Table* parent_;

    std::pair<store_type::iterator, bool> prepare_modify(Str key, const SinkRange* sink, store_type::insert_commit_data& cd);
    void finish_modify(std::pair<store_type::iterator, bool> p, const store_type::insert_commit_data& cd, Datum* d, Str key, const SinkRange* sink, String value);
    inline void notify(Datum* d, const String& old_value, SourceRange::notify_type notifier);
    bool hard_flush_for_pull(uint64_t now);

    friend class Server;
    friend class iterator;
};

class Table::iterator : public std::iterator<std::forward_iterator_tag, Datum> {
  public:
    inline iterator() = default;
    inline iterator(Table* table, ServerStore::iterator it);

    inline Datum& operator*() const;
    inline Datum* operator->() const;

    inline bool operator==(const iterator& x) const;
    inline bool operator!=(const iterator& x) const;

    inline void operator++();

  private:
    ServerStore::iterator it_;
    Table* table_;

    inline void maybe_fix();
    void fix();
    friend class Table;
};

class Server {
  public:
    typedef ServerStore store_type;

    Server();

    typedef Table::iterator iterator;
    inline iterator begin();
    inline iterator end();
    inline const Datum* find(Str key) const;
    inline const Datum& operator[](Str key) const;
    inline size_t count(Str first, Str last) const;

    Table& table(Str tname) const;
    Table& make_table(Str tname);

    Table& table_for(Str key) const;
    Table& table_for(Str first, Str last) const;
    Table& make_table_for(Str key);
    Table& make_table_for(Str first, Str last);

    inline void insert(Str key, const String& value);
    inline void erase(Str key);

    void add_join(Str first, Str last, Join* j, ErrorHandler* errh = 0);

    inline uint64_t next_validate_at();
    inline Table::iterator validate(Str key);
    inline Table::iterator validate(Str first, Str last);
    size_t validate_count(Str first, Str last);

    Json stats() const;
    void print(std::ostream& stream);

  private:
    mutable Table supertable_;
    uint64_t last_validate_at_;

    Table::local_iterator create_table(Str tname);
    friend class const_iterator;
};

inline bool operator<(const Table& a, const Table& b) {
    return a.key() < b.key();
}
inline bool operator==(const Table& a, const Table& b) {
    return a.key() == b.key();
}
inline bool operator>(const Table& a, const Table& b) {
    return a.key() > b.key();
}

inline void Table::iterator::maybe_fix() {
    if (it_ == table_->store_.end() || it_->is_table())
        fix();
}

inline Table::iterator::iterator(Table* table, ServerStore::iterator it)
    : it_(it), table_(table) {
    maybe_fix();
}

inline Datum& Table::iterator::operator*() const {
    return *it_;
}

inline Datum* Table::iterator::operator->() const {
    return it_.operator->();
}

inline bool Table::iterator::operator==(const iterator& x) const {
    return it_ == x.it_;
}

inline bool Table::iterator::operator!=(const iterator& x) const {
    return it_ != x.it_;
}

inline void Table::iterator::operator++() {
    ++it_;
    maybe_fix();
}

inline Str Table::name() const {
    return key();
}

inline Str Table::hashkey() const {
    return key();
}

inline const Table& Datum::table() const {
    return static_cast<const Table&>(*this);
}

inline Table& Datum::table() {
    return static_cast<Table&>(*this);
}

inline auto Table::lbegin() -> local_iterator {
    return store_.begin();
}

inline auto Table::lend() -> local_iterator {
    return store_.end();
}

inline auto Table::lfind(Str key) -> local_iterator {
    return store_.find(key, DatumCompare());
}

inline auto Table::begin() -> iterator {
    return iterator(this, store_.begin());
}

inline auto Table::end() -> iterator {
    return iterator(this, store_.end());
}

inline auto Table::lower_bound_hint(Str str) -> iterator {
    return iterator(this, store_.lower_bound(str, DatumCompare()));
}

inline auto Table::lower_bound(Str str) -> iterator {
    return iterator(this, store_.lower_bound(str, DatumCompare()));
}

inline size_t Table::size() const {
    return store_.size();
}

inline const Datum& Table::operator[](Str key) const {
    auto it = store_.find(key, DatumCompare());
    return it == store_.end() ? Datum::empty_datum : *it;
}

inline size_t Table::count(Str key) const {
    return store_.count(key, DatumCompare());
}

inline Table& Server::table(Str tname) const {
    auto it = supertable_.lfind(tname);
    return it != supertable_.lend() ? it->table() : Table::empty_table;
}

inline Table& Server::table_for(Str key) const {
    return table(table_name(key));
}

inline Table& Server::table_for(Str first, Str last) const {
    return table(table_name(first, last));
}

inline Table& Server::make_table(Str tname) {
    auto it = supertable_.lfind(tname);
    if (unlikely(it == supertable_.lend()))
        it = create_table(tname);
    return it->table();
}

inline Table& Server::make_table_for(Str key) {
    return make_table(table_name(key));
}

inline Table& Server::make_table_for(Str first, Str last) {
    return make_table(table_name(first, last));
}

inline const Datum* Server::find(Str key) const {
    Table& t = table_for(key);
    auto it = t.lfind(key);
    return it != t.lend() ? it.operator->() : nullptr;
}

inline const Datum& Server::operator[](Str key) const {
    return table_for(key)[key];
}

inline size_t Server::count(Str first, Str last) const {
    Table& t = table_for(first);
    return std::distance(t.lower_bound(first), t.lower_bound(last));
}

inline void Table::notify(Datum* d, const String& old_value, SourceRange::notify_type notifier) {
    Str key(d->key());
    for (auto it = source_ranges_.begin_contains(key);
         it != source_ranges_.end(); ) {
        // SourceRange::notify() might remove the SourceRange from the tree
        SourceRange* source = it.operator->();
        ++it;
        if (source->check_match(key))
            source->notify(d, old_value, notifier);
    }
}

inline auto Table::validate(Str key, uint64_t now) -> iterator {
    LocalStr<24> next_key;
    next_key.assign_uninitialized(key.length() + 1);
    memcpy(next_key.mutable_data(), key.data(), key.length());
    next_key.mutable_data()[key.length()] = 0;
    return validate(key, next_key, now);
}

inline void Table::invalidate_dependents(Str key) {
    for (auto it = source_ranges_.begin_contains(key);
         it != source_ranges_.end(); ) {
        // simple, but obviously we could do better
        SourceRange* source = it.operator->();
        ++it;
        source->invalidate();
    }
}

inline void Table::invalidate_dependents(Str first, Str last) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
         it != source_ranges_.end(); ) {
        // simple, but obviously we could do better
        SourceRange* source = it.operator->();
        ++it;
        source->invalidate();
    }
}

inline void Table::unlink_source(SourceRange* r) {
    source_ranges_.erase(*r);
}

template <typename F>
inline void Table::modify(Str key, const SinkRange* sink, const F& func) {
    store_type::insert_commit_data cd;
    std::pair<ServerStore::iterator, bool> p = prepare_modify(key, sink, cd);
    Datum* d = p.second ? NULL : p.first.operator->();
    finish_modify(p, cd, d, key, sink, func(d));
}

inline auto Table::erase(iterator it) -> iterator {
    assert(it.table_ == this);
    Datum* d = it.operator->();
    it.it_ = store_.erase(it.it_);
    it.maybe_fix();
    if (d->owner())
        d->owner()->remove_datum(d);
    String old_value = erase_marker();
    std::swap(d->value(), old_value);
    notify(d, old_value, SourceRange::notify_erase);
    d->invalidate();
    return it;
}

inline void Table::invalidate_erase(Datum* d) {
    store_.erase(store_.iterator_to(*d));
    invalidate_dependents(d->key());
    d->invalidate();
}

inline auto Table::erase_invalid(iterator it) -> iterator {
    Datum* d = it.operator->();
    it.it_ = it.table_->store_.erase(it.it_);
    it.maybe_fix();
    d->invalidate();
    return it;
}

inline bool Table::flush_for_pull(uint64_t now) {
    return all_pull_ && flush_at_ != now && hard_flush_for_pull(now);
}

inline void Server::insert(Str key, const String& value) {
    make_table_for(key).insert(key, value);
}

inline void Server::erase(Str key) {
    auto it = supertable_.lfind(table_name(key));
    if (it != supertable_.lend())
        static_cast<Table&>(*it).erase(key);
}

inline uint64_t Server::next_validate_at() {
    uint64_t now = tstamp();
    now += now <= last_validate_at_;
    return last_validate_at_ = now;
}

inline Table::iterator Server::validate(Str first, Str last) {
    Str tname = table_name(first, last);
    assert(tname);
    return make_table(tname).validate(first, last, next_validate_at());
}

inline Table::iterator Server::validate(Str key) {
    return make_table_for(key).validate(key, next_validate_at());
}

inline Table::iterator Server::begin() {
    return supertable_.begin();
}

inline Table::iterator Server::end() {
    return supertable_.end();
}

} // namespace
#endif
