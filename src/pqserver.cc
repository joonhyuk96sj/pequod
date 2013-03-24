#include <unistd.h>
#include <set>
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "error.hh"
#include <sys/resource.h>

namespace pq {

const Datum Datum::empty_datum{Str()};
const Datum Datum::max_datum(Str("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"));
Table Table::empty_table{Str(), nullptr, nullptr};
const char Datum::table_marker[] = "TABLE MARKER";

void Table::iterator::fix() {
 retry:
    if (it_ == table_->store_.end()) {
        if (table_->parent_) {
            it_ = table_->parent_->store_.iterator_to(*table_);
            table_ = table_->parent_;
            ++it_;
            goto retry;
        }
    } else if (it_->is_table()) {
        table_ = &it_->table();
        it_ = table_->store_.begin();
        goto retry;
    }
}

Table::Table(Str name, Table* parent, Server* server)
    : Datum(name, String::make_stable(Datum::table_marker)),
      ninsert_(0), nmodify_(0), nmodify_nohint_(0), nerase_(0), nvalidate_(0),
      njoins_(0), flush_at_(0), all_pull_(true),
      server_{server}, parent_{parent} {
}

Table::~Table() {
    while (SourceRange* r = source_ranges_.unlink_leftmost_without_rebalance()) {
        r->clear_without_deref();
        delete r;
    }
    while (JoinRange* r = join_ranges_.unlink_leftmost_without_rebalance())
        delete r;
    // delete store last since join_ranges_ have refs to Datums
    while (Datum* d = store_.unlink_leftmost_without_rebalance()) {
        if (d->is_table())
            delete &d->table();
        else
            delete d;
    }
}

void Table::add_source(SourceRange* r) {
    for (auto it = source_ranges_.begin_contains(r->interval());
	 it != source_ranges_.end(); ++it)
	if (it->join() == r->join() && it->joinpos() == r->joinpos()) {
	    // XXX may copy too much. This will not actually cause visible
	    // bugs I think?, but will grow the store
	    it->take_results(*r);
            delete r;
	    return;
	}
    source_ranges_.insert(*r);
}

void Table::remove_source(Str first, Str last, SinkRange* sink, Str context) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
	 it != source_ranges_.end(); ) {
        SourceRange* source = it.operator->();
        ++it;
	if (source->join() == sink->join())
            source->remove_sink(sink, context);
    }
}

void Table::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    FileErrorHandler xerrh(stderr);
    errh = errh ? errh : &xerrh;

    // check for redundant join
    for (auto it = join_ranges_.begin_overlaps(first, last);
         it != join_ranges_.end(); ++it)
        if (it->join()->same_structure(*join)) {
            errh->error("join on [%p{Str}, %p{Str}) has same structure as overlapping join\n(new join ignored)", &first, &last);
            return;
        }

    join_ranges_.insert(*new JoinRange(first, last, join));
    if (join->maintained() || join->staleness())
        all_pull_ = false;
    ++njoins_;
}

void Server::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    join->attach(*this);
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_join(first, last, join, errh);
}

auto Table::insert(Table& t) -> local_iterator {
    // XXX assert?
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(t.name(), DatumCompare(), cd);
    assert(p.second);
    return store_.insert_commit(t, cd);
}

void Table::insert(Str key, String value) {
    assert(name());
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
        value = String();
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
        d->value().swap(value);
    }
    notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    ++ninsert_;
    all_pull_ = false;
}

std::pair<ServerStore::iterator, bool> Table::prepare_modify(Str key, const SinkRange* sink, ServerStore::insert_commit_data& cd) {
    assert(name() && sink);
    std::pair<ServerStore::iterator, bool> p;
    Datum* hint = sink->hint();
    if (!hint || !hint->valid()) {
        ++nmodify_nohint_;
        p = store_.insert_check(key, DatumCompare(), cd);
    } else {
        p.first = store_.iterator_to(*hint);
        if (hint->key() == key)
            p.second = false;
        else {
            ++p.first;
            p = store_.insert_check(p.first, key, DatumCompare(), cd);
        }
    }
    return p;
}

void Table::finish_modify(std::pair<ServerStore::iterator, bool> p,
                          const store_type::insert_commit_data& cd,
                          Datum* d, Str key, const SinkRange* sink,
                          String value) {
    SourceRange::notify_type n = SourceRange::notify_update;
    if (!is_marker(value)) {
        if (p.second) {
            d = new Datum(key, sink);
            sink->add_datum(d);
            p.first = store_.insert_commit(*d, cd);
            n = SourceRange::notify_insert;
        }
    } else if (is_erase_marker(value)) {
        if (!p.second) {
            p.first = store_.erase(p.first);
            n = SourceRange::notify_erase;
        } else
            goto done;
    } else if (is_invalidate_marker(value)) {
        invalidate_dependents(d->key());
        const_cast<SinkRange*>(sink)->add_invalidate(key);
        goto done;
    } else
        goto done;

    d->value().swap(value);
    notify(d, value, n);
    if (n == SourceRange::notify_erase)
        d->invalidate();

 done:
    sink->update_hint(store_, p.first);
    ++nmodify_;
}

auto Table::validate(Str first, Str last, uint64_t now) -> iterator {
    if (njoins_ != 0) {
        if (njoins_ == 1) {
            auto it = store_.lower_bound(first, DatumCompare());
            auto itx = it;
            if ((itx == store_.end() || itx->key() >= last) && itx != store_.begin())
                --itx;
            if (itx != store_.end() && itx->key() < last && itx->owner()
                && itx->owner()->valid() && !itx->owner()->has_expired(now)
                && itx->owner()->ibegin() <= first && last <= itx->owner()->iend()
                && !itx->owner()->need_update())
                return iterator(this, it);
        }
        for (auto it = join_ranges_.begin_overlaps(first, last);
             it != join_ranges_.end(); ++it)
            it->validate(first, last, *server_, now);
    }
    return iterator(this, store_.lower_bound(first, DatumCompare()));
}

bool Table::hard_flush_for_pull(uint64_t now) {
    while (Datum* d = store_.unlink_leftmost_without_rebalance()) {
        assert(!d->is_table());
        invalidate_dependents(d->key());
        d->invalidate();
    }
    flush_at_ = now;
    return true;
}

void Table::erase(Str key) {
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end())
        erase(iterator(this, it));
    ++nerase_;
}

Server::Server()
    : supertable_(Str(), nullptr, this), last_validate_at_(0) {
}

auto Server::create_table(Str tname) -> Table::local_iterator {
    assert(tname);
    Table* t = new Table(tname, &supertable_, this);
    return supertable_.insert(*t);
}

size_t Server::validate_count(Str first, Str last) {
    Table& t = make_table(table_name(first));
    auto it = validate(first, last);
    auto itend = t.end();
    size_t n = 0;
    for (; it != itend && it->key() < last; ++it)
        ++n;
    return n;
}

void Table::add_stats(Json& j) const {
    j["ninsert"] += ninsert_;
    j["nmodify"] += nmodify_;
    j["nmodify_nohint"] += nmodify_nohint_;
    j["nerase"] += nerase_;
    j["store_size"] += store_.size();
    j["source_ranges_size"] += source_ranges_.size();
    for (auto& jr : join_ranges_)
        j["sink_ranges_size"] += jr.valid_ranges_size();
    j["nvalidate"] += nvalidate_;
}

Json Server::stats() const {
    size_t store_size = 0, source_ranges_size = 0, join_ranges_size = 0,
        sink_ranges_size = 0;
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);

    Json tables = Json::make_array();
    for (auto it = supertable_.lbegin(); it != supertable_.lend(); ++it) {
        assert(it->is_table());
        Table& t = it->table();
        Json j = Json().set("name", t.name());
        t.add_stats(j);
        for (auto it = j.obegin(); it != j.oend(); )
            if (it->second.is_i() && !it->second.as_i())
                it = j.erase(it);
            else
                ++it;
        tables.push_back(j);

        store_size += j["store_size"].to_i();
        source_ranges_size += j["source_ranges_size"].to_i();
        join_ranges_size += t.join_ranges_.size();
        sink_ranges_size += j["sink_ranges_size"].to_i();
    }

    Json answer;
    answer.set("store_size", store_size)
	.set("source_ranges_size", source_ranges_size)
	.set("join_ranges_size", join_ranges_size)
	.set("valid_ranges_size", sink_ranges_size)
        .set("server_user_time", to_real(ru.ru_utime))
        .set("server_system_time", to_real(ru.ru_stime))
        .set("server_max_rss_mb", ru.ru_maxrss / 1024);
    if (SourceRange::allocated_key_bytes)
        answer.set("source_allocated_key_bytes", SourceRange::allocated_key_bytes);
    if (ServerRangeBase::allocated_key_bytes)
        answer.set("sink_allocated_key_bytes", ServerRangeBase::allocated_key_bytes);
    if (SinkRange::invalidate_hit_keys)
        answer.set("invalidate_hits", SinkRange::invalidate_hit_keys);
    if (SinkRange::invalidate_miss_keys)
        answer.set("invalidate_misses", SinkRange::invalidate_miss_keys);
    return answer.set("tables", tables);
}

void Table::print_sources(std::ostream& stream) const {
    stream << source_ranges_;
}

void Server::print(std::ostream& stream) {
    stream << "sources:" << std::endl;
    bool any = false;
    for (auto it = supertable_.lbegin(); it != supertable_.lend(); ++it) {
        assert(it->is_table());
        Table& t = it->table();
        if (!t.source_ranges_.empty()) {
            stream << t.source_ranges_;
            any = true;
        }
    }
    if (!any)
        stream << "<empty>\n";
}

} // namespace
