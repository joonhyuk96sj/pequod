#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRangeBase::allocated_key_bytes = 0;
uint64_t SinkRange::invalidate_hit_keys = 0;
uint64_t SinkRange::invalidate_miss_keys = 0;

JoinRange::JoinRange(Str first, Str last, Join* join)
    : ServerRangeBase(first, last), join_(join), flush_at_(0) {
}

JoinRange::~JoinRange() {
    while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
        // XXX no one else had better deref this shit
        delete sink;
}

struct JoinRange::validate_args {
    Str first;
    Str last;
    Match match;
    Server* server;
    SinkRange* sink;
    uint64_t now;
    int notifier;
    int filters;
    Match::state filtermatch;
};

inline void JoinRange::validate_one(Str first, Str last, Server& server,
                                    uint64_t now) {
    validate_args va{first, last, Match(), &server, nullptr, now,
            SourceRange::notify_insert, 0, Match::state()};
    join_->sink().match_range(first, last, va.match);
    va.sink = new SinkRange(this, first, last, va.match, now);
    valid_ranges_.insert(*va.sink);
    validate_step(va, 0);
}

void JoinRange::validate(Str first, Str last, Server& server, uint64_t now) {
    if (!join_->maintained() && !join_->staleness() && flush_at_ != now) {
        join_->sink_table()->flush_for_pull(now);
        while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
            delete sink;        // XXX be careful of refcounting
        flush_at_ = now;
    }

    Str last_valid = first;
    for (auto it = valid_ranges_.begin_overlaps(first, last);
         it != valid_ranges_.end(); ) {
        SinkRange* sink = it.operator->();
        if (sink->has_expired(now)) {
            ++it;
            sink->invalidate();
        } else {
            if (last_valid < sink->ibegin())
                validate_one(last_valid, sink->ibegin(), server, now);
            if (sink->need_update())
                sink->update(first, last, server, now);
            last_valid = sink->iend();
            ++it;
        }
    }
    if (last_valid < last)
        validate_one(last_valid, last, server, now);
}

void JoinRange::validate_filters(validate_args& va) {
    int filters = va.filters;
    Match::state mstate = va.match.save();
    va.match.restore(va.filtermatch);

    for (int jp = 0; filters; ++jp, filters >>= 1)
        if (filters & 1) {
            uint8_t kf[128], kl[128];
            int kflen = join_->expand_first(kf, join_->source(jp),
                                            va.first, va.last, va.match);
            int kllen = join_->expand_last(kl, join_->source(jp),
                                           va.first, va.last, va.match);
            assert(Str(kf, kflen) <= Str(kl, kllen));
            join_->source_table(jp)->validate(Str(kf, kflen), Str(kl, kllen), va.now);
        }

    va.match.restore(mstate);
}

void JoinRange::validate_step(validate_args& va, int joinpos) {
    Table* sourcet = join_->source_table(joinpos);

    uint8_t kf[128], kl[128];
    int kflen = join_->expand_first(kf, join_->source(joinpos),
                                    va.first, va.last, va.match);
    int kllen = join_->expand_last(kl, join_->source(joinpos),
                                   va.first, va.last, va.match);
    assert(Str(kf, kflen) <= Str(kl, kllen));

    SourceRange* r = 0;
    if (joinpos + 1 == join_->nsource())
        r = join_->make_source(*va.server, va.match,
                               Str(kf, kflen), Str(kl, kllen), va.sink);

    if (!join_->maintained() && join_->source_is_filter(joinpos)) {
        if (!va.filters)
            va.filtermatch = va.match.save();
        int known_at_sink = join_->source_mask(join_->nsource() - 1)
            | join_->known_mask(va.filtermatch);
        if ((join_->source_mask(joinpos) & ~known_at_sink) == 0) {
            va.filters |= 1 << joinpos;
            validate_step(va, joinpos + 1);
            va.filters &= ~(1 << joinpos);
            return;
        }
    }

    // need to validate the source ranges in case they have not been
    // expanded yet.
    if (Datum* lower_d = sourcet->validate(Str(kf, kflen), Str(kl, kllen), va.now)) {
        auto it = sourcet->iterator_to(*lower_d);
        auto itend = sourcet->end();

        Match::state mstate(va.match.save());
        const Pattern& pat = join_->source(joinpos);
        ++sourcet->nvalidate_;

        // match not optimizable
        if (!r) {
            for (; it != itend && it->key() < Str(kl, kllen); ++it)
                if (it->key().length() == pat.key_length()) {
                    if (pat.match(it->key(), va.match))
                        validate_step(va, joinpos + 1);
                    va.match.restore(mstate);
                }
        } else if (va.filters) {
            bool filters_validated = false;
            uint8_t filterstr[key_capacity];
            for (; it != itend && it->key() < Str(kl, kllen); ++it)
                if (it->key().length() == pat.key_length()) {
                    if (pat.match(it->key(), va.match)) {
                        if (!filters_validated) {
                            validate_filters(va);
                            filters_validated = true;
                        }
                        int filters = va.filters;
                        for (int jp = 0; filters; ++jp, filters >>= 1)
                            if (filters & 1) {
                                int filterlen = join_->source(jp).expand(filterstr, va.match);
                                if (!join_->source_table(jp)->count(Str(filterstr, filterlen)))
                                    goto give_up;
                            }
                        r->notify(it.operator->(), String(), va.notifier);
                    }
                give_up:
                    va.match.restore(mstate);
                }
        } else {
            for (; it != itend && it->key() < Str(kl, kllen); ++it)
                if (it->key().length() == pat.key_length()) {
                    if (pat.match(it->key(), va.match))
                        r->notify(it.operator->(), String(), va.notifier);
                    va.match.restore(mstate);
                }
        }
    }

    if (join_->maintained() && va.notifier == SourceRange::notify_erase) {
        if (r) {
            LocalStr<12> remove_context;
            join_->make_context(remove_context, va.match, join_->context_mask(joinpos) & ~va.sink->context_mask());
            va.server->remove_source(r->ibegin(), r->iend(), va.sink, remove_context);
            delete r;
        }
    } else if (join_->maintained()) {
        if (r)
            va.server->add_source(r);
        else {
            SourceRange::parameters p{*va.server, join_, joinpos, va.match,
                    Str(kf, kflen), Str(kl, kllen), va.sink};
            va.server->add_source(new InvalidatorRange(p));
        }
    } else if (r)
        delete r;
}

#if 0
std::ostream& operator<<(std::ostream& stream, const ServerRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << ")";
    if (r.type_ == ServerRange::joinsink)
        stream << ": joinsink @" << (void*) r.join_;
    else if (r.type_ == ServerRange::validjoin) {
        stream << ": validjoin @" << (void*) r.join_ << ", expires: ";
        if (r.expires_at_) {
            uint64_t now = tstamp();
            if (r.expired_at(now))
                stream << "EXPIRED";
            else
                stream << "in " << fromus(r.expires_at_ - now) << " seconds";
        }
        else
            stream << "NEVER";
    }
    else
	stream << ": ??";
    return stream << "}";
}
#endif

IntermediateUpdate::IntermediateUpdate(Str first, Str last,
                                       SinkRange* sink, int joinpos, const Match& m,
                                       int notifier)
    : ServerRangeBase(first, last), joinpos_(joinpos), notifier_(notifier) {
    Join* j = sink->join();
    unsigned context_mask = (j->context_mask(joinpos) | j->source_mask(joinpos)) & ~sink->context_mask();
    j->make_context(context_, m, context_mask);
}

SinkRange::SinkRange(JoinRange* jr, Str first, Str last, const Match& m, uint64_t now)
    : ServerRangeBase(first, last), jr_(jr), refcount_(0), hint_{nullptr},
      data_free_(uintptr_t(-1)) {
    Join* j = jr_->join();
    if (j->maintained())
        expires_at_ = 0;
    else
        expires_at_ = now + j->staleness();

    context_mask_ = j->known_mask(m);
    j->make_context(context_, m, context_mask_);
}

SinkRange::~SinkRange() {
    while (IntermediateUpdate* iu = updates_.unlink_leftmost_without_rebalance())
        delete iu;
    if (hint_)
        hint_->deref();
}

void SinkRange::add_update(int joinpos, Str context, Str key, int notifier) {
    Match m;
    join()->source(joinpos).match(key, m);
    join()->assign_context(m, context);
    uint8_t kf[key_capacity], kl[key_capacity];
    int kflen = join()->expand_first(kf, join()->sink(), ibegin(), iend(), m);
    int kllen = join()->expand_last(kl, join()->sink(), ibegin(), iend(), m);

    IntermediateUpdate* iu = new IntermediateUpdate
        (Str(kf, kflen), Str(kl, kllen), this, joinpos, m, notifier);
    updates_.insert(*iu);

    join()->sink_table()->invalidate_dependents(Str(kf, kflen), Str(kl, kllen));
    // std::cerr << *iu << "\n";
}

bool SinkRange::update_iu(Str first, Str last, IntermediateUpdate* iu,
                          Server& server, uint64_t now) {
    if (first < iu->ibegin())
        first = iu->ibegin();
    if (iu->iend() < last)
        last = iu->iend();
    if (first != iu->ibegin() && last != iu->iend())
        // XXX embiggening range
        last = iu->iend();

    Join* join = jr_->join();
    JoinRange::validate_args va{first, last, Match(), &server, this, now,
            iu->notifier_, 0, Match::state()};
    join->assign_context(va.match, context_);
    join->assign_context(va.match, iu->context_);
    jr_->validate_step(va, iu->joinpos_ + 1);

    if (first == iu->ibegin())
        iu->ibegin_ = last;
    if (last == iu->iend())
        iu->iend_ = first;
    return iu->ibegin_ < iu->iend_;
}

void SinkRange::update(Str first, Str last, Server& server, uint64_t now) {
    for (auto it = updates_.begin_overlaps(first, last);
         it != updates_.end(); ) {
        IntermediateUpdate* iu = it.operator->();
        ++it;

        updates_.erase(*iu);
        if (update_iu(first, last, iu, server, now))
            updates_.insert(*iu);
        else
            delete iu;
    }
}

void SinkRange::invalidate() {
    if (valid()) {
        jr_->valid_ranges_.erase(*this);

        while (data_free_ != uintptr_t(-1)) {
            uintptr_t pos = data_free_;
            data_free_ = (uintptr_t) data_[pos];
            data_[pos] = 0;
        }

        Table* t = table();
        for (auto d : data_)
            if (d) {
                t->invalidate_erase(d);
                ++invalidate_hit_keys;
            }

        ibegin_ = Str();
        if (refcount_ == 0)
            delete this;
    }
}

std::ostream& operator<<(std::ostream& stream, const IntermediateUpdate& iu) {
    stream << "UPDATE{" << iu.interval() << " "
           << (iu.notifier() > 0 ? "+" : "-")
           << iu.context() << " " << iu.context_ << "}";
    return stream;
}

std::ostream& operator<<(std::ostream& stream, const SinkRange& sink) {
    if (sink.valid())
        stream << "SINK{" << sink.interval().unparse().printable() << "}";
    else
        stream << "SINK{INVALID}";
    return stream;
}

} // namespace pq
