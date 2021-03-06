#ifndef GSTORE_XINTERVAL_HH
#define GSTORE_XINTERVAL_HH 1
#include "compiler.hh"
#include "straccum.hh"
#include <iostream>

template <typename T>
class interval {
  public:
    typedef T endpoint_type;
    typedef bool (interval<T>::*unspecified_bool_type)() const;

    inline interval();
    inline interval(const T& first, const T& last);
    inline interval(T&& first, T&& last);
    inline interval(const interval<T>& x);
    inline interval(interval<T>&& x);

    inline const T& ibegin() const;
    inline const T& iend() const;

    inline operator unspecified_bool_type() const;
    inline bool operator!() const;
    inline bool empty() const;

    static inline bool contains(const T& first, const T& last, const T& x);
    template <typename X>
    static inline bool contains(const T& first, const T& last,
                                const X& xfirst, const X& xlast);
    template <typename X>
    static inline bool overlaps(const T& first, const T& last,
                                const X& xfirst, const X& xlast);

    inline bool contains(const T& x) const;
    inline bool contains(const interval<T>& x) const;
    template <typename X> inline bool contains(const interval<X>& x) const;
    template <typename X> inline bool contains(const X& first, const X& last) const;
    inline bool overlaps(const interval<T>& x) const;
    template <typename X> inline bool overlaps(const interval<X>& x) const;
    template <typename X> inline bool overlaps(const X& first, const X& last) const;

    inline String unparse() const;

  private:
    T ibegin_;
    T iend_;
};

template <typename T>
struct default_comparator<interval<T> > {
    inline int operator()(const interval<T>& a, const interval<T>& b) const;
};


template <typename T>
inline interval<T>::interval()
    : ibegin_(), iend_() {
}

template <typename T>
inline interval<T>::interval(const T& ibegin, const T& iend)
    : ibegin_(ibegin), iend_(iend < ibegin ? ibegin : iend) {
}

template <typename T>
inline interval<T>::interval(T&& ibegin, T&& iend)
    : ibegin_(std::move(ibegin)),
      iend_(std::move(iend < ibegin ? ibegin : iend)) {
}

template <typename T>
inline interval<T>::interval(const interval<T>& x)
    : ibegin_(x.ibegin()), iend_(x.iend()) {
}

template <typename T>
inline interval<T>::interval(interval<T>&& x)
    : ibegin_(std::move(x.ibegin_)), iend_(std::move(x.iend_)) {
}

template <typename T>
inline interval<T> make_interval(const T& ibegin, const T& iend) {
    return interval<T>(ibegin, iend);
}

template <typename T>
inline const T& interval<T>::ibegin() const {
    return ibegin_;
}

template <typename T>
inline const T& interval<T>::iend() const {
    return iend_;
}

template <typename T>
inline interval<T>::operator unspecified_bool_type() const {
    return ibegin_ == iend_ ? 0 : &interval<T>::empty;
}

template <typename T>
inline bool interval<T>::operator!() const {
    return ibegin_ == iend_;
}

template <typename T>
inline bool interval<T>::empty() const {
    return ibegin_ == iend_;
}

template <typename T>
inline bool interval<T>::contains(const T& first, const T& last, const T& x) {
    return !(x < first) && x < last;
}

template <typename T> template <typename X>
inline bool interval<T>::contains(const T& first, const T& last,
                                  const X& xfirst, const X& xlast) {
    return !(xfirst < first || last < xlast);
}

template <typename T> template <typename X>
inline bool interval<T>::overlaps(const T& first, const T& last,
                                  const X& xfirst, const X& xlast) {
    return xfirst < last && first < xlast;
}

template <typename T>
inline bool interval<T>::contains(const T& x) const {
    return contains(ibegin(), iend(), x);
}

template <typename T>
inline bool interval<T>::contains(const interval<T>& x) const {
    return contains(ibegin(), iend(), x.ibegin(), x.iend());
}

template <typename T> template <typename X>
inline bool interval<T>::contains(const interval<X>& x) const {
    return contains(ibegin(), iend(), x.ibegin(), x.iend());
}

template <typename T> template <typename X>
inline bool interval<T>::contains(const X& first, const X& last) const {
    return contains(ibegin(), iend(), first, last);
}

template <typename T>
inline bool interval<T>::overlaps(const interval<T>& x) const {
    return overlaps(ibegin(), iend(), x.ibegin(), x.iend());
}

template <typename T> template <typename X>
inline bool interval<T>::overlaps(const interval<X>& x) const {
    return overlaps(ibegin(), iend(), x.ibegin(), x.iend());
}

template <typename T> template <typename X>
inline bool interval<T>::overlaps(const X& first, const X& last) const {
    return overlaps(ibegin(), iend(), first, last);
}

template <typename T>
inline bool operator==(const interval<T>& a, const interval<T>& b) {
    return a.ibegin() == b.ibegin() && a.iend() == b.iend();
}

template <typename T>
inline bool operator!=(const interval<T>& a, const interval<T>& b) {
    return !(a.ibegin() == b.ibegin()) || !(a.iend() == b.iend());
}

template <typename T>
inline bool operator<(const interval<T>& a, const interval<T>& b) {
    int cmp = default_comparator<T>()(a.ibegin(), b.ibegin());
    return cmp < 0 || (cmp == 0 && a.iend() < b.iend());
}

template <typename T>
inline bool operator<=(const interval<T>& a, const interval<T>& b) {
    int cmp = default_comparator<T>()(a.ibegin(), b.ibegin());
    return cmp < 0 || (cmp == 0 && !(b.iend() < a.iend()));
}

template <typename T>
inline bool operator>=(const interval<T>& a, const interval<T>& b) {
    int cmp = default_comparator<T>()(a.ibegin(), b.ibegin());
    return cmp > 0 || (cmp == 0 && !(a.iend() < b.iend()));
}

template <typename T>
inline bool operator>(const interval<T>& a, const interval<T>& b) {
    int cmp = default_comparator<T>()(a.ibegin(), b.ibegin());
    return cmp > 0 || (cmp == 0 && b.iend() < a.iend());
}

template <typename T>
inline interval<T> operator&(const interval<T>& a, const interval<T>& b) {
    return interval<T>(a.ibegin() > b.ibegin() ? a.ibegin() : b.ibegin(),
		       a.iend() < b.iend() ? a.iend() : b.iend());
}

template <typename T>
inline interval<T> operator|(const interval<T>& a, const interval<T>& b) {
    return interval<T>(a.ibegin() < b.ibegin() ? a.ibegin() : b.ibegin(),
		       a.iend() > b.iend() ? a.iend() : b.iend());
}

template <typename T>
inline int default_comparator<interval<T> >::operator()(const interval<T>& a,
							const interval<T>& b) const {
    int cmp = default_comparator<T>()(a.ibegin(), b.ibegin());
    return cmp ? cmp : default_comparator<T>()(a.iend(), b.iend());
};

template <typename T>
std::ostream& operator<<(std::ostream& s, const interval<T>& x) {
    return (s << '[' << x.ibegin() << ", " << x.iend() << ')');
}

template <typename T>
StringAccum& operator<<(StringAccum& sa, const interval<T>& x) {
    return (sa << '[' << x.ibegin() << ", " << x.iend() << ')');
}

template <typename T>
String interval<T>::unparse() const {
    StringAccum sa;
    sa << *this;
    return sa.take_string();
}

#endif
