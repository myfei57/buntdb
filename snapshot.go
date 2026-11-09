package buntdb

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/match"
)

var (
	// ErrSnapshotClosed is returned when an operation is performed on a
	// snapshot that has already been closed with Snapshot.Close().
	ErrSnapshotClosed = errors.New("snapshot closed")

	// ErrSpatialIndexNotSupported is returned when a spatial index query
	// such as Intersects or Nearby is attempted on a read-only snapshot.
	// Snapshots only capture ordered (b-tree) indexes.
	ErrSpatialIndexNotSupported = errors.New(
		"snapshot: spatial index queries are not supported")
)

// snapshotIndex is an immutable, self-contained copy of an index definition
// captured at snapshot creation time. It never references the live database
// index, therefore replacing or dropping the live index cannot affect the
// snapshot. The btr field is nil for spatial (r-tree) indexes, which only
// need to be remembered so that spatial queries can be explicitly rejected
// instead of being silently reported as missing.
type snapshotIndex struct {
	less func(a, b string) bool // ordering function, nil on spatial indexes
	btr  *btree.BTree           // independent ordered tree, nil on spatial
}

// Snapshot is an immutable, read-only capture of a database at a single
// committed point in time. A snapshot is created with DB.Snapshot and must be
// released with Close when it is no longer needed.
//
// The creation of a snapshot briefly takes the database read lock in order to
// observe one consistent committed state. Once Snapshot returns, the lock is
// released and every subsequent operation is served from the snapshot alone;
// the live database is never touched again. In particular, long running
// scans and slow iterators do not block database writers.
//
// A snapshot owns independent b-tree structures that share the immutable
// *dbItem record pointers with the database and with each other. Key and
// value strings are not copied, and a record captured by both the primary key
// tree and several indexes exists only once. A database Set never mutates a
// record in place but installs a brand new *dbItem, so later writes can never
// reach a record that a snapshot holds.
//
// Expiration is evaluated against the fixed capture time. Items that were
// already expired at that time are invisible for the whole life of the
// snapshot, and the passage of real time never changes the snapshot content
// or makes a visible item disappear.
//
// Closing the database does not close its snapshots: they remain fully
// functional until their own Close method is called. Snapshot.Close is
// idempotent and blocks until all in-flight snapshot reads have finished, so
// records can never be cleared out from under an iterator callback.
type Snapshot struct {
	mu     sync.RWMutex // guards closed and the fields below
	closed bool
	now    time.Time                 // the single capture time
	keys   *btree.BTree              // independent primary key tree
	idxs   map[string]*snapshotIndex // independent index definitions/trees
	n      int                       // number of items visible at capture
}

// expiredAt reports whether the item was expired at the specified time. The
// boundary mirrors dbItem.expired: an item is expired only when now is
// strictly after its absolute expiration time.
func (dbi *dbItem) expiredAt(now time.Time) bool {
	return dbi.opts != nil && dbi.opts.ex && now.After(dbi.opts.exat)
}

// Snapshot creates a new read-only snapshot of the database. The call briefly
// acquires the database read lock so that the captured keys, values, absolute
// expiration times and ordered index definitions all belong to the same
// committed point in time. It returns ErrDatabaseClosed when the database has
// already been closed.
func (db *DB) Snapshot() (*Snapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	return db.snapshotLocked(time.Now()), nil
}

// snapshotLocked builds the immutable snapshot from a database that is
// already protected by db.mu (read or write lock). The capture time is passed
// in so that expiration visibility is evaluated against a single instant.
func (db *DB) snapshotLocked(now time.Time) *Snapshot {
	snap := &Snapshot{
		now:  now,
		keys: btreeNew(lessCtx(nil)),
		idxs: make(map[string]*snapshotIndex, len(db.idxs)),
	}
	// Build an independent primary key tree. Only the tree structure is
	// copied; the immutable *dbItem records (and their strings) are shared.
	btreeAscend(db.keys, func(item interface{}) bool {
		dbi := item.(*dbItem)
		snap.keys.Load(dbi)
		if !dbi.expiredAt(now) {
			snap.n++
		}
		return true
	})
	// Capture every index as a self-contained definition with its own tree.
	// The live *index objects, their closures and the database itself are
	// never referenced, so index replacement/drop and DeleteAll cannot reach
	// the snapshot. Spatial indexes are recorded without a tree so that
	// spatial queries can be explicitly rejected.
	for name, idx := range db.idxs {
		sidx := &snapshotIndex{less: idx.less}
		if idx.btr != nil {
			sidx.btr = btreeNew(lessCtx(sidx))
			btreeAscend(idx.btr, func(item interface{}) bool {
				// Bulk loading in ascending order; the same *dbItem pointer
				// is shared with the snapshot key tree and with all indexes.
				sidx.btr.Load(item.(*dbItem))
				return true
			})
		}
		snap.idxs[name] = sidx
	}
	return snap
}

// beginRead acquires the snapshot for reading. It returns ErrSnapshotClosed
// when the snapshot has been closed. The caller must call snap.mu.RUnlock
// when the read is finished.
func (snap *Snapshot) beginRead() error {
	snap.mu.RLock()
	if snap.closed {
		snap.mu.RUnlock()
		return ErrSnapshotClosed
	}
	return nil
}

// Close releases all structures held by the snapshot. It is idempotent:
// closing an already closed snapshot is a no-op and returns nil. Close waits
// for reads that are currently in progress to finish before dropping the
// internal references, therefore iterator callbacks always observe valid
// records. Operations started after Close fail with ErrSnapshotClosed.
func (snap *Snapshot) Close() error {
	snap.mu.Lock()
	defer snap.mu.Unlock()
	if snap.closed {
		return nil
	}
	snap.closed = true
	snap.keys = nil
	snap.idxs = nil
	snap.n = 0
	return nil
}

// Get returns the value for a key as captured by the snapshot. An item that
// did not exist or that was already expired at the capture time returns
// ErrNotFound. Passing ignoreExpired=true returns the value even when the
// captured item was already expired at the capture time.
func (snap *Snapshot) Get(key string,
	ignoreExpired ...bool) (val string, err error) {
	if err := snap.beginRead(); err != nil {
		return "", err
	}
	defer snap.mu.RUnlock()
	var ignore bool
	if len(ignoreExpired) != 0 {
		ignore = ignoreExpired[0]
	}
	item := snap.keys.Get(&dbItem{key: key})
	if item == nil {
		return "", ErrNotFound
	}
	dbi := item.(*dbItem)
	if dbi.expiredAt(snap.now) && !ignore {
		return "", ErrNotFound
	}
	return dbi.val, nil
}

// TTL returns the remaining time-to-live of an item relative to the snapshot
// capture time. The value is frozen: real time passing after the snapshot was
// created never changes it. A duration of -1 is returned for items without an
// expiration, preserving the existing negative-value convention. An item that
// was missing or already expired at the capture time returns ErrNotFound.
func (snap *Snapshot) TTL(key string) (time.Duration, error) {
	if err := snap.beginRead(); err != nil {
		return 0, err
	}
	defer snap.mu.RUnlock()
	item := snap.keys.Get(&dbItem{key: key})
	if item == nil {
		return 0, ErrNotFound
	}
	dbi := item.(*dbItem)
	if dbi.opts == nil || !dbi.opts.ex {
		return -1, nil
	}
	dur := dbi.opts.exat.Sub(snap.now)
	if dur < 0 {
		// expired at the capture time: invisible, mirroring Tx.TTL
		return 0, ErrNotFound
	}
	return dur, nil
}

// Len returns the number of items visible in the snapshot. Items that were
// already expired at the capture time are not counted. The value is frozen
// and cannot change while the snapshot is open.
func (snap *Snapshot) Len() (int, error) {
	if err := snap.beginRead(); err != nil {
		return 0, err
	}
	defer snap.mu.RUnlock()
	return snap.n, nil
}

// Indexes returns a sorted list of the index names captured by the snapshot,
// including spatial indexes.
func (snap *Snapshot) Indexes() ([]string, error) {
	if err := snap.beginRead(); err != nil {
		return nil, err
	}
	defer snap.mu.RUnlock()
	names := make([]string, 0, len(snap.idxs))
	for name := range snap.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// GetLess returns the ordering function captured for an ordered index.
// Unknown indexes return ErrNotFound while spatial indexes return
// ErrSpatialIndexNotSupported.
func (snap *Snapshot) GetLess(index string) (func(a, b string) bool, error) {
	if err := snap.beginRead(); err != nil {
		return nil, err
	}
	defer snap.mu.RUnlock()
	sidx, ok := snap.idxs[index]
	if !ok {
		return nil, ErrNotFound
	}
	if sidx.less == nil {
		return nil, ErrSpatialIndexNotSupported
	}
	return sidx.less, nil
}

// scan iterates through a captured index using exactly the same limit and
// ordering rules as Tx.scan, but without ever touching the live database.
func (snap *Snapshot) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key, value string) bool) error {
	if err := snap.beginRead(); err != nil {
		return err
	}
	defer snap.mu.RUnlock()
	// freeze visibility against the single capture time
	iter := func(item interface{}) bool {
		dbi := item.(*dbItem)
		if dbi.expiredAt(snap.now) {
			return true
		}
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means the primary key tree
		tr = snap.keys
	} else {
		sidx, ok := snap.idxs[index]
		if !ok {
			return ErrNotFound
		}
		if sidx.btr == nil {
			// the name belongs to a spatial index, which snapshots do not
			// capture for querying. Reject explicitly.
			return ErrSpatialIndexNotSupported
		}
		tr = sidx.btr
	}
	// create the limit items, mirroring Tx.scan
	var itemA, itemB *dbItem
	if gt || lt {
		if index == "" {
			itemA = &dbItem{key: start}
			itemB = &dbItem{key: stop}
		} else {
			itemA = &dbItem{val: start}
			itemB = &dbItem{val: stop}
			if desc {
				itemA.keyless = true
				itemB.keyless = true
			}
		}
	}
	if desc {
		switch {
		case gt && lt:
			btreeDescendRange(tr, itemA, itemB, iter)
		case gt:
			btreeDescendGreaterThan(tr, itemA, iter)
		case lt:
			btreeDescendLessOrEqual(tr, itemA, iter)
		default:
			btreeDescend(tr, iter)
		}
	} else {
		switch {
		case gt && lt:
			btreeAscendRange(tr, itemA, itemB, iter)
		case gt:
			btreeAscendGreaterOrEqual(tr, itemA, iter)
		case lt:
			btreeAscendLessThan(tr, itemA, iter)
		default:
			btreeAscend(tr, iter)
		}
	}
	return nil
}

// Ascend calls the iterator for every visible item in the snapshot until the
// iterator returns false. An empty index scans the primary key tree, any
// other name scans the captured ordered index.
func (snap *Snapshot) Ascend(index string,
	iterator func(key, value string) bool) error {
	return snap.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every visible item in the range
// [pivot, last].
func (snap *Snapshot) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return snap.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every visible item in the range
// [first, pivot).
func (snap *Snapshot) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return snap.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every visible item in the range
// [greaterOrEqual, lessThan).
func (snap *Snapshot) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return snap.scan(false, true, true, index, greaterOrEqual, lessThan,
		iterator)
}

// Descend calls the iterator for every visible item in descending order until
// the iterator returns false.
func (snap *Snapshot) Descend(index string,
	iterator func(key, value string) bool) error {
	return snap.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every visible item in the range
// [last, pivot).
func (snap *Snapshot) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return snap.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every visible item in the range
// [pivot, first].
func (snap *Snapshot) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return snap.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every visible item in the range
// [lessOrEqual, greaterThan) in descending order.
func (snap *Snapshot) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return snap.scan(true, true, true, index, lessOrEqual, greaterThan,
		iterator)
}

// AscendEqual calls the iterator for every visible item that equals pivot
// according to the captured index ordering (or the key when no index is
// given).
func (snap *Snapshot) AscendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var less func(a, b string) bool
	if index != "" {
		var err error
		less, err = snap.GetLess(index)
		if err != nil {
			return err
		}
	}
	return snap.AscendGreaterOrEqual(index, pivot,
		func(key, value string) bool {
			if less == nil {
				if key != pivot {
					return false
				}
			} else if less(pivot, value) {
				return false
			}
			return iterator(key, value)
		})
}

// DescendEqual calls the iterator for every visible item that equals pivot
// according to the captured index ordering (or the key when no index is
// given), scanning in descending order.
func (snap *Snapshot) DescendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var less func(a, b string) bool
	if index != "" {
		var err error
		less, err = snap.GetLess(index)
		if err != nil {
			return err
		}
	}
	return snap.DescendLessOrEqual(index, pivot,
		func(key, value string) bool {
			if less == nil {
				if key != pivot {
					return false
				}
			} else if less(value, pivot) {
				return false
			}
			return iterator(key, value)
		})
}

// AscendKeys allows for iterating through keys based on the specified pattern
// in ascending order.
func (snap *Snapshot) AscendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return snap.Ascend("", iterator)
		}
		return snap.Ascend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return snap.AscendGreaterOrEqual("", min, func(key, value string) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern
// in descending order.
func (snap *Snapshot) DescendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return snap.Descend("", iterator)
		}
		return snap.Descend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return snap.DescendLessOrEqual("", max, func(key, value string) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Intersects is not supported on snapshots because only ordered indexes are
// captured. It always returns ErrSpatialIndexNotSupported for a known index
// and ErrNotFound for an unknown one.
func (snap *Snapshot) Intersects(index, bounds string,
	iterator func(key, value string) bool) error {
	if err := snap.beginRead(); err != nil {
		return err
	}
	defer snap.mu.RUnlock()
	if _, ok := snap.idxs[index]; !ok {
		return ErrNotFound
	}
	return ErrSpatialIndexNotSupported
}

// Nearby is not supported on snapshots because only ordered indexes are
// captured. It always returns ErrSpatialIndexNotSupported for a known index
// and ErrNotFound for an unknown one.
func (snap *Snapshot) Nearby(index, bounds string,
	iterator func(key, value string, dist float64) bool) error {
	if err := snap.beginRead(); err != nil {
		return err
	}
	defer snap.mu.RUnlock()
	if _, ok := snap.idxs[index]; !ok {
		return ErrNotFound
	}
	return ErrSpatialIndexNotSupported
}
