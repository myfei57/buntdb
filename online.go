package buntdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// DefaultOnlineIndexChangeBudget is the default maximum number of committed
// key changes that may accumulate while an online index build is catching up.
// A committed DeleteAll counts as one change.
const DefaultOnlineIndexChangeBudget = 100000

// OnlineIndexOptions are the options used for creating an online index.
// Online indexes are ordinary ordered (b-tree) indexes; spatial indexes and
// uniqueness constraints are not supported, and the index definition itself
// is never persisted to the append-only file.
type OnlineIndexOptions struct {
	// IndexOptions carries the standard index options.
	IndexOptions

	// MaxPendingChanges bounds the number of committed key changes (a
	// DeleteAll commit counts as one change as well) that are allowed to
	// accumulate between catch-up exchanges. A value of zero or less falls
	// back to DefaultOnlineIndexChangeBudget. When the budget is exceeded
	// the build fails with ErrIndexBuildBudget and releases all of its
	// resources instead of growing the pending change log without bound.
	// Normal commits never wait on a slow build.
	MaxPendingChanges int
}

// IndexBuild is a handle on an asynchronous online index build.
type IndexBuild struct {
	db *DB
	b  *indexBuild
}

// Name returns the name of the index being built.
func (h *IndexBuild) Name() string {
	return h.b.name
}

// Wait blocks until the build reaches its terminal state and returns the
// build error, or nil once the index has been published.
//
// Canceling ctx ends this particular wait only; the build keeps running.
// Only an explicit Cancel can stop a build.
func (h *IndexBuild) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Prefer the terminal state when the build has already finished even if
	// the context is canceled as well.
	select {
	case <-h.b.doneCh:
		return h.b.err
	default:
	}
	select {
	case <-h.b.doneCh:
		return h.b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel explicitly asks the build to stop. It blocks until the build has
// reached its terminal state and released the reserved index name.
// Canceling an already finished build returns that build's terminal error.
func (h *IndexBuild) Cancel() error {
	h.b.requestCancel(ErrIndexBuildCanceled)
	<-h.b.doneCh
	return h.b.err
}

// indexBuild is the internal state of a single online index build. Every
// build runs in exactly one background goroutine and owns a private index
// tree that is not visible to queries until publication.
type indexBuild struct {
	db         *DB
	name       string
	idx        *index // private, partially populated index
	maxPending int

	mu   sync.Mutex
	cond *sync.Cond

	// terminal state and stop requests, all guarded by mu.
	done       bool
	err        error
	canceled   bool
	cancelErr  error
	closing    bool
	overBudget bool

	// pending committed changes that have not yet been applied to the
	// private index. A nil *dbItem means the key was deleted. Coalescing by
	// key collapses consecutive overwrites to the last committed state.
	pending      map[string]*dbItem
	pendingFlush bool // a committed DeleteAll requires a full index reset
	pendingCount int  // committed changes since the last exchange
	queuedSeq    uint64

	doneCh chan struct{}
}

// CreateIndexOnline starts building an ordinary ordered (b-tree) secondary
// index in the background. Existing reads and writes keep working while the
// initial backfill, ordering and change catch-up are in progress.
//
// The build identity is registered and a consistent record snapshot is
// captured at the same protected point. The capture may briefly hold the
// database lock, but the user supplied comparator, backfill, ordering and
// catch-up never hold the global write lock for long.
//
// Only successfully committed key changes are fed to the build: rolled back
// transactions and failed AOF writes can never appear in the result. The
// index is installed atomically once the build has verified that no
// committed change was missed, so queries never observe a partial index.
//
// The index name is reserved for the duration of the build; any other
// create or replace with the same name fails with ErrIndexBuilding. Spatial
// indexes, uniqueness constraints and persisted function definitions are not
// supported.
func (db *DB) CreateIndexOnline(name, pattern string,
	opts *OnlineIndexOptions, less ...func(a, b string) bool) (*IndexBuild, error) {
	if name == "" {
		// an empty name index is designated for the main "keys" tree.
		return nil, ErrIndexExists
	}
	var sopts IndexOptions
	maxPending := DefaultOnlineIndexChangeBudget
	if opts != nil {
		sopts = opts.IndexOptions
		if opts.MaxPendingChanges > 0 {
			maxPending = opts.MaxPendingChanges
		}
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	lessFn := resolveLess(less)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if _, ok := db.idxs[name]; ok {
		return nil, ErrIndexExists
	}
	if _, ok := db.onlineBuilds[name]; ok {
		return nil, ErrIndexBuilding
	}
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    lessFn,
		db:      db,
		opts:    sopts,
	}
	idx.btr = btreeNew(lessCtx(idx))
	b := &indexBuild{
		db:         db,
		name:       name,
		idx:        idx,
		maxPending: maxPending,
		pending:    make(map[string]*dbItem),
		queuedSeq:  db.commitSeq,
		doneCh:     make(chan struct{}),
	}
	b.cond = sync.NewCond(&b.mu)
	// Capture a consistent snapshot and register the build identity at the
	// same protected point. Only item pointers are copied; dbItems are
	// immutable once inserted, so the snapshot stays valid while backfill
	// runs later without holding the database lock.
	snapshot := make([]*dbItem, 0, db.keys.Len())
	btreeAscend(db.keys, func(item interface{}) bool {
		snapshot = append(snapshot, item.(*dbItem))
		return true
	})
	db.onlineBuilds[name] = b
	go b.run(snapshot)
	return &IndexBuild{db: db, b: b}, nil
}

// run is the single entry point of the build goroutine. It funnels every
// outcome through finalize so that Cancel, Close and completion race into
// exactly one terminal state.
func (b *indexBuild) run(snapshot []*dbItem) {
	err := b.build(snapshot)
	b.finalize(err, err == nil)
}

// build executes the backfill, catch-up and publish phases. A panic raised
// by the user supplied comparator is converted into a build error, so a
// background goroutine can never take the process down.
func (b *indexBuild) build(snapshot []*dbItem) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("buntdb: online index comparator panic: %v", r)
		}
	}()
	idx := b.idx
	// Phase 1: backfill the consistent snapshot entirely outside of the
	// database write lock. Each b-tree Set maintains ordering and invokes
	// the user comparator here, never while the global write lock is held.
	for i, dbi := range snapshot {
		if i&0xff == 0 {
			if err = b.checkStop(); err != nil {
				return err
			}
		}
		if idx.match(dbi.key) {
			idx.btr.Set(dbi)
		}
	}
	// Phases 2/3: repeatedly exchange limited incremental batches, apply
	// them outside the lock and atomically publish once fully caught up.
	for {
		if err = b.checkStop(); err != nil {
			return err
		}
		batch, flush, seq := b.drainPending()
		if flush {
			// A committed DeleteAll invalidated every previously built item.
			idx.btr = btreeNew(lessCtx(idx))
		}
		for key, item := range batch {
			// Remove the previous physical item for the key, if any. The
			// b-tree falls back to key comparison for deletion, which also
			// covers items whose indexed value changed.
			idx.btr.Delete(&dbItem{key: key})
			if item != nil && idx.match(key) {
				idx.btr.Set(item)
			}
		}
		published, err := b.tryPublish(seq)
		if err != nil {
			return err
		}
		if published {
			return nil
		}
		b.waitForWork()
	}
}

// tryPublish atomically verifies that no committed change was missed and,
// when fully caught up, installs the complete index into the queryable set.
// The verification and publication both happen under db.mu, so readers can
// never observe a partially built index.
func (b *indexBuild) tryPublish(seq uint64) (bool, error) {
	db := b.db
	db.mu.Lock()
	defer db.mu.Unlock()
	// Lock ordering is always db.mu then b.mu, the same order used when
	// commits fan out change notifications.
	b.mu.Lock()
	stopErr := b.stopErrLocked()
	caughtUp := seq == db.commitSeq
	b.mu.Unlock()
	if stopErr != nil {
		return false, stopErr
	}
	if db.closed {
		return false, ErrDatabaseClosed
	}
	if !caughtUp {
		// More commits landed after this batch was exchanged. The next
		// iteration applies the additional limited batch outside the lock.
		return false, nil
	}
	// Identity check: this build must still own the name reservation.
	if db.onlineBuilds[b.name] != b {
		return false, ErrIndexBuildCanceled
	}
	if _, ok := db.idxs[b.name]; ok {
		return false, ErrIndexExists
	}
	db.idxs[b.name] = b.idx
	delete(db.onlineBuilds, b.name)
	return true, nil
}

// finalize transitions the build to its single terminal state and releases
// the name reservation. It is only called by the build goroutine itself.
func (b *indexBuild) finalize(err error, publish bool) {
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return
	}
	b.done = true
	b.err = err
	close(b.doneCh)
	b.cond.Broadcast()
	b.mu.Unlock()

	db := b.db
	db.mu.Lock()
	defer db.mu.Unlock()
	// Only release a reservation that still belongs to this identity. A
	// late build can therefore never remove a newer reservation nor publish
	// into an index it no longer owns.
	if cur := db.onlineBuilds[b.name]; cur == b {
		delete(db.onlineBuilds, b.name)
		if publish && err == nil {
			if _, ok := db.idxs[b.name]; !ok {
				db.idxs[b.name] = b.idx
			}
		}
	}
}

// note records one successful commit. It runs while db.mu is held, so it may
// only touch build-local state and must never perform expensive work or
// block; normal commits must not wait on a slow build.
func (b *indexBuild) note(seq uint64, flush bool, changes map[string]*dbItem) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queuedSeq = seq
	if b.done || b.canceled || b.closing || b.overBudget {
		// A build that is already stopping stops accumulating changes, so
		// the pending log cannot grow without bound during wind-down.
		b.cond.Broadcast()
		return
	}
	if flush {
		// The DeleteAll commit supersedes every pre-flush change.
		b.pending = make(map[string]*dbItem)
		b.pendingFlush = true
		b.pendingCount = 0
	}
	for key, item := range changes {
		b.pending[key] = item
		b.pendingCount++
	}
	if b.pendingCount > b.maxPending {
		b.overBudget = true
	}
	b.cond.Broadcast()
}

// drainPending swaps the accumulated committed changes out for an empty
// batch. The returned batch is owned by the build goroutine and is applied
// without holding any database lock.
func (b *indexBuild) drainPending() (batch map[string]*dbItem, flush bool, seq uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	batch = b.pending
	flush = b.pendingFlush
	seq = b.queuedSeq
	b.pending = make(map[string]*dbItem)
	b.pendingFlush = false
	b.pendingCount = 0
	return batch, flush, seq
}

// waitForWork parks the build goroutine until new committed changes, a stop
// request or Close arrives.
func (b *indexBuild) waitForWork() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.stopErrLocked() == nil && b.pendingCount == 0 {
		b.cond.Wait()
	}
}

// checkStop returns the current terminal error, if any stop request has been
// made.
func (b *indexBuild) checkStop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stopErrLocked()
}

// stopErrLocked reports the terminal error for an already requested stop.
// The caller must hold b.mu.
func (b *indexBuild) stopErrLocked() error {
	switch {
	case b.done:
		return b.err
	case b.canceled:
		return b.cancelErr
	case b.closing:
		return ErrDatabaseClosed
	case b.overBudget:
		return ErrIndexBuildBudget
	default:
		return nil
	}
}

// requestCancel records an explicit cancellation request.
func (b *indexBuild) requestCancel(err error) {
	b.mu.Lock()
	if !b.done && !b.canceled {
		b.canceled = true
		b.cancelErr = err
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

// requestClose marks the build as stopped because the database is closing.
func (b *indexBuild) requestClose() {
	b.mu.Lock()
	if !b.done {
		b.closing = true
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

// notifyOnlineBuilds fans a successfully committed write transaction out to
// every active online index build. Rolled back transactions and commits whose
// AOF write failed never call this function, so such changes can never enter
// a build result.
func (db *DB) notifyOnlineBuilds(wc *txWriteContext) {
	if len(db.onlineBuilds) == 0 {
		return
	}
	flush := wc.rbkeys != nil
	var changes map[string]*dbItem
	if db.persist {
		if !flush && len(wc.commitItems) == 0 {
			return
		}
		// commitItems stores nil values for deletes.
		changes = wc.commitItems
	} else {
		if !flush && len(wc.changedKeys) == 0 {
			return
		}
		// Resolve the committed post-state of every touched key. A missing
		// key means the commit deleted it.
		changes = make(map[string]*dbItem, len(wc.changedKeys))
		for key := range wc.changedKeys {
			changes[key] = db.get(key)
		}
	}
	db.commitSeq++
	seq := db.commitSeq
	for _, b := range db.onlineBuilds {
		b.note(seq, flush, changes)
	}
}
