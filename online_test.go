package buntdb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// errRollbackFine is a sentinel used to force a transaction rollback.
var errRollbackFine = errors.New("rollback is fine")

// barrierLess returns a comparator that blocks on the barrier channel until
// it is closed, and a "started" channel that receives once the comparator
// has been entered. The release is idempotent because closing a channel
// unblocks every receiver.
func barrierLess(t *testing.T) (less func(a, b string) bool,
	started chan struct{}, release func()) {
	started = make(chan struct{}, 1)
	barrier := make(chan struct{})
	var once sync.Once
	less = func(a, b string) bool {
		select {
		case started <- struct{}{}:
		default:
		}
		<-barrier
		return a < b
	}
	release = func() {
		once.Do(func() { close(barrier) })
	}
	return less, started, release
}

// scanIndex collects key/value pairs from an index in the requested order.
func scanIndex(t *testing.T, db *DB, name string, desc bool) []string {
	t.Helper()
	var out []string
	err := db.View(func(tx *Tx) error {
		iter := func(key, val string) bool {
			out = append(out, key, val)
			return true
		}
		if desc {
			return tx.Descend(name, iter)
		}
		return tx.Ascend(name, iter)
	})
	if err != nil {
		t.Fatalf("scan index %q failed: %v", name, err)
	}
	return out
}

// assertIndexParity verifies that two indexes expose exactly the same
// key/value pairs, in the same order, in both scan directions.
func assertIndexParity(t *testing.T, db *DB, online, ref string) {
	t.Helper()
	for _, desc := range []bool{false, true} {
		got := scanIndex(t, db, online, desc)
		want := scanIndex(t, db, ref, desc)
		if len(got) != len(want) {
			t.Fatalf("index %q has %d entries, reference %q has %d",
				online, len(got)/2, ref, len(want)/2)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("index %q diverges from reference %q at position %d: "+
					"%q != %q (desc=%v)", online, ref, i, got[i], want[i], desc)
			}
		}
	}
}

// waitBuild waits for a build with a generous test timeout.
func waitBuild(t *testing.T, h *IndexBuild) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return h.Wait(ctx)
}

// TestOnlineIndexBarrier proves that backfill runs without the global write
// lock: an independent write and read transaction complete while the user
// comparator is blocked. The building index is not queryable until published,
// and the published scan matches a synchronously rebuilt reference index.
func TestOnlineIndexBarrier(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 300; i++ {
		if err := db.Update(func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("key:%04d", i),
				fmt.Sprintf("%04d", i), nil)
			return e
		}); err != nil {
			t.Fatal(err)
		}
	}

	less, started, release := barrierLess(t)
	h, err := db.CreateIndexOnline("online", "*", nil, less)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if h.Name() != "online" {
		t.Fatalf("unexpected build name %q", h.Name())
	}

	// Wait until the backfill comparator is actually running.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill comparator never started")
	}

	// The building name is reserved but the index is not queryable yet.
	if err := db.CreateIndex("online", "*", IndexBinary); err != ErrIndexBuilding {
		t.Fatalf("expected ErrIndexBuilding, got %v", err)
	}
	if err := db.ReplaceIndex("online", "*", IndexBinary); err != ErrIndexBuilding {
		t.Fatalf("expected ErrIndexBuilding on replace, got %v", err)
	}
	names, err := db.Indexes()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if n == "online" {
			t.Fatal("building index must not be visible in Indexes()")
		}
	}
	if err := db.View(func(tx *Tx) error {
		return tx.Ascend("online", func(key, value string) bool { return true })
	}); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound while building, got %v", err)
	}

	// Independent write and read transactions must complete while the
	// backfill comparator remains blocked.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.Update(func(tx *Tx) error {
			_, _, e := tx.Set("key:live", "9999", nil)
			return e
		})
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write tx blocked/failed during backfill: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write transaction blocked during online backfill")
	}
	readDone := make(chan error, 1)
	go func() {
		readDone <- db.View(func(tx *Tx) error {
			_, e := tx.Get("key:0001")
			return e
		})
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read tx blocked/failed during backfill: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read transaction blocked during online backfill")
	}

	release()
	if err := waitBuild(t, h); err != nil {
		t.Fatalf("online build failed: %v", err)
	}

	// Reference index built synchronously over the exact same live data.
	if err := db.CreateIndex("ref", "*", IndexBinary); err != nil {
		t.Fatal(err)
	}
	assertIndexParity(t, db, "online", "ref")
}

// TestOnlineIndexCatchUp exercises every committed change shape against the
// build: capture boundary commits, rollback exclusion, same-key overwrite,
// delete followed by recreate, DeleteAll, background expiration cleanup and a
// concurrent write storm. The final scan must match a synchronous rebuild
// item by item. It runs on both in-memory and file-backed databases.
func TestOnlineIndexCatchUp(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistent=%v", persistent), func(t *testing.T) {
			var db *DB
			var err error
			if persistent {
				db, err = Open(filepath.Join(t.TempDir(), "data.db"))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				db, err = Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
			}
			defer db.Close()

			// Items present before the capture must arrive via backfill.
			for i := 0; i < 400; i++ {
				if err := db.Update(func(tx *Tx) error {
					_, _, e := tx.Set(fmt.Sprintf("key:%04d", i),
						fmt.Sprintf("%04d", i), nil)
					return e
				}); err != nil {
					t.Fatal(err)
				}
			}

			less, started, release := barrierLess(t)
			h, err := db.CreateIndexOnline("online", "key:*", nil, less)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("backfill comparator never started")
			}

			// All of the following commits happen while backfill is blocked,
			// hence they are guaranteed to enter via the change log.
			// 1. capture boundary commit
			mustCommit(t, db, func(tx *Tx) error {
				_, _, e := tx.Set("key:boundary", "1000", nil)
				return e
			})
			// 2. rolled back changes must never reach the build
			if err := db.Update(func(tx *Tx) error {
				if _, _, e := tx.Set("key:rolledback", "x", nil); e != nil {
					return e
				}
				if _, e := tx.Delete("key:0001"); e != nil {
					return e
				}
				return errRollbackFine
			}); err != errRollbackFine {
				t.Fatalf("expected sentinel rollback error, got %v", err)
			}
			// 3. same-key consecutive overwrites collapse to the last value
			for i := 0; i < 6; i++ {
				mustCommit(t, db, func(tx *Tx) error {
					_, _, e := tx.Set("key:overwrite", fmt.Sprintf("%d", i), nil)
					return e
				})
			}
			// 4. delete then recreate the same key
			mustCommit(t, db, func(tx *Tx) error {
				_, e := tx.Delete("key:0002")
				if e != nil {
					return e
				}
				_, _, e = tx.Set("key:0002", "reborn", nil)
				return e
			})
			// 5. a plain delete
			mustCommit(t, db, func(tx *Tx) error {
				_, e := tx.Delete("key:0003")
				return e
			})
			// 6. a short-lived item that the background sweeper will remove
			mustCommit(t, db, func(tx *Tx) error {
				_, _, e := tx.Set("key:ephemeral", "ttl", &SetOptions{
					Expires: true,
					TTL:     200 * time.Millisecond,
				})
				return e
			})
			// 7. DeleteAll followed by fresh inserts
			mustCommit(t, db, func(tx *Tx) error {
				if e := tx.DeleteAll(); e != nil {
					return e
				}
				for i := 0; i < 50; i++ {
					if _, _, e := tx.Set(fmt.Sprintf("key:fresh:%03d", i),
						fmt.Sprintf("%d", 100-i), nil); e != nil {
						return e
					}
				}
				_, _, e := tx.Set("key:boundary", "1000", nil)
				return e
			})

			release()

			// 8. concurrent write storm while catch-up is running
			var wg sync.WaitGroup
			for w := 0; w < 4; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < 100; i++ {
						_ = db.Update(func(tx *Tx) error {
							_, _, e := tx.Set(fmt.Sprintf("key:storm:%d:%04d", w, i),
								fmt.Sprintf("%d", i), nil)
							return e
						})
					}
				}(w)
			}
			wg.Wait()

			// 9. an item added after the flush expires and is removed by the
			// background sweeper, exercising the committed-expiry path against
			// a build that may already have published.
			mustCommit(t, db, func(tx *Tx) error {
				_, _, e := tx.Set("key:ephemeral2", "ttl", &SetOptions{
					Expires: true,
					TTL:     200 * time.Millisecond,
				})
				return e
			})

			if err := waitBuild(t, h); err != nil {
				t.Fatalf("online build failed: %v", err)
			}

			// Give the background sweeper time to physically remove the
			// expired item; the committed delete is fed to any active build.
			time.Sleep(1300 * time.Millisecond)
			if err := db.View(func(tx *Tx) error {
				if _, e := tx.Get("key:ephemeral"); e != ErrNotFound {
					return fmt.Errorf("expected flushed item absent, got %v", e)
				}
				if _, e := tx.Get("key:ephemeral2"); e != ErrNotFound {
					return fmt.Errorf("expected ephemeral item expired, got %v", e)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if err := db.CreateIndex("ref", "key:*", IndexBinary); err != nil {
				t.Fatal(err)
			}
			assertIndexParity(t, db, "online", "ref")

			// Rolled back data must be absent from both the live tree and
			// the published index.
			if err := db.View(func(tx *Tx) error {
				if _, e := tx.Get("key:rolledback"); e != ErrNotFound {
					return fmt.Errorf("rolled back key visible: %v", e)
				}
				vals := scanIndex(t, db, "online", false)
				for _, v := range vals {
					if v == "x" {
						return errors.New("rolled back value reached the index")
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func mustCommit(t *testing.T, db *DB, fn func(tx *Tx) error) {
	t.Helper()
	if err := db.Update(fn); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

// TestOnlineIndexBudget verifies the pending change budget: sustained writes
// while the build cannot catch up fail the build with ErrIndexBuildBudget,
// release the name and never block the committing transactions.
func TestOnlineIndexBudget(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 50; i++ {
		mustCommit(t, db, func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("k:%03d", i), "v", nil)
			return e
		})
	}

	less, started, release := barrierLess(t)
	defer release()
	h, err := db.CreateIndexOnline("budget", "*", &OnlineIndexOptions{
		MaxPendingChanges: 5,
	}, less)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill comparator never started")
	}

	// Name reservation is enforced while the build is active.
	if err := db.CreateIndex("budget", "*", IndexBinary); err != ErrIndexBuilding {
		t.Fatalf("expected ErrIndexBuilding, got %v", err)
	}

	// Flood commits. Each must return promptly even though the build is
	// stuck in its comparator.
	for i := 0; i < 50; i++ {
		done := make(chan error, 1)
		go func(i int) {
			done <- db.Update(func(tx *Tx) error {
				_, _, e := tx.Set(fmt.Sprintf("flood:%04d", i), "v", nil)
				return e
			})
		}(i)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("commit %d failed: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("commit %d blocked by slow online build", i)
		}
	}

	release()
	if err := waitBuild(t, h); err != ErrIndexBuildBudget {
		t.Fatalf("expected ErrIndexBuildBudget, got %v", err)
	}

	// The name must be released after the failed build.
	if err := db.CreateIndex("budget", "*", IndexBinary); err != nil {
		t.Fatalf("name not released after budget failure: %v", err)
	}
}

// TestOnlineIndexCancel verifies that canceling a Wait context only ends the
// wait, while explicit Cancel stops the build. After cancellation the name is
// released and a fresh build with the same name can be published.
func TestOnlineIndexCancel(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 20; i++ {
		mustCommit(t, db, func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("k:%02d", i), "v", nil)
			return e
		})
	}

	less, started, release := barrierLess(t)
	defer release()
	h, err := db.CreateIndexOnline("cancel", "*", nil, less)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill comparator never started")
	}

	// Context cancellation ends this wait only.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Wait(ctx); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// The build is still running and still reserves the name.
	if err := db.CreateIndex("cancel", "*", IndexBinary); err != ErrIndexBuilding {
		t.Fatalf("expected ErrIndexBuilding after wait cancellation, got %v", err)
	}

	// Explicit cancellation terminates the build exactly once. Cancel blocks
	// until the (currently comparator-stuck) build reaches its terminal state.
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- h.Cancel() }()
	select {
	case <-cancelDone:
		t.Fatal("Cancel returned while the build was still blocked")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case err := <-cancelDone:
		if err != ErrIndexBuildCanceled {
			t.Fatalf("expected ErrIndexBuildCanceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Cancel did not wait for build cleanup")
	}
	if err := h.Cancel(); err != ErrIndexBuildCanceled {
		t.Fatalf("second Cancel must report the same terminal state, got %v", err)
	}
	if err := waitBuild(t, h); err != ErrIndexBuildCanceled {
		t.Fatalf("Wait after cancel mismatch: %v", err)
	}

	// Name is released: a new build can take it and publish successfully.
	h2, err := db.CreateIndexOnline("cancel", "*", nil, IndexBinary)
	if err != nil {
		t.Fatalf("rebuild after cancel failed: %v", err)
	}
	if err := waitBuild(t, h2); err != nil {
		t.Fatalf("replacement build failed: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		return tx.Ascend("cancel", func(key, val string) bool { return true })
	}); err != nil {
		t.Fatalf("rebuilt index not queryable: %v", err)
	}
}

// TestOnlineIndexClose verifies that Close blocks while a build is stuck in
// the user comparator, waits for its cleanup and that the build terminates
// with ErrDatabaseClosed.
func TestOnlineIndexClose(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		mustCommit(t, db, func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("k:%02d", i), "v", nil)
			return e
		})
	}

	less, started, release := barrierLess(t)
	h, err := db.CreateIndexOnline("close", "*", nil, less)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill comparator never started")
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while the build was still blocked")
	case <-time.After(300 * time.Millisecond):
	}

	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not wait for build cleanup")
	}
	if err := h.Wait(context.Background()); err != ErrDatabaseClosed {
		t.Fatalf("expected ErrDatabaseClosed, got %v", err)
	}
	// Closing again reports the standard error.
	if err := db.Close(); err != ErrDatabaseClosed {
		t.Fatalf("expected ErrDatabaseClosed on second Close, got %v", err)
	}
}

// TestOnlineIndexComparatorPanic verifies that a panicking user comparator
// becomes a build error, leaves the process alive and releases the name.
func TestOnlineIndexComparatorPanic(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 20; i++ {
		mustCommit(t, db, func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("k:%02d", i), "v", nil)
			return e
		})
	}

	panicked := make(chan struct{}, 1)
	less := func(a, b string) bool {
		select {
		case panicked <- struct{}{}:
		default:
		}
		panic("boom from user comparator")
	}
	h, err := db.CreateIndexOnline("pan", "*", nil, less)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-panicked:
	case <-time.After(5 * time.Second):
		t.Fatal("panicking comparator never invoked")
	}
	err = waitBuild(t, h)
	if err == nil || !strings.Contains(err.Error(), "comparator panic") {
		t.Fatalf("expected comparator panic build error, got %v", err)
	}

	// Name released and a well-behaved build can take over.
	h2, err := db.CreateIndexOnline("pan", "*", nil, IndexBinary)
	if err != nil {
		t.Fatalf("name not released after panic: %v", err)
	}
	if err := waitBuild(t, h2); err != nil {
		t.Fatalf("replacement build failed: %v", err)
	}
}

// TestOnlineIndexExpirySemantics verifies publish-time expiry semantics: an
// item that expires while the build is running may remain a physical entry of
// the published index, but ordinary scans filter it out. The valid entries
// stay ordered exactly like a synchronous reference rebuild.
func TestOnlineIndexExpirySemantics(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 100; i++ {
		mustCommit(t, db, func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("k:%03d", i),
				fmt.Sprintf("%d", i), nil)
			return e
		})
	}
	// Expires before publication but within the background sweeper's first
	// one second tick, so it remains a physical, yet expired, entry at
	// publish time.
	mustCommit(t, db, func(tx *Tx) error {
		_, _, e := tx.Set("k:expiring", "0", &SetOptions{
			Expires: true,
			TTL:     400 * time.Millisecond,
		})
		return e
	})

	less, started, release := barrierLess(t)
	h, err := db.CreateIndexOnline("online", "*", nil, less)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill comparator never started")
	}
	// Wait long enough for the item to expire, but stay inside the one
	// second background sweeper window so it is not physically deleted.
	time.Sleep(550 * time.Millisecond)
	release()
	if err := waitBuild(t, h); err != nil {
		t.Fatalf("online build failed: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		// The physical entry is still present ...
		if _, e := tx.Get("k:expiring", true); e != nil {
			return fmt.Errorf("expired physical item missing: %v", e)
		}
		// ... but ordinary reads filter it using publish-time semantics.
		if _, e := tx.Get("k:expiring"); e != ErrNotFound {
			return fmt.Errorf("expected expired item filtered, got %v", e)
		}
		var seen bool
		if e := tx.Ascend("online", func(key, val string) bool {
			if key == "k:expiring" {
				seen = true
			}
			return true
		}); e != nil {
			return e
		}
		if seen {
			return errors.New("scan exposed an expired item")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A synchronous rebuild over the same physical data retains the same
	// expired entry and applies the same scan filtering, hence the scans
	// must be identical item by item.
	if err := db.CreateIndex("ref", "*", IndexBinary); err != nil {
		t.Fatal(err)
	}
	assertIndexParity(t, db, "online", "ref")
}

// TestOnlineIndexConcurrentBuilds verifies that two builds on distinct names
// run concurrently and both publish indexes matching synchronous references.
func TestOnlineIndexConcurrentBuilds(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 200; i++ {
		mustCommit(t, db, func(tx *Tx) error {
			_, _, e := tx.Set(fmt.Sprintf("k:%04d", i), fmt.Sprintf("%d", i), nil)
			return e
		})
	}

	h1, err := db.CreateIndexOnline("i1", "*", nil, IndexBinary)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := db.CreateIndexOnline("i2", "*", nil, IndexInt)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitBuild(t, h1); err != nil {
		t.Fatalf("build i1 failed: %v", err)
	}
	if err := waitBuild(t, h2); err != nil {
		t.Fatalf("build i2 failed: %v", err)
	}
	if err := db.CreateIndex("r1", "*", IndexBinary); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("r2", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	assertIndexParity(t, db, "i1", "r1")
	assertIndexParity(t, db, "i2", "r2")
}
