package buntdb

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memSnapshotDB opens a pure in-memory database for snapshot tests.
func memSnapshotDB(t testing.TB) *DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// waitErr waits for an error channel or fails the test after a timeout.
func waitErr(t *testing.T, ch <-chan error, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
	case <-time.After(timeout):
		t.Fatalf("%s: timed out after %v", msg, timeout)
	}
}

// assertSnapshotClosed verifies that every read entry point rejects use after
// the snapshot has been closed.
func assertSnapshotClosed(t *testing.T, snap *Snapshot) {
	t.Helper()
	if _, err := snap.Get("anything"); err != ErrSnapshotClosed {
		t.Fatalf("Get after close: got %v, want ErrSnapshotClosed", err)
	}
	if _, err := snap.Get("anything", true); err != ErrSnapshotClosed {
		t.Fatalf("Get(ignoreExpired) after close: got %v", err)
	}
	if _, err := snap.TTL("anything"); err != ErrSnapshotClosed {
		t.Fatalf("TTL after close: got %v", err)
	}
	if _, err := snap.Len(); err != ErrSnapshotClosed {
		t.Fatalf("Len after close: got %v", err)
	}
	if err := snap.Ascend("", func(k, v string) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("Ascend after close: got %v", err)
	}
	if err := snap.Descend("", func(k, v string) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("Descend after close: got %v", err)
	}
	if err := snap.Ascend("ix", func(k, v string) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("index Ascend after close: got %v", err)
	}
	if _, err := snap.Indexes(); err != ErrSnapshotClosed {
		t.Fatalf("Indexes after close: got %v", err)
	}
	if _, err := snap.GetLess("ix"); err != ErrSnapshotClosed {
		t.Fatalf("GetLess after close: got %v", err)
	}
	if err := snap.Intersects("geo", "{}", func(k, v string) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("Intersects after close: got %v", err)
	}
	if err := snap.Nearby("geo", "{}", func(k, v string, d float64) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("Nearby after close: got %v", err)
	}
	if err := snap.AscendKeys("*", func(k, v string) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("AscendKeys after close: got %v", err)
	}
	if err := snap.DescendKeys("*", func(k, v string) bool { return true }); err != ErrSnapshotClosed {
		t.Fatalf("DescendKeys after close: got %v", err)
	}
}

// TestSnapshotBasicGetTTLAndLen covers the core read surface: captured values
// are frozen, TTL uses the negative convention for non-expiring items and
// live mutations never reach the snapshot.
func TestSnapshotBasicGetTTLAndLen(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("k1", "v1", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("k2", "v2",
			&SetOptions{Expires: true, TTL: time.Hour}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	n, err := snap.Len()
	if err != nil || n != 2 {
		t.Fatalf("Len = %v, %v, want 2", n, err)
	}
	if v, err := snap.Get("k1"); err != nil || v != "v1" {
		t.Fatalf("Get k1 = %q, %v", v, err)
	}
	if _, err := snap.Get("missing"); err != ErrNotFound {
		t.Fatalf("Get missing: got %v, want ErrNotFound", err)
	}
	if ttl, err := snap.TTL("k1"); err != nil || ttl != -1 {
		t.Fatalf("TTL k1 = %v, %v, want -1", ttl, err)
	}
	ttl, err := snap.TTL("k2")
	if err != nil || ttl <= time.Hour-time.Second || ttl > time.Hour {
		t.Fatalf("TTL k2 = %v, %v, want ~1h", ttl, err)
	}
	if _, err := snap.TTL("missing"); err != ErrNotFound {
		t.Fatalf("TTL missing: got %v, want ErrNotFound", err)
	}

	// mutate the live database aggressively
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("k1", "changed", nil); err != nil {
			return err
		}
		if _, err := tx.Delete("k2"); err != nil {
			return err
		}
		if _, _, err := tx.Set("k3", "v3", nil); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// the snapshot must be completely unchanged
	if v, _ := snap.Get("k1"); v != "v1" {
		t.Fatalf("snapshot changed by live Set: k1 = %q, want v1", v)
	}
	if v, err := snap.Get("k2"); err != nil || v != "v2" {
		t.Fatalf("snapshot changed by live Delete: k2 = %q, %v", v, err)
	}
	if _, err := snap.Get("k3"); err != ErrNotFound {
		t.Fatalf("live Set leaked into snapshot: %v", err)
	}
	if n, _ := snap.Len(); n != 2 {
		t.Fatalf("snapshot Len changed: got %d, want 2", n)
	}
	// while the live database reflects the mutations
	if err := db.View(func(tx *Tx) error {
		if v, err := tx.Get("k1"); err != nil || v != "changed" {
			t.Fatalf("live Get k1 = %q, %v", v, err)
		}
		if _, err := tx.Get("k2"); err != ErrNotFound {
			t.Fatalf("live Get k2: %v", err)
		}
		if n, _ := tx.Len(); n != 2 {
			t.Fatalf("live Len = %d, want 2", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotTTLBoundaries deterministically exercises expiration
// visibility against the fixed capture instant, including the exact
// exat == now boundary and items already expired at capture.
func TestSnapshotTTLBoundaries(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateIndex("num", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// white-box: plant records with controlled absolute expiration times in
	// both the key tree and an index, bypassing the background cleaner and
	// capture at a fixed instant.
	boundary := &dbItem{
		key: "boundary", val: "1",
		opts: &dbItemOpts{ex: true, exat: now},
	}
	expired := &dbItem{
		key: "expired", val: "2",
		opts: &dbItemOpts{ex: true, exat: now.Add(-time.Second)},
	}
	forever := &dbItem{key: "forever", val: "0"}
	db.mu.Lock()
	idx := db.idxs["num"]
	for _, dbi := range []*dbItem{boundary, expired, forever} {
		db.keys.Set(dbi)
		idx.btr.Set(dbi)
	}
	snap := db.snapshotLocked(now)
	db.mu.Unlock()
	defer snap.Close()

	// exat == capture time is still visible (same boundary as Tx.Get)
	if v, err := snap.Get("boundary"); err != nil || v != "1" {
		t.Fatalf("boundary Get = %q, %v", v, err)
	}
	if ttl, err := snap.TTL("boundary"); err != nil || ttl != 0 {
		t.Fatalf("boundary TTL = %v, %v, want 0", ttl, err)
	}
	// already expired at capture: invisible through every read path
	if _, err := snap.Get("expired"); err != ErrNotFound {
		t.Fatalf("expired Get: got %v, want ErrNotFound", err)
	}
	if v, err := snap.Get("expired", true); err != nil || v != "2" {
		t.Fatalf("expired Get(ignore) = %q, %v", v, err)
	}
	if _, err := snap.TTL("expired"); err != ErrNotFound {
		t.Fatalf("expired TTL: got %v, want ErrNotFound", err)
	}
	if n, _ := snap.Len(); n != 2 {
		t.Fatalf("Len with expired item = %d, want 2", n)
	}
	var keys []string
	if err := snap.Ascend("", func(k, v string) bool {
		keys = append(keys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "boundary" || keys[1] != "forever" {
		t.Fatalf("key scan did not filter expired item: %v", keys)
	}
	// the expired record must also disappear from captured index scans
	keys = keys[:0]
	if err := snap.Ascend("num", func(k, v string) bool {
		keys = append(keys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "forever" || keys[1] != "boundary" {
		t.Fatalf("index scan did not filter expired item: %v", keys)
	}
	keys = keys[:0]
	if err := snap.Descend("num", func(k, v string) bool {
		keys = append(keys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "boundary" || keys[1] != "forever" {
		t.Fatalf("descending index scan did not filter expired item: %v", keys)
	}
}

// TestSnapshotTTLFreezesOverTime proves real time passing after creation does
// not change snapshot content or its reported TTL, while the live database
// expires and evicts the same item.
func TestSnapshotTTLFreezesOverTime(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("mortal", "value",
			&SetOptions{Expires: true, TTL: 300 * time.Millisecond})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	ttlAtCapture, err := snap.TTL("mortal")
	if err != nil {
		t.Fatal(err)
	}
	// wait long enough for the live item to expire and be background-cleaned
	time.Sleep(1400 * time.Millisecond)
	if _, err := func() (string, error) {
		var v string
		err := db.View(func(tx *Tx) error {
			var e error
			v, e = tx.Get("mortal")
			return e
		})
		return v, err
	}(); err != ErrNotFound {
		t.Fatalf("live item should have expired: %v", err)
	}
	if v, err := snap.Get("mortal"); err != nil || v != "value" {
		t.Fatalf("frozen Get = %q, %v", v, err)
	}
	if ttl, err := snap.TTL("mortal"); err != nil || ttl != ttlAtCapture {
		t.Fatalf("TTL drifted: capture=%v now=%v err=%v", ttlAtCapture, ttl, err)
	}
	if n, _ := snap.Len(); n != 1 {
		t.Fatalf("frozen Len = %d, want 1", n)
	}
	var seen bool
	if err := snap.Ascend("", func(k, v string) bool {
		seen = seen || k == "mortal"
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Fatal("frozen scan lost the expired item")
	}
}

// TestSnapshotIndexScansAndTieOrder verifies ascending/descending scans on the
// primary tree and on ordered indexes, including same-value tie breaking by
// key, range scans and equality scans.
func TestSnapshotIndexScansAndTieOrder(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateIndex("num", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	// values deliberately repeat to exercise tie ordering
	items := map[string]string{
		"a": "10", "b": "20", "c": "20", "d": "30", "e": "10", "f": "30",
	}
	if err := db.Update(func(tx *Tx) error {
		for k, v := range items {
			if _, _, err := tx.Set(k, v, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	// primary key tree, ascending/descending
	var got []string
	if err := snap.Ascend("", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"a", "b", "c", "d", "e", "f"}
	if len(got) != len(wantKeys) {
		t.Fatalf("Ascend keys = %v", got)
	}
	for i := range wantKeys {
		if got[i] != wantKeys[i] {
			t.Fatalf("Ascend key order = %v, want %v", got, wantKeys)
		}
	}
	got = got[:0]
	if err := snap.Descend("", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	for i := range wantKeys {
		if got[i] != wantKeys[len(wantKeys)-1-i] {
			t.Fatalf("Descend key order = %v", got)
		}
	}

	// index ordering: value first, key fallback on ties
	got = got[:0]
	if err := snap.Ascend("num", func(k, v string) bool {
		got = append(got, k+":"+v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	wantOrd := []string{
		"a:10", "e:10", "b:20", "c:20", "d:30", "f:30",
	}
	if len(got) != len(wantOrd) {
		t.Fatalf("index Ascend = %v", got)
	}
	for i := range wantOrd {
		if got[i] != wantOrd[i] {
			t.Fatalf("index Ascend = %v, want %v", got, wantOrd)
		}
	}
	got = got[:0]
	if err := snap.Descend("num", func(k, v string) bool {
		got = append(got, k+":"+v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	for i := range wantOrd {
		if got[i] != wantOrd[len(wantOrd)-1-i] {
			t.Fatalf("index Descend = %v, want reverse of %v", got, wantOrd)
		}
	}

	// equality scan returns all same-value items in key order
	got = got[:0]
	if err := snap.AscendEqual("num", "20", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("AscendEqual(20) = %v", got)
	}
	got = got[:0]
	if err := snap.DescendEqual("num", "20", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "c" || got[1] != "b" {
		t.Fatalf("DescendEqual(20) = %v", got)
	}

	// range scans on the index: [20, 30) excludes value 30
	got = got[:0]
	if err := snap.AscendRange("num", "20", "30", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("AscendRange = %v", got)
	}
	// descending range: (greaterThan, lessOrEqual] = (20, 30], mirroring
	// Tx.DescendRange semantics, ties in descending key order
	got = got[:0]
	if err := snap.DescendRange("num", "30", "20", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "f" || got[1] != "d" {
		t.Fatalf("DescendRange = %v", got)
	}
	// primary key range and pattern scans
	got = got[:0]
	if err := snap.AscendGreaterOrEqual("", "c", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0] != "c" || got[3] != "f" {
		t.Fatalf("AscendGreaterOrEqual = %v", got)
	}
	got = got[:0]
	if err := snap.AscendKeys("*", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("AscendKeys(*) = %v", got)
	}
	got = got[:0]
	if err := snap.AscendKeys("a*", func(k, v string) bool {
		got = append(got, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("AscendKeys(a*) = %v", got)
	}
	if _, err := snap.GetLess("num"); err != nil {
		t.Fatalf("GetLess: %v", err)
	}
	if _, err := snap.GetLess("unknown"); err != ErrNotFound {
		t.Fatalf("GetLess unknown: %v", err)
	}
	if err := snap.Ascend("unknown", func(k, v string) bool { return true }); err != ErrNotFound {
		t.Fatalf("Ascend unknown index: %v", err)
	}
}

// TestSnapshotCompoundIndex ensures compound comparators (multiple pure less
// functions) are captured and applied.
func TestSnapshotCompoundIndex(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	// order by integer value, ties broken by binary key comparison
	if err := db.CreateIndex("comp", "*", IndexInt, IndexBinary); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for _, kv := range [][2]string{
			{"z", "1"}, {"a", "1"}, {"m", "0"}, {"b", "0"},
		} {
			if _, _, err := tx.Set(kv[0], kv[1], nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	var got []string
	if err := snap.Ascend("comp", func(k, v string) bool {
		got = append(got, k+":"+v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"b:0", "m:0", "a:1", "z:1"}
	if len(got) != len(want) {
		t.Fatalf("compound Ascend = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("compound Ascend = %v, want %v", got, want)
		}
	}
}

// TestSnapshotIndexReplacementDropAndDeleteAll proves that dropping,
// rebuilding or replacing indexes, as well as DeleteAll, cannot change a
// captured snapshot.
func TestSnapshotIndexReplacementDropAndDeleteAll(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateIndex("ix", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
			if _, _, err := tx.Set(kv[0], kv[1], nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	// replace the live index with a reversed comparator of the same name
	if err := db.ReplaceIndex("ix", "*", Desc(IndexInt)); err != nil {
		t.Fatal(err)
	}
	var snapOrder []string
	if err := snap.Ascend("ix", func(k, v string) bool {
		snapOrder = append(snapOrder, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(snapOrder) != 3 || snapOrder[0] != "a" || snapOrder[2] != "c" {
		t.Fatalf("snapshot index changed by ReplaceIndex: %v", snapOrder)
	}
	var liveOrder []string
	if err := db.View(func(tx *Tx) error {
		return tx.Ascend("ix", func(k, v string) bool {
			liveOrder = append(liveOrder, k)
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	if liveOrder[0] != "c" || liveOrder[2] != "a" {
		t.Fatalf("live index not replaced: %v", liveOrder)
	}

	// drop and rebuild the live index; snapshot keeps its own tree
	if err := db.DropIndex("ix"); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		err := tx.Ascend("ix", func(k, v string) bool { return true })
		if err != ErrNotFound {
			t.Fatalf("live Ascend dropped index: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := snap.Ascend("ix", func(k, v string) bool { return true }); err != nil {
		t.Fatalf("snapshot index lost by DropIndex: %v", err)
	}
	if err := db.CreateIndex("ix", "*", IndexBinary); err != nil {
		t.Fatal(err)
	}
	if err := snap.Ascend("ix", func(k, v string) bool {
		if v != "1" && v != "2" && v != "3" {
			t.Fatalf("snapshot picked up rebuilt index data: %s", v)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}

	// DeleteAll on the live database must not touch the snapshot
	if err := db.Update(func(tx *Tx) error {
		return tx.DeleteAll()
	}); err != nil {
		t.Fatal(err)
	}
	if n, _ := snap.Len(); n != 3 {
		t.Fatalf("snapshot changed by DeleteAll: Len=%d", n)
	}
	if v, err := snap.Get("b"); err != nil || v != "2" {
		t.Fatalf("snapshot changed by DeleteAll: b=%q, %v", v, err)
	}
	if err := snap.Ascend("ix", func(k, v string) bool { return true }); err != nil {
		t.Fatalf("snapshot index changed by DeleteAll: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		if n, _ := tx.Len(); n != 0 {
			t.Fatalf("live Len after DeleteAll = %d", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotSpatialQueriesRejected ensures spatial indexes are remembered
// but their queries are explicitly rejected, while unknown names stay
// ErrNotFound.
func TestSnapshotSpatialQueriesRejected(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateSpatialIndex("geo", "*", IndexRect); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("p1", Point(1, 2), nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	names, err := snap.Indexes()
	if err != nil || len(names) != 1 || names[0] != "geo" {
		t.Fatalf("Indexes = %v, %v", names, err)
	}
	if err := snap.Intersects("geo", Point(1, 2),
		func(k, v string) bool { return true }); err != ErrSpatialIndexNotSupported {
		t.Fatalf("Intersects: got %v, want ErrSpatialIndexNotSupported", err)
	}
	if err := snap.Nearby("geo", Point(1, 2),
		func(k, v string, d float64) bool { return true }); err != ErrSpatialIndexNotSupported {
		t.Fatalf("Nearby: got %v, want ErrSpatialIndexNotSupported", err)
	}
	if err := snap.Ascend("geo", func(k, v string) bool { return true }); err != ErrSpatialIndexNotSupported {
		t.Fatalf("Ascend on spatial index: got %v", err)
	}
	if err := snap.DescendRange("geo", "x", "y",
		func(k, v string) bool { return true }); err != ErrSpatialIndexNotSupported {
		t.Fatalf("DescendRange on spatial index: got %v", err)
	}
	if _, err := snap.GetLess("geo"); err != ErrSpatialIndexNotSupported {
		t.Fatalf("GetLess on spatial index: got %v", err)
	}
	// unknown index names keep the not-found contract on every entry point
	if _, err := snap.GetLess("nope"); err != ErrNotFound {
		t.Fatalf("GetLess unknown: %v", err)
	}
	if err := snap.Intersects("nope", "{}",
		func(k, v string) bool { return true }); err != ErrNotFound {
		t.Fatalf("Intersects unknown: %v", err)
	}
	if err := snap.Nearby("nope", "{}",
		func(k, v string, d float64) bool { return true }); err != ErrNotFound {
		t.Fatalf("Nearby unknown: %v", err)
	}
}

// TestSnapshotCloseLifecycle covers idempotent Close, errors after Close and
// independent lifetimes of multiple snapshots.
func TestSnapshotCloseLifecycle(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("k", "v", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap1, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snap1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// idempotent: repeated Close is a successful no-op
	if err := snap1.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	assertSnapshotClosed(t, snap1)
	// the second snapshot remains fully functional
	if v, err := snap2.Get("k"); err != nil || v != "v" {
		t.Fatalf("snap2 affected by snap1 close: %q, %v", v, err)
	}
	if n, err := snap2.Len(); err != nil || n != 1 {
		t.Fatalf("snap2 Len after snap1 close: %d, %v", n, err)
	}
	if err := snap2.Close(); err != nil {
		t.Fatalf("snap2 Close: %v", err)
	}
	assertSnapshotClosed(t, snap2)
}

// TestSnapshotCloseWaitsForInFlightRead pins down the lifecycle rule: Close
// blocks until an in-flight scan finishes, and a callback never observes
// cleared records. Operations started afterwards fail with a clear error.
func TestSnapshotCloseWaitsForInFlightRead(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("k", "v", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- snap.Ascend("", func(key, value string) bool {
			if key != "k" || value != "v" {
				t.Errorf("callback observed corrupted record: %q=%q", key, value)
			}
			close(started)
			<-release // hold the read open; Close must wait
			return true
		})
	}()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- snap.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close returned while a read was still in flight")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	waitErr(t, scanDone, time.Second, "in-flight scan")
	waitErr(t, closeDone, time.Second, "blocked Close")
	// idempotent and clearly closed afterwards
	if err := snap.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	assertSnapshotClosed(t, snap)
}

// TestSnapshotSurvivesDatabaseClose proves DB.Close does not invalidate an
// open snapshot, while creating a new snapshot on a closed DB fails clearly.
func TestSnapshotSurvivesDatabaseClose(t *testing.T) {
	db := memSnapshotDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("k", "v", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(); err != nil {
		t.Fatalf("second snapshot while open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	if _, err := db.Snapshot(); err != ErrDatabaseClosed {
		t.Fatalf("Snapshot on closed db: got %v, want ErrDatabaseClosed", err)
	}
	// the snapshot stays fully readable after the database is gone
	if v, err := snap.Get("k"); err != nil || v != "v" {
		t.Fatalf("snapshot read after db close: %q, %v", v, err)
	}
	if n, err := snap.Len(); err != nil || n != 1 {
		t.Fatalf("snapshot Len after db close: %d, %v", n, err)
	}
	var count int
	if err := snap.Ascend("", func(k, v string) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("snapshot scan after db close: %v", err)
	}
	if count != 1 {
		t.Fatalf("snapshot scan count after db close = %d", count)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	assertSnapshotClosed(t, snap)
}

// TestSnapshotSlowScanDoesNotBlockDB is the barrier-controlled proof that a
// slow snapshot scan holds no database lock: a concurrent Update and even
// DB.Close complete while the scan is parked inside a user callback.
func TestSnapshotSlowScanDoesNotBlockDB(t *testing.T) {
	db := memSnapshotDB(t)
	const total = 100
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < total; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("key:%03d", i),
				fmt.Sprintf("val:%d", i), nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	scanDone := make(chan error, 1)
	var scanned int64
	go func() {
		scanDone <- snap.Ascend("", func(key, value string) bool {
			if key == "key:000" {
				close(started)
				<-release // park inside the user callback
			}
			atomic.AddInt64(&scanned, 1)
			return true
		})
	}()
	<-started

	// a writer must not be blocked by the parked snapshot scan
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.Update(func(tx *Tx) error {
			_, _, err := tx.Set("key:000", "mutated", nil)
			return err
		})
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("concurrent Update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Update blocked while a snapshot scan was in progress")
	}

	// closing the database must also complete despite the parked scan
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("concurrent DB.Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DB.Close blocked while a snapshot scan was in progress")
	}

	// release the barrier; the full frozen scan must finish successfully
	close(release)
	waitErr(t, scanDone, 2*time.Second, "slow scan")
	if atomic.LoadInt64(&scanned) != total {
		t.Fatalf("scanned %d rows, want %d", scanned, total)
	}
	// frozen content survived both the live mutation and DB.Close
	if v, err := snap.Get("key:000"); err != nil || v != "val:0" {
		t.Fatalf("snapshot content after live write and db close: %q, %v", v, err)
	}
	if n, _ := snap.Len(); n != total {
		t.Fatalf("Len after barrier scan = %d, want %d", n, total)
	}
}

// TestSnapshotConcurrentCaptureAndCommits hammers the database with
// concurrent commits while snapshots are continuously captured. Every
// snapshot must be internally consistent and usable under the race detector.
func TestSnapshotConcurrentCaptureAndCommits(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateIndex("ord", "*", IndexBinary); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 300; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("seed:%04d", i), "s", nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const writers = 4
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w:%d:%03d", w, i%150)
				_ = db.Update(func(tx *Tx) error {
					if _, _, err := tx.Set(key, fmt.Sprintf("v:%d", i), nil); err != nil {
						return err
					}
					if i%5 == 0 {
						if _, _, err := tx.Set(key, "ttl",
							&SetOptions{Expires: true, TTL: 80 * time.Millisecond}); err != nil {
							return err
						}
					}
					if i%7 == 0 {
						_, _ = tx.Delete(fmt.Sprintf("w:%d:%03d", w, (i+1)%150))
					}
					return nil
				})
			}
		}(w)
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	var captures int32
	for time.Now().Before(deadline) {
		snap, err := db.Snapshot()
		if err != nil {
			t.Fatalf("capture: %v", err)
		}
		n, err := snap.Len()
		if err != nil {
			t.Fatalf("snapshot Len: %v", err)
		}
		var keyCount, idxCount int
		if err := snap.Ascend("", func(k, v string) bool {
			gv, gerr := snap.Get(k)
			if gerr != nil || gv != v {
				t.Fatalf("inconsistent snapshot Get(%q)=%q,%v scan=%q",
					k, gv, gerr, v)
			}
			ttl, terr := snap.TTL(k)
			if terr != nil {
				t.Fatalf("visible item %q TTL error: %v", k, terr)
			}
			if ttl != -1 && ttl < 0 {
				t.Fatalf("negative non-sentinel TTL for %q: %v", k, ttl)
			}
			keyCount++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if keyCount != n {
			t.Fatalf("Len=%d but key scan=%d", n, keyCount)
		}
		if err := snap.Ascend("ord", func(k, v string) bool {
			if _, err := snap.Get(k); err != nil {
				t.Fatalf("index item %q not in key tree: %v", k, err)
			}
			idxCount++
			return true
		}); err != nil {
			t.Fatalf("index scan: %v", err)
		}
		if idxCount != n {
			t.Fatalf("index scan=%d but Len=%d", idxCount, n)
		}
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := snap.Len(); err != ErrSnapshotClosed {
			t.Fatalf("post-close Len: %v", err)
		}
		atomic.AddInt32(&captures, 1)
	}
	close(stop)
	wg.Wait()
	if atomic.LoadInt32(&captures) == 0 {
		t.Fatal("no snapshots captured")
	}
}

// TestSnapshotModelConsistency cross-checks a snapshot against a model read
// from a View transaction at the same logical moment.
func TestSnapshotModelConsistency(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateIndex("num", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	type modelItem struct {
		val    string
		ttl    time.Duration
		hasTTL bool
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 50; i++ {
			kv := fmt.Sprintf("%03d", i%10)
			if i%4 == 0 {
				if _, _, err := tx.Set(fmt.Sprintf("k:%03d", i), kv,
					&SetOptions{Expires: true, TTL: time.Hour}); err != nil {
					return err
				}
			} else {
				if _, _, err := tx.Set(fmt.Sprintf("k:%03d", i), kv, nil); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	model := make(map[string]modelItem)
	if err := db.View(func(tx *Tx) error {
		return tx.Ascend("", func(k, v string) bool {
			ttl, err := tx.TTL(k)
			if err != nil {
				t.Fatalf("model TTL %q: %v", k, err)
			}
			model[k] = modelItem{
				val: v, ttl: ttl, hasTTL: ttl != -1,
			}
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if n, _ := snap.Len(); n != len(model) {
		t.Fatalf("Len=%d model=%d", n, len(model))
	}
	for k, m := range model {
		v, err := snap.Get(k)
		if err != nil || v != m.val {
			t.Fatalf("snapshot Get %q = %q,%v model=%q", k, v, err, m.val)
		}
		ttl, err := snap.TTL(k)
		if err != nil {
			t.Fatalf("snapshot TTL %q: %v", k, err)
		}
		if !m.hasTTL {
			if ttl != -1 {
				t.Fatalf("TTL %q = %v, want -1", k, ttl)
			}
			continue
		}
		// capture happened a touch after the model read: TTL may be slightly
		// smaller but must never grow or drift by more than a second
		if ttl > m.ttl {
			t.Fatalf("TTL %q grew: model=%v snapshot=%v", k, m.ttl, ttl)
		}
		if m.ttl-ttl > time.Second {
			t.Fatalf("TTL %q drift too large: model=%v snapshot=%v", k, m.ttl, ttl)
		}
	}
	// every snapshot key exists in the model and vice versa
	if err := snap.Ascend("", func(k, v string) bool {
		if _, ok := model[k]; !ok {
			t.Fatalf("snapshot key %q absent from model", k)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	// index scan yields the same keys as the model, ordered by (value, key)
	var idxKeys []string
	if err := snap.Ascend("num", func(k, v string) bool {
		idxKeys = append(idxKeys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(idxKeys) != len(model) {
		t.Fatalf("index scan count=%d model=%d", len(idxKeys), len(model))
	}
	for i := 1; i < len(idxKeys); i++ {
		pa, pb := model[idxKeys[i-1]], model[idxKeys[i]]
		if pa.val > pb.val || (pa.val == pb.val && idxKeys[i-1] > idxKeys[i]) {
			t.Fatalf("index order violated at %d: %v before %v",
				i, idxKeys[i-1], idxKeys[i])
		}
	}
}

// TestSnapshotSharedRecordPointers verifies the no-copy sharing rule: the same
// immutable record pointer backs the primary key tree and every index tree.
func TestSnapshotSharedRecordPointers(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	if err := db.CreateIndex("num", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("shared", "42", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	keyItem := snap.keys.Get(&dbItem{key: "shared"}).(*dbItem)
	var idxItem *dbItem
	snap.idxs["num"].btr.Ascend(nil, func(item interface{}) bool {
		if dbi := item.(*dbItem); dbi.key == "shared" {
			idxItem = dbi
			return false
		}
		return true
	})
	if idxItem == nil {
		t.Fatal("record missing from snapshot index tree")
	}
	if keyItem != idxItem {
		t.Fatal("key tree and index tree hold different record copies")
	}
	// the snapshot trees must not be the live tree objects
	if err := db.View(func(tx *Tx) error {
		if snap.keys == tx.db.keys {
			t.Fatal("snapshot shares the live key tree")
		}
		if snap.idxs["num"].btr == tx.db.idxs["num"].btr {
			t.Fatal("snapshot shares the live index tree")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotFileBackedDatabase makes sure capture works on a persistent
// database as well as the in-memory variant. A private database file is used
// so the test never contends with the shared data.db used by other tests.
func TestSnapshotFileBackedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("disk", "rock", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if v, err := snap.Get("disk"); err != nil || v != "rock" {
		t.Fatalf("file-backed snapshot Get = %q, %v", v, err)
	}
}

// TestViewLockSemanticsPreserved confirms the pre-existing transaction
// locking contract is untouched: a managed View holds the read lock for its
// whole duration and blocks writers.
func TestViewLockSemanticsPreserved(t *testing.T) {
	db := memSnapshotDB(t)
	defer db.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- db.View(func(tx *Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(func(tx *Tx) error {
			_, _, err := tx.Set("x", "y", nil)
			return err
		})
	}()
	select {
	case err := <-updateDone:
		t.Fatalf("Update proceeded while View was active: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	waitErr(t, viewDone, time.Second, "View")
	waitErr(t, updateDone, time.Second, "blocked Update")
}
