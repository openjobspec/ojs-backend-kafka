package state_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func uniqueJob(id string) *core.Job {
	return &core.Job{
		ID:    id,
		Type:  "task.unique",
		Queue: "q-unique",
		State: core.StateAvailable,
	}
}

// TestClaimUniqueJobConcurrentRejectCreatesExactlyOneJob covers finding #2: many
// concurrent reject claims for the same fingerprint must yield exactly one
// created job and one claim; the GET/inspect/SET race previously allowed two.
func TestClaimUniqueJobConcurrentRejectCreatesExactlyOneJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const n = 32
	const fingerprint = "fp-reject"

	var wg sync.WaitGroup
	outcomes := make(chan string, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-"+strconv.Itoa(i)), float64(i), false, 0, "reject", nil)
			if err != nil {
				errs <- err
				return
			}
			outcomes <- result.Outcome
		}(i)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	close(errs)

	for err := range errs {
		t.Fatalf("claim unique: %v", err)
	}
	claimed, rejected := 0, 0
	for outcome := range outcomes {
		switch outcome {
		case state.UniqueClaimClaimed:
			claimed++
		case state.UniqueClaimRejected:
			rejected++
		default:
			t.Fatalf("unexpected outcome %q", outcome)
		}
	}
	if claimed != 1 || rejected != n-1 {
		t.Fatalf("claimed=%d rejected=%d, want 1/%d", claimed, rejected, n-1)
	}

	members, err := raw.ZRange(ctx, "ojs:queue:q-unique:available", 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange available: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("available members = %v, want exactly one job", members)
	}
	claimID, err := raw.Get(ctx, "ojs:unique:"+fingerprint).Result()
	if err != nil || claimID != members[0] {
		t.Fatalf("claim %q does not point to the single created job %q (err=%v)", claimID, members[0], err)
	}
}

// TestClaimUniqueJobConcurrentIgnoreReturnsWinner covers the ignore policy under
// concurrency: exactly one claim wins and every other claim reports the winner.
func TestClaimUniqueJobConcurrentIgnoreReturnsWinner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const n = 24
	const fingerprint = "fp-ignore"

	var wg sync.WaitGroup
	type outcome struct {
		outcome  string
		existing string
	}
	results := make(chan outcome, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-"+strconv.Itoa(i)), float64(i), false, 0, "ignore", nil)
			if err != nil {
				errs <- err
				return
			}
			results <- outcome{result.Outcome, result.ExistingID}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("claim unique: %v", err)
	}
	claimID, err := raw.Get(ctx, "ojs:unique:"+fingerprint).Result()
	if err != nil {
		t.Fatalf("get claim: %v", err)
	}
	claimed, ignored := 0, 0
	for result := range results {
		switch result.outcome {
		case state.UniqueClaimClaimed:
			claimed++
		case state.UniqueClaimIgnored:
			ignored++
			if result.existing != claimID {
				t.Fatalf("ignored existing=%q, want winner %q", result.existing, claimID)
			}
		default:
			t.Fatalf("unexpected outcome %q", result.outcome)
		}
	}
	if claimed != 1 || ignored != n-1 {
		t.Fatalf("claimed=%d ignored=%d, want 1/%d", claimed, ignored, n-1)
	}
}

// TestReplaceUniqueJobConcurrentCreatesAtMostOneReplacement verifies that all
// racers compare against the same predecessor and only one can cancel it and
// create a replacement.
func TestReplaceUniqueJobConcurrentCreatesAtMostOneReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const n = 24
	const fingerprint = "fp-replace"

	// Seed an existing relevant claim.
	seed, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-seed"), 0, false, 0, "reject", nil)
	if err != nil || seed.Outcome != state.UniqueClaimClaimed {
		t.Fatalf("seed claim = %+v, err=%v", seed, err)
	}

	var wg sync.WaitGroup
	results := make(chan *state.UniqueClaimResult, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := store.ReplaceUniqueJob(
				ctx,
				fingerprint,
				"job-seed",
				uniqueJob("job-"+strconv.Itoa(i)),
				float64(i+1),
				false,
				0,
				nil,
				core.NowFormatted(),
			)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("replace claim: %v", err)
	}

	claimed, rejected := 0, 0
	for result := range results {
		switch result.Outcome {
		case state.UniqueClaimClaimed:
			claimed++
			if result.ExistingID != "job-seed" {
				t.Fatalf("winner replaced %q, want job-seed", result.ExistingID)
			}
		case state.UniqueClaimRejected:
			rejected++
			if result.ExistingID == "" || result.ExistingID == "job-seed" {
				t.Fatalf("loser returned stale existing ID %q", result.ExistingID)
			}
		default:
			t.Fatalf("unexpected outcome %q", result.Outcome)
		}
	}
	if claimed != 1 || rejected != n-1 {
		t.Fatalf("claimed=%d rejected=%d, want 1/%d", claimed, rejected, n-1)
	}

	seedJob, err := store.GetJob(ctx, "job-seed")
	if err != nil || seedJob.State != core.StateCancelled {
		t.Fatalf("seed after replacement = %+v, %v; want cancelled", seedJob, err)
	}
	claimID, err := raw.Get(ctx, "ojs:unique:"+fingerprint).Result()
	if err != nil {
		t.Fatalf("get claim: %v", err)
	}
	if claimID == "job-seed" {
		t.Fatalf("claim still points to predecessor %q", claimID)
	}
	members, err := raw.ZRange(ctx, "ojs:queue:q-unique:available", 0, -1).Result()
	if err != nil {
		t.Fatalf("list available: %v", err)
	}
	if len(members) != 1 || members[0] != claimID {
		t.Fatalf("available replacements = %v, claim=%q; want one winner", members, claimID)
	}
}

// TestClaimUniqueJobStaleClaimsAreOverwritten covers stale/non-relevant existing
// IDs: a claim pointing to a terminal or missing job is treated as free.
func TestClaimUniqueJobStaleClaimsAreOverwritten(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("terminal existing job", func(t *testing.T) {
		const fingerprint = "fp-stale-terminal"
		done := uniqueJob("job-done")
		done.State = core.StateCompleted
		if err := store.SaveJob(ctx, done); err != nil {
			t.Fatalf("save terminal job: %v", err)
		}
		if err := raw.Set(ctx, "ojs:unique:"+fingerprint, done.ID, 0).Err(); err != nil {
			t.Fatalf("seed stale claim: %v", err)
		}
		result, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-fresh-1"), 1, false, 0, "reject", nil)
		if err != nil || result.Outcome != state.UniqueClaimClaimed {
			t.Fatalf("claim over terminal = %+v, err=%v", result, err)
		}
		claimID, _ := raw.Get(ctx, "ojs:unique:"+fingerprint).Result()
		if claimID != "job-fresh-1" {
			t.Fatalf("claim = %q, want job-fresh-1", claimID)
		}
	})

	t.Run("missing existing job", func(t *testing.T) {
		const fingerprint = "fp-stale-missing"
		if err := raw.Set(ctx, "ojs:unique:"+fingerprint, "ghost-id", 0).Err(); err != nil {
			t.Fatalf("seed ghost claim: %v", err)
		}
		result, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-fresh-2"), 1, false, 0, "reject", nil)
		if err != nil || result.Outcome != state.UniqueClaimClaimed {
			t.Fatalf("claim over ghost = %+v, err=%v", result, err)
		}
	})
}

// TestClaimUniqueJobRelevantStateFilter covers the States filter: a claim is only
// relevant when the existing job's state is in the configured set.
func TestClaimUniqueJobRelevantStateFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("existing state outside filter is not relevant", func(t *testing.T) {
		const fingerprint = "fp-filter-free"
		existing := uniqueJob("job-available")
		existing.State = core.StateAvailable
		if err := store.SaveJob(ctx, existing); err != nil {
			t.Fatalf("save existing: %v", err)
		}
		if err := raw.Set(ctx, "ojs:unique:"+fingerprint, existing.ID, 0).Err(); err != nil {
			t.Fatalf("seed claim: %v", err)
		}
		// Only "active" is relevant, so an available job does not block the claim.
		result, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-new"), 1, false, 0, "reject", []string{core.StateActive})
		if err != nil || result.Outcome != state.UniqueClaimClaimed {
			t.Fatalf("filtered claim = %+v, err=%v", result, err)
		}
	})

	t.Run("existing state inside filter is relevant", func(t *testing.T) {
		const fingerprint = "fp-filter-block"
		existing := uniqueJob("job-active")
		existing.State = core.StateActive
		if err := store.SaveJob(ctx, existing); err != nil {
			t.Fatalf("save existing: %v", err)
		}
		if err := raw.Set(ctx, "ojs:unique:"+fingerprint, existing.ID, 0).Err(); err != nil {
			t.Fatalf("seed claim: %v", err)
		}
		result, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-new"), 1, false, 0, "reject", []string{core.StateActive})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if result.Outcome != state.UniqueClaimRejected || result.ExistingID != existing.ID {
			t.Fatalf("filtered reject = %+v, want rejected pointing at %q", result, existing.ID)
		}
	})
}

// TestClaimUniqueJobScheduledPlacement covers scheduled jobs: a claimed scheduled
// job lands in the scheduled set rather than a queue's available set.
func TestClaimUniqueJobScheduledPlacement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const fingerprint = "fp-scheduled"
	scheduledScore := float64(time.Now().Add(time.Hour).UnixMilli())

	job := uniqueJob("job-scheduled")
	job.State = core.StateScheduled
	result, err := store.ClaimUniqueJob(ctx, fingerprint, job, scheduledScore, true, 0, "reject", nil)
	if err != nil || result.Outcome != state.UniqueClaimClaimed {
		t.Fatalf("scheduled claim = %+v, err=%v", result, err)
	}
	scheduledMembers, err := raw.ZRange(ctx, "ojs:scheduled", 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange scheduled: %v", err)
	}
	if len(scheduledMembers) != 1 || scheduledMembers[0] != job.ID {
		t.Fatalf("scheduled members = %v, want [%s]", scheduledMembers, job.ID)
	}
	availableCount, err := raw.ZCard(ctx, "ojs:queue:q-unique:available").Result()
	if err != nil {
		t.Fatalf("zcard available: %v", err)
	}
	if availableCount != 0 {
		t.Fatalf("scheduled job leaked into available set: count=%d", availableCount)
	}
}

func TestReplaceUniqueJobCancelsEligiblePredecessorsBeforeCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, predecessorState := range []string{core.StateAvailable, core.StateScheduled, core.StateRetryable} {
		t.Run(predecessorState, func(t *testing.T) {
			fingerprint := "fp-replace-" + predecessorState
			old := uniqueJob("job-old-" + predecessorState)
			old.Queue = "q-replace-" + predecessorState
			scheduled := predecessorState == core.StateScheduled
			if scheduled {
				old.State = core.StateScheduled
			}
			seed, err := store.ClaimUniqueJob(ctx, fingerprint, old, 1, scheduled, 0, "reject", nil)
			if err != nil || seed.Outcome != state.UniqueClaimClaimed {
				t.Fatalf("seed = %+v, %v", seed, err)
			}
			if predecessorState == core.StateRetryable {
				if err := store.RemoveFromAvailable(ctx, old.Queue, old.ID); err != nil {
					t.Fatalf("remove available: %v", err)
				}
				if err := store.UpdateJob(ctx, old.ID, map[string]any{"state": core.StateRetryable}); err != nil {
					t.Fatalf("mark retryable: %v", err)
				}
				if err := store.AddToRetry(ctx, old.ID, time.Now().Add(time.Minute).UnixMilli()); err != nil {
					t.Fatalf("add retry: %v", err)
				}
			}

			replacement := uniqueJob("job-new-" + predecessorState)
			replacement.Queue = old.Queue
			result, err := store.ReplaceUniqueJob(
				ctx,
				fingerprint,
				old.ID,
				replacement,
				2,
				false,
				0,
				nil,
				core.NowFormatted(),
			)
			if err != nil || result.Outcome != state.UniqueClaimClaimed || result.ExistingID != old.ID {
				t.Fatalf("replace = %+v, %v", result, err)
			}
			cancelled, err := store.GetJob(ctx, old.ID)
			if err != nil || cancelled.State != core.StateCancelled || cancelled.CancelledAt == "" {
				t.Fatalf("predecessor = %+v, %v; want cancelled", cancelled, err)
			}
			if members, err := raw.ZRange(ctx, "ojs:queue:"+old.Queue+":available", 0, -1).Result(); err != nil || len(members) != 1 || members[0] != replacement.ID {
				t.Fatalf("available members = %v, %v; want only replacement", members, err)
			}
			if scheduledMembers, err := raw.ZRange(ctx, "ojs:scheduled", 0, -1).Result(); err != nil || len(scheduledMembers) != 0 {
				t.Fatalf("scheduled predecessor was not removed: %v, %v", scheduledMembers, err)
			}
			if retryMembers, err := raw.ZRange(ctx, "ojs:retry", 0, -1).Result(); err != nil || len(retryMembers) != 0 {
				t.Fatalf("retry predecessor was not removed: %v, %v", retryMembers, err)
			}
			claimID, err := store.GetUniqueJobID(ctx, fingerprint)
			if err != nil || claimID != replacement.ID {
				t.Fatalf("claim = %q, %v; want replacement", claimID, err)
			}
		})
	}
}

func TestReplaceUniqueJobRejectsActivePredecessor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()
	ctx := context.Background()
	const fingerprint = "fp-replace-active"

	old := uniqueJob("job-old-active")
	if _, err := store.ClaimUniqueJob(ctx, fingerprint, old, 1, false, 0, "reject", nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fetchedID, err := store.AtomicFetch(
		ctx,
		old.Queue,
		core.FormatTime(time.Now().Add(time.Minute)),
		core.NowFormatted(),
		"worker-1",
	)
	if err != nil || fetchedID != old.ID {
		t.Fatalf("fetch predecessor = %q, %v", fetchedID, err)
	}

	replacement := uniqueJob("job-new-active")
	result, err := store.ReplaceUniqueJob(
		ctx, fingerprint, old.ID, replacement, 2, false, 0, nil, core.NowFormatted(),
	)
	if err != nil {
		t.Fatalf("replace active: %v", err)
	}
	if result.Outcome != state.UniqueClaimRejected || result.ExistingID != old.ID || result.ExistingState != core.StateActive {
		t.Fatalf("active result = %+v", result)
	}
	if exists, err := raw.Exists(ctx, "ojs:job:"+replacement.ID).Result(); err != nil || exists != 0 {
		t.Fatalf("active conflict exposed replacement: exists=%d, %v", exists, err)
	}
	persisted, err := store.GetJob(ctx, old.ID)
	if err != nil || persisted.State != core.StateActive {
		t.Fatalf("active predecessor mutated: %+v, %v", persisted, err)
	}
	claimID, err := store.GetUniqueJobID(ctx, fingerprint)
	if err != nil || claimID != old.ID {
		t.Fatalf("claim = %q, %v; want active predecessor", claimID, err)
	}
}

func TestReplaceUniqueJobFailureLeavesPredecessorAndClaimIntact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()
	ctx := context.Background()
	const fingerprint = "fp-replace-failure"

	old := uniqueJob("job-old-failure")
	if _, err := store.ClaimUniqueJob(ctx, fingerprint, old, 1, false, 0, "reject", nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	replacement := uniqueJob("job-new-failure")
	replacement.Queue = "broken"
	if err := raw.Set(ctx, "ojs:queue:broken:available", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poison replacement index: %v", err)
	}

	if _, err := store.ReplaceUniqueJob(
		ctx, fingerprint, old.ID, replacement, 2, false, 0, nil, core.NowFormatted(),
	); err == nil {
		t.Fatal("expected replacement preflight failure")
	}
	persisted, err := store.GetJob(ctx, old.ID)
	if err != nil || persisted.State != core.StateAvailable {
		t.Fatalf("predecessor partially cancelled: %+v, %v", persisted, err)
	}
	claimID, err := store.GetUniqueJobID(ctx, fingerprint)
	if err != nil || claimID != old.ID {
		t.Fatalf("claim changed after failure: %q, %v", claimID, err)
	}
	if exists, err := raw.Exists(ctx, "ojs:job:"+replacement.ID).Result(); err != nil || exists != 0 {
		t.Fatalf("replacement created after failure: exists=%d, %v", exists, err)
	}
}

// TestClaimUniqueJobFailureLeavesNoDanglingClaim covers finding #2's failure
// case: a WRONGTYPE preflight error must not create a job or leave a claim.
func TestClaimUniqueJobFailureLeavesNoDanglingClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const fingerprint = "fp-wrongtype"

	// Poison the available set with the wrong type.
	if err := raw.Set(ctx, "ojs:queue:q-unique:available", "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("seed wrong type: %v", err)
	}

	if _, err := store.ClaimUniqueJob(ctx, fingerprint, uniqueJob("job-doomed"), 1, false, 0, "reject", nil); err == nil {
		t.Fatal("expected WRONGTYPE-safe preflight failure")
	}

	if exists, err := raw.Exists(ctx, "ojs:unique:"+fingerprint).Result(); err != nil || exists != 0 {
		t.Fatalf("dangling unique claim after failure: exists=%d err=%v", exists, err)
	}
	if exists, err := raw.Exists(ctx, "ojs:job:job-doomed").Result(); err != nil || exists != 0 {
		t.Fatalf("job hash created despite failure: exists=%d err=%v", exists, err)
	}
}
