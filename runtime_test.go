package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qmuntal/stateless"
)

const (
	stateIdle    = "idle"
	stateWorking = "working"
	stateDone    = "done"

	triggerStart  = "start"
	triggerFinish = "finish"
)

type testDefinition struct {
	handlers map[string]EntryHandler
	ignored  map[string][]stateless.Trigger
}

func (d testDefinition) Configure(rules *Rules) {
	idle := rules.Configure(stateIdle).
		Permit(triggerStart, stateWorking).
		Permit(triggerFinish, stateDone)
	rules.Configure(stateWorking).
		Permit(triggerFinish, stateDone)
	for _, trigger := range d.ignored[stateIdle] {
		idle.Ignore(trigger)
	}
}

func (d testDefinition) IsTerminal(state stateless.State) bool {
	return state == stateDone
}

func (d testDefinition) EntryHandler(state stateless.State) (EntryHandler, bool) {
	name, err := encodeSymbol("state", state)
	if err != nil {
		return nil, false
	}
	handler, ok := d.handlers[name]
	return handler, ok
}

type customDefinition struct {
	configure func(*Rules)
	terminal  func(stateless.State) bool
	handler   func(stateless.State) (EntryHandler, bool)
}

func (d customDefinition) Configure(rules *Rules) {
	if d.configure != nil {
		d.configure(rules)
	}
}

func (d customDefinition) IsTerminal(state stateless.State) bool {
	if d.terminal == nil {
		return false
	}
	return d.terminal(state)
}

func (d customDefinition) EntryHandler(state stateless.State) (EntryHandler, bool) {
	if d.handler == nil {
		return nil, false
	}
	return d.handler(state)
}

type providerCase struct {
	name string
	new  func(*testing.T) Provider
}

func providerCases() []providerCase {
	return []providerCase{
		{
			name: "memory",
			new: func(t *testing.T) Provider {
				t.Helper()
				return NewMemoryProvider()
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T) Provider {
				t.Helper()
				provider, err := OpenSQLiteProvider(filepath.Join(t.TempDir(), "machines.db"))
				if err != nil {
					t.Fatalf("open sqlite provider: %v", err)
				}
				t.Cleanup(func() {
					if err := provider.Close(); err != nil {
						t.Fatalf("close sqlite provider: %v", err)
					}
				})
				return provider
			},
		},
	}
}

func TestCreateMachineUsesConfiguredSharder(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			sharder := MustHashSharder(32)
			rt := newTestRuntime(t, provider, testDefinition{}, WithSharder(sharder))

			if err := rt.CreateMachine(ctx, MachineInit{ID: "m1", State: stateIdle}); err != nil {
				t.Fatalf("create machine: %v", err)
			}
			snap := readMachine(t, ctx, provider, "m1")
			if snap.ShardID != sharder.ShardForMachine("m1") {
				t.Fatalf("expected shard %d, got %d", sharder.ShardForMachine("m1"), snap.ShardID)
			}
		})
	}
}

func TestRejectsNegativeShardIDs(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{}, WithRetryPolicy(RetryPolicy{}))

			if err := rt.CreateMachineInShard(ctx, -1, MachineInit{ID: "m1", State: stateIdle}); !errors.Is(err, ErrInvalidShard) {
				t.Fatalf("expected invalid shard on create, got %v", err)
			}
			worker := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{-1}})
			if _, err := worker.Work(ctx, 1); !errors.Is(err, ErrInvalidShard) {
				t.Fatalf("expected invalid shard on worker, got %v", err)
			}
		})
	}
}

func TestSignalInvalidTriggerDoesNotMutateState(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{}, WithRetryPolicy(RetryPolicy{}))
			createMachine(t, ctx, rt, "m1", 7, stateIdle)

			if err := rt.Signal(ctx, NewSignal("s1", "m1", "bogus")); err != nil {
				t.Fatalf("signal: %v", err)
			}
			processed, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{7}}).Work(ctx, 10)
			if err == nil {
				t.Fatal("expected invalid trigger error")
			}
			if processed != 0 {
				t.Fatalf("expected zero processed signals, got %d", processed)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 || snap.Terminal() {
				t.Fatalf("unexpected snapshot after failed signal: %+v", snap)
			}
			signals := claimSignals(t, ctx, provider, "inspector", 7, 10, time.Second)
			if len(signals) != 1 || signals[0].Status != EntryProcessing || signals[0].LastError == "" {
				t.Fatalf("expected failed signal to be claimable with error, got %+v", signals)
			}
		})
	}
}

func TestSignalCommitsTransitionAndCreatesPendingEntry(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			}, WithLeaseDuration(-time.Second))
			createMachine(t, ctx, rt, "m1", 1, stateIdle)

			processSignal(t, ctx, rt, provider, "worker", 1, NewSignal("s1", "m1", triggerStart, "payload"))

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateWorking || snap.Version != 1 || snap.Terminal() {
				t.Fatalf("unexpected snapshot after signal: %+v", snap)
			}
			if len(snap.Args) != 1 || snap.Args[0] != "payload" {
				t.Fatalf("unexpected snapshot args: %#v", snap.Args)
			}

			entries := claimEntries(t, ctx, provider, "worker", 1, 10, time.Second)
			if len(entries) != 1 {
				t.Fatalf("expected one entry, got %d", len(entries))
			}
			entry := entries[0]
			if entry.Key != (EntryKey{MachineID: "m1", Version: 1}) ||
				entry.SourceState != stateIdle ||
				entry.DestState != stateWorking ||
				entry.Trigger != triggerStart ||
				entry.Status != EntryProcessing ||
				entry.Owner != "worker" ||
				entry.Attempts != 1 {
				t.Fatalf("unexpected claimed entry: %+v", entry)
			}
		})
	}
}

func TestTerminalSignalCreatesNoEntry(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			}, WithLeaseDuration(-time.Second))
			createMachine(t, ctx, rt, "m1", 3, stateIdle)

			processSignal(t, ctx, rt, provider, "worker", 3, NewSignal("s1", "m1", triggerFinish))

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateDone || snap.Version != 1 || !snap.Terminal() {
				t.Fatalf("unexpected terminal snapshot: %+v", snap)
			}
			entries := claimEntries(t, ctx, provider, "worker", 3, 10, time.Second)
			if len(entries) != 0 {
				t.Fatalf("expected no terminal entries, got %+v", entries)
			}
		})
	}
}

func TestLazyRecoveryRetriesSameEntryKeyAfterCrash(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var calls []EntryKey
			handler := func(_ context.Context, entry Entry) (HandlerResult, error) {
				calls = append(calls, entry.Key)
				return FireNext(triggerFinish), nil
			}

			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{stateWorking: handler},
			})
			createMachine(t, ctx, rt, "m1", 4, stateIdle)
			processSignal(t, ctx, rt, provider, "worker-a", 4, NewSignal("s1", "m1", triggerStart))

			claimed := claimEntries(t, ctx, provider, "worker-a", 4, 1, -time.Second)
			if len(claimed) != 1 {
				t.Fatalf("expected one claimed entry, got %d", len(claimed))
			}
			if _, err := handler(ctx, claimed[0]); err != nil {
				t.Fatalf("simulate handler side effect: %v", err)
			}

			processed, err := rt.Worker(WorkerConfig{ID: "worker-b", Shards: []ShardID{4}}).Work(ctx, 10)
			if err != nil {
				t.Fatalf("recover worker-b: %v", err)
			}
			if processed != 1 {
				t.Fatalf("expected one recovered entry, got %d", processed)
			}
			if len(calls) != 2 || calls[0] != (EntryKey{MachineID: "m1", Version: 1}) || calls[1] != calls[0] {
				t.Fatalf("expected retry with same idempotency key, got %+v", calls)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateDone || snap.Version != 2 || !snap.Terminal() {
				t.Fatalf("expected recovered terminal machine, got %+v", snap)
			}
		})
	}
}

func TestEntryCompletionAndNextTriggerAreAtomic(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return FireNext("bogus"), nil
					},
				},
			}, WithRetryPolicy(RetryPolicy{}))
			createMachine(t, ctx, rt, "m1", 5, stateIdle)
			processSignal(t, ctx, rt, provider, "worker", 5, NewSignal("s1", "m1", triggerStart))

			processed, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{5}}).Work(ctx, 10)
			if err == nil {
				t.Fatal("expected invalid next trigger error")
			}
			if processed != 0 {
				t.Fatalf("expected zero completed entries, got %d", processed)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateWorking || snap.Version != 1 || snap.Terminal() {
				t.Fatalf("completion should have rolled back with next trigger: %+v", snap)
			}
			reclaimed := claimEntries(t, ctx, provider, "inspector", 5, 10, time.Second)
			if len(reclaimed) != 1 || reclaimed[0].Attempts != 2 {
				t.Fatalf("expected failed entry to be claimable, got %+v", reclaimed)
			}
		})
	}
}

func TestWorkerRecoversOpenEntryBeforeSignalWithNormalLease(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			createMachine(t, ctx, rt, "m1", 10, stateIdle)
			processSignal(t, ctx, rt, provider, "worker", 10, NewSignal("s1", "m1", triggerStart))

			if err := rt.Signal(ctx, NewSignal("s2", "m1", triggerFinish)); err != nil {
				t.Fatalf("signal finish: %v", err)
			}
			processed, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{10}}).Work(ctx, 10)
			if err != nil {
				t.Fatalf("recover entry before signal: %v", err)
			}
			if processed != 2 {
				t.Fatalf("expected entry and signal to process without waiting for lease expiry, got %d", processed)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateDone || snap.Version != 2 || !snap.Terminal() {
				t.Fatalf("expected terminal snapshot after same work pass: %+v", snap)
			}
		})
	}
}

func TestSignalDeduplicatesByID(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 8, stateIdle)

			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart, "a")); err != nil {
				t.Fatalf("first signal: %v", err)
			}
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart, "a")); err != nil {
				t.Fatalf("duplicate signal should be idempotent: %v", err)
			}
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart, "b")); !errors.Is(err, ErrSignalConflict) {
				t.Fatalf("expected signal conflict, got %v", err)
			}
			signals := claimSignals(t, ctx, provider, "worker", 8, 10, time.Second)
			if len(signals) != 1 {
				t.Fatalf("expected one deduped signal, got %+v", signals)
			}
		})
	}
}

func TestCommitUsesExpectedVersionAndRollsBackCompletions(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			createMachine(t, ctx, rt, "m1", 16, stateIdle)

			_, err := provider.Commit(ctx, AtomicCommit{
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 1,
					Record: TransitionRecord{
						SourceState: stateIdle,
						DestState:   stateWorking,
						Trigger:     triggerStart,
					},
				},
			})
			if !errors.Is(err, ErrVersionConflict) {
				t.Fatalf("expected version conflict, got %v", err)
			}

			processSignal(t, ctx, rt, provider, "worker", 16, NewSignal("s1", "m1", triggerStart))
			claimed := claimEntries(t, ctx, provider, "worker", 16, 1, -time.Second)
			if len(claimed) != 1 {
				t.Fatalf("expected one claimed entry, got %+v", claimed)
			}
			_, err = provider.Commit(ctx, AtomicCommit{
				CompleteEntry: &CompleteEntryCommand{Key: claimed[0].Key, Owner: "worker", Attempt: claimed[0].Attempts},
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 0,
					Record: TransitionRecord{
						SourceState: stateWorking,
						DestState:   stateDone,
						Trigger:     triggerFinish,
						Terminal:    true,
					},
				},
			})
			if !errors.Is(err, ErrVersionConflict) {
				t.Fatalf("expected version conflict, got %v", err)
			}
			reclaimed := claimEntries(t, ctx, provider, "worker-2", 16, 1, -time.Second)
			if len(reclaimed) != 1 || reclaimed[0].Key != claimed[0].Key {
				t.Fatalf("entry completion should roll back with failed transition, got %+v", reclaimed)
			}
		})
	}
}

func TestStaleEntryOwnerCannotCommitAfterLeaseIsStolen(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 17, stateIdle)

			_, err := provider.Commit(ctx, AtomicCommit{
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 0,
					Record: TransitionRecord{
						SourceState: stateIdle,
						DestState:   stateWorking,
						Trigger:     triggerStart,
						CreateEntry: true,
					},
				},
			})
			if err != nil {
				t.Fatalf("seed entry: %v", err)
			}

			claimedA := claimEntries(t, ctx, provider, "worker", 17, 1, -time.Second)
			if len(claimedA) != 1 {
				t.Fatalf("expected first claim, got %+v", claimedA)
			}
			claimedB := claimEntries(t, ctx, provider, "worker", 17, 1, time.Second)
			if len(claimedB) != 1 || claimedB[0].Key != claimedA[0].Key {
				t.Fatalf("expected same owner to reclaim expired lease, got %+v", claimedB)
			}

			_, err = provider.Commit(ctx, AtomicCommit{
				CompleteEntry: &CompleteEntryCommand{Key: claimedB[0].Key, Owner: "worker", Attempt: claimedB[0].Attempts},
			})
			if err != nil {
				t.Fatalf("complete with second claim: %v", err)
			}

			_, err = provider.Commit(ctx, AtomicCommit{
				CompleteEntry: &CompleteEntryCommand{Key: claimedA[0].Key, Owner: "worker", Attempt: claimedA[0].Attempts},
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 1,
					Record: TransitionRecord{
						SourceState: stateWorking,
						DestState:   stateDone,
						Trigger:     triggerFinish,
						Terminal:    true,
					},
				},
			})
			if !errors.Is(err, ErrEntryNotOwned) {
				t.Fatalf("expected stale owner to be rejected, got %v", err)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateWorking || snap.Version != 1 || snap.Terminal() {
				t.Fatalf("stale owner should not advance machine: %+v", snap)
			}
		})
	}
}

func TestStaleSignalOwnerCannotCommitAfterLeaseIsStolen(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 18, stateIdle)
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart)); err != nil {
				t.Fatalf("signal: %v", err)
			}

			claimedA := claimSignals(t, ctx, provider, "worker", 18, 1, -time.Second)
			if len(claimedA) != 1 {
				t.Fatalf("expected first signal claim, got %+v", claimedA)
			}
			claimedB := claimSignals(t, ctx, provider, "worker", 18, 1, time.Second)
			if len(claimedB) != 1 || claimedB[0].ID != claimedA[0].ID {
				t.Fatalf("expected same owner to reclaim expired signal lease, got %+v", claimedB)
			}

			_, err := provider.Commit(ctx, AtomicCommit{
				CompleteSignal: &CompleteSignalCommand{ID: claimedB[0].ID, Owner: "worker", Attempt: claimedB[0].Attempts},
			})
			if err != nil {
				t.Fatalf("complete signal with second claim: %v", err)
			}

			_, err = provider.Commit(ctx, AtomicCommit{
				CompleteSignal: &CompleteSignalCommand{ID: claimedA[0].ID, Owner: "worker", Attempt: claimedA[0].Attempts},
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 0,
					Record: TransitionRecord{
						SourceState: stateIdle,
						DestState:   stateWorking,
						Trigger:     triggerStart,
					},
				},
			})
			if !errors.Is(err, ErrSignalNotOwned) {
				t.Fatalf("expected stale signal owner to be rejected, got %v", err)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 || snap.Terminal() {
				t.Fatalf("stale signal owner should not advance machine: %+v", snap)
			}
		})
	}
}

func TestHandlerCanAtomicallyEmitSignals(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(_ context.Context, entry Entry) (HandlerResult, error) {
						return HandlerResult{
							Signals: []Signal{NewSignal(entry.Key.String()+":notify", "m2", triggerFinish)},
						}, nil
					},
				},
			})
			createMachine(t, ctx, rt, "m1", 21, stateIdle)
			createMachine(t, ctx, rt, "m2", 22, stateIdle)
			processSignal(t, ctx, rt, provider, "worker-a", 21, NewSignal("s1", "m1", triggerStart))

			processed, err := rt.Worker(WorkerConfig{ID: "worker-a", Shards: []ShardID{21}}).Work(ctx, 10)
			if err != nil {
				t.Fatalf("process emitting entry: %v", err)
			}
			if processed != 1 {
				t.Fatalf("expected one entry processed, got %d", processed)
			}
			signals := claimSignals(t, ctx, provider, "worker-b", 22, 10, time.Second)
			if len(signals) != 1 || signals[0].MachineID != "m2" || signals[0].Trigger != triggerFinish {
				t.Fatalf("expected emitted signal for m2, got %+v", signals)
			}
		})
	}
}

func TestHandlerSignalConflictRollsBackEntryCompletion(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(_ context.Context, entry Entry) (HandlerResult, error) {
						return EmitSignals(NewSignal(entry.Key.String()+":notify", "m2", triggerFinish, "new")), nil
					},
				},
			}, WithRetryPolicy(RetryPolicy{}))
			createMachine(t, ctx, rt, "m1", 23, stateIdle)
			createMachine(t, ctx, rt, "m2", 24, stateIdle)
			if err := rt.Signal(ctx, NewSignal("m1/1:notify", "m2", triggerFinish, "old")); err != nil {
				t.Fatalf("seed conflicting signal: %v", err)
			}
			processSignal(t, ctx, rt, provider, "worker-a", 23, NewSignal("s1", "m1", triggerStart))

			processed, err := rt.Worker(WorkerConfig{ID: "worker-a", Shards: []ShardID{23}}).Work(ctx, 10)
			if !errors.Is(err, ErrSignalConflict) {
				t.Fatalf("expected signal conflict, got %v", err)
			}
			if processed != 0 {
				t.Fatalf("conflicting output should not complete entry, processed %d", processed)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateWorking || snap.Version != 1 || snap.Terminal() {
				t.Fatalf("output conflict should not advance machine: %+v", snap)
			}
			reclaimed := claimEntries(t, ctx, provider, "worker-b", 23, 10, time.Second)
			if len(reclaimed) != 1 || reclaimed[0].Key != (EntryKey{MachineID: "m1", Version: 1}) ||
				reclaimed[0].LastError == "" {
				t.Fatalf("entry completion should roll back and become retryable, got %+v", reclaimed)
			}

			signals := claimSignals(t, ctx, provider, "worker-c", 24, 10, time.Second)
			if len(signals) != 1 || signals[0].ID != "m1/1:notify" || len(signals[0].Args) != 1 || signals[0].Args[0] != "old" {
				t.Fatalf("original signal should remain unchanged, got %+v", signals)
			}
		})
	}
}

func TestClaimEntriesHonorsLimitAndLeases(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("m%d", i)
				createMachine(t, ctx, rt, id, 9, stateIdle)
				processSignal(t, ctx, rt, provider, "signal-worker", 9, NewSignal("s-"+id, id, triggerStart))
			}

			first := claimEntries(t, ctx, provider, "worker-a", 9, 2, time.Minute)
			if len(first) != 2 {
				t.Fatalf("expected first claim limit of 2, got %d", len(first))
			}

			second := claimEntries(t, ctx, provider, "worker-b", 9, 10, time.Minute)
			if len(second) != 1 {
				t.Fatalf("expected only unleased entry, got %d", len(second))
			}
		})
	}
}

func TestClaimEntriesIgnoresStaleVersions(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 11, stateIdle)

			_, err := provider.Commit(ctx, AtomicCommit{
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 0,
					Record: TransitionRecord{
						SourceState: stateIdle,
						DestState:   stateWorking,
						Trigger:     triggerStart,
						CreateEntry: true,
					},
				},
			})
			if err != nil {
				t.Fatalf("seed first transition: %v", err)
			}
			claimed := claimEntries(t, ctx, provider, "worker", 11, 1, time.Second)
			if len(claimed) != 1 {
				t.Fatalf("expected first entry claim, got %+v", claimed)
			}
			_, err = provider.Commit(ctx, AtomicCommit{
				CompleteEntry: &CompleteEntryCommand{Key: claimed[0].Key, Owner: "worker", Attempt: claimed[0].Attempts},
			})
			if err != nil {
				t.Fatalf("complete first entry: %v", err)
			}
			_, err = provider.Commit(ctx, AtomicCommit{
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 1,
					Record: TransitionRecord{
						SourceState: stateWorking,
						DestState:   "blocked",
						Trigger:     "advance",
						CreateEntry: true,
					},
				},
			})
			if err != nil {
				t.Fatalf("seed second transition: %v", err)
			}

			entries := claimEntries(t, ctx, provider, "worker", 11, 10, time.Second)
			if len(entries) != 1 || entries[0].Key != (EntryKey{MachineID: "m1", Version: 2}) {
				t.Fatalf("expected only current entry version, got %+v", entries)
			}
		})
	}
}

func TestTerminalTransitionCannotCreateEntryWork(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 15, stateIdle)

			_, err := provider.Commit(ctx, AtomicCommit{
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 0,
					Record: TransitionRecord{
						SourceState: stateIdle,
						DestState:   stateDone,
						Trigger:     triggerFinish,
						Terminal:    true,
						CreateEntry: true,
					},
				},
			})
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected invalid transition, got %v", err)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 || snap.Terminal() {
				t.Fatalf("invalid terminal entry should not mutate state: %+v", snap)
			}
		})
	}
}

func TestSignalArgsAreClonedBeforeStorage(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 19, stateIdle)

			payload := map[string]any{"value": "before"}
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart, payload)); err != nil {
				t.Fatalf("signal: %v", err)
			}
			payload["value"] = "after"

			signals := claimSignals(t, ctx, provider, "worker", 19, 1, time.Second)
			if len(signals) != 1 || len(signals[0].Args) != 1 {
				t.Fatalf("expected one signal with args, got %+v", signals)
			}
			got, ok := signals[0].Args[0].(map[string]any)
			if !ok || got["value"] != "before" {
				t.Fatalf("stored args should not alias caller mutation, got %#v", signals[0].Args)
			}
		})
	}
}

func TestWorkContinuesAfterEntryFailure(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handlerErr := errors.New("handler failed")
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(_ context.Context, entry Entry) (HandlerResult, error) {
						if entry.Key.MachineID == "m1" {
							return NoNext(), handlerErr
						}
						return NoNext(), nil
					},
				},
			}, WithRetryPolicy(RetryPolicy{}))
			for _, id := range []string{"m1", "m2"} {
				createMachine(t, ctx, rt, id, 12, stateIdle)
				processSignal(t, ctx, rt, provider, "signal-worker", 12, NewSignal("s-"+id, id, triggerStart))
			}

			processed, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{12}}).Work(ctx, 10)
			if !errors.Is(err, handlerErr) {
				t.Fatalf("expected joined handler error, got %v", err)
			}
			if processed != 1 {
				t.Fatalf("expected one successful entry despite failure, got %d", processed)
			}

			reclaimed := claimEntries(t, ctx, provider, "worker-2", 12, 10, time.Second)
			if len(reclaimed) != 1 || reclaimed[0].Key.MachineID != "m1" {
				t.Fatalf("expected only failed entry to remain claimable, got %+v", reclaimed)
			}
		})
	}
}

func TestStatelessPanicFailsSignalWithoutStateMutation(t *testing.T) {
	funcConfigure := func(rules *Rules) {
		rules.SetTriggerParameters(triggerStart, reflect.TypeOf(0))
		rules.Configure(stateIdle).Permit(triggerStart, stateWorking)
	}

	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, customDefinition{
				configure: funcConfigure,
				terminal: func(state stateless.State) bool {
					return state == stateDone
				},
			})
			createMachine(t, ctx, rt, "m1", 13, stateIdle)
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart, "wrong")); err != nil {
				t.Fatalf("signal: %v", err)
			}

			_, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{13}}).Work(ctx, 10)
			if err == nil || !strings.Contains(err.Error(), "stateless panic") {
				t.Fatalf("expected stateless panic to become error, got %v", err)
			}
			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 {
				t.Fatalf("panic should not mutate state: %+v", snap)
			}
		})
	}
}

func TestNilEntryHandlerFailsSignalWithoutStateMutation(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, customDefinition{
				configure: func(rules *Rules) {
					rules.Configure(stateIdle).Permit(triggerStart, stateWorking)
				},
				terminal: func(state stateless.State) bool {
					return state == stateDone
				},
				handler: func(state stateless.State) (EntryHandler, bool) {
					if state == stateWorking {
						return nil, true
					}
					return nil, false
				},
			})
			createMachine(t, ctx, rt, "m1", 14, stateIdle)
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart)); err != nil {
				t.Fatalf("signal: %v", err)
			}

			_, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{14}}).Work(ctx, 10)
			if !errors.Is(err, ErrNilEntryHandler) {
				t.Fatalf("expected nil handler error, got %v", err)
			}
			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 {
				t.Fatalf("nil handler should not mutate state: %+v", snap)
			}
		})
	}
}

func TestSQLitePersistsMachinesSignalsAndEntriesAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "machines.db")

	provider, err := OpenSQLiteProvider(path)
	if err != nil {
		t.Fatalf("open sqlite provider: %v", err)
	}
	rt := newTestRuntime(t, provider, testDefinition{
		handlers: map[string]EntryHandler{
			stateWorking: func(context.Context, Entry) (HandlerResult, error) {
				return NoNext(), nil
			},
		},
	})
	createMachine(t, ctx, rt, "m1", 2, stateIdle)
	if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart)); err != nil {
		t.Fatalf("signal: %v", err)
	}
	processClaimedSignal(t, ctx, rt, provider, "worker", 2)
	if err := provider.Close(); err != nil {
		t.Fatalf("close first provider: %v", err)
	}

	reopened, err := OpenSQLiteProvider(path)
	if err != nil {
		t.Fatalf("reopen sqlite provider: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened provider: %v", err)
		}
	})
	if err := reopened.Migrate(ctx); err != nil {
		t.Fatalf("migrate reopened provider: %v", err)
	}

	snap := readMachine(t, ctx, reopened, "m1")
	if snap.State != stateWorking || snap.Version != 1 {
		t.Fatalf("unexpected persisted snapshot: %+v", snap)
	}
	entries := claimEntries(t, ctx, reopened, "worker", 2, 10, time.Second)
	if len(entries) != 1 || entries[0].Key != (EntryKey{MachineID: "m1", Version: 1}) {
		t.Fatalf("unexpected persisted entries: %+v", entries)
	}
	signals := claimSignals(t, ctx, reopened, "worker", 2, 10, time.Second)
	if len(signals) != 0 {
		t.Fatalf("completed signal should not be claimable, got %+v", signals)
	}
}

func TestProcessEntryFailsEntryWhenHandlerFails(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handlerErr := errors.New("handler failed")
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), handlerErr
					},
				},
			}, WithRetryPolicy(RetryPolicy{}))
			createMachine(t, ctx, rt, "m1", 6, stateIdle)
			processSignal(t, ctx, rt, provider, "signal-worker", 6, NewSignal("s1", "m1", triggerStart))

			_, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{6}}).Work(ctx, 10)
			if !errors.Is(err, handlerErr) {
				t.Fatalf("expected handler error, got %v", err)
			}

			reclaimed := claimEntries(t, ctx, provider, "worker-2", 6, 10, time.Second)
			if len(reclaimed) != 1 || reclaimed[0].LastError != handlerErr.Error() {
				t.Fatalf("expected failed entry with error, got %+v", reclaimed)
			}
		})
	}
}

func TestRetryBackoffDelaysFailedEntryAndSignal(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handlerErr := errors.New("handler failed")
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), handlerErr
					},
				},
			}, WithRetryPolicy(RetryPolicy{InitialBackoff: 30 * time.Millisecond}))
			createMachine(t, ctx, rt, "entry-machine", 30, stateIdle)
			processSignal(t, ctx, rt, provider, "signal-worker", 30, NewSignal("s-entry", "entry-machine", triggerStart))

			_, err := rt.Worker(WorkerConfig{ID: "entry-worker", Shards: []ShardID{30}}).Work(ctx, 10)
			if !errors.Is(err, handlerErr) {
				t.Fatalf("expected handler error, got %v", err)
			}
			if entries := claimEntries(t, ctx, provider, "inspector", 30, 10, time.Second); len(entries) != 0 {
				t.Fatalf("failed entry should wait for retry_at, got %+v", entries)
			}
			time.Sleep(45 * time.Millisecond)
			entries := claimEntries(t, ctx, provider, "inspector", 30, 10, time.Second)
			if len(entries) != 1 || entries[0].Attempts != 2 || entries[0].LastError != handlerErr.Error() {
				t.Fatalf("expected entry after backoff with second attempt, got %+v", entries)
			}

			createMachine(t, ctx, rt, "signal-machine", 31, stateIdle)
			if err := rt.Signal(ctx, NewSignal("s-bogus", "signal-machine", "bogus")); err != nil {
				t.Fatalf("signal bogus: %v", err)
			}
			_, err = rt.Worker(WorkerConfig{ID: "signal-worker", Shards: []ShardID{31}}).Work(ctx, 10)
			if err == nil {
				t.Fatal("expected invalid signal error")
			}
			if signals := claimSignals(t, ctx, provider, "inspector", 31, 10, time.Second); len(signals) != 0 {
				t.Fatalf("failed signal should wait for retry_at, got %+v", signals)
			}
			time.Sleep(45 * time.Millisecond)
			signals := claimSignals(t, ctx, provider, "inspector", 31, 10, time.Second)
			if len(signals) != 1 || signals[0].Attempts != 2 || signals[0].LastError == "" {
				t.Fatalf("expected signal after backoff with second attempt, got %+v", signals)
			}
		})
	}
}

func TestRetryPolicyDeadLettersAfterMaxAttempts(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handlerErr := errors.New("handler failed")
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), handlerErr
					},
				},
			}, WithRetryPolicy(RetryPolicy{MaxAttempts: 1}))
			createMachine(t, ctx, rt, "m1", 32, stateIdle)
			processSignal(t, ctx, rt, provider, "signal-worker", 32, NewSignal("s1", "m1", triggerStart))

			_, err := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{32}}).Work(ctx, 10)
			if !errors.Is(err, handlerErr) {
				t.Fatalf("expected handler error, got %v", err)
			}
			if entries := claimEntries(t, ctx, provider, "worker-2", 32, 10, -time.Second); len(entries) != 0 {
				t.Fatalf("dead-lettered entry should not be claimable, got %+v", entries)
			}
			_, err = provider.Commit(ctx, AtomicCommit{
				Transition: &CommitTransition{
					MachineID:       "m1",
					ExpectedVersion: 1,
					Record: TransitionRecord{
						SourceState: stateWorking,
						DestState:   stateDone,
						Trigger:     triggerFinish,
						Terminal:    true,
					},
				},
			})
			if !errors.Is(err, ErrEntryInProgress) {
				t.Fatalf("dead-lettered entry should still block machine advancement, got %v", err)
			}

			createMachine(t, ctx, rt, "m2", 33, stateIdle)
			if err := rt.Signal(ctx, NewSignal("s2", "m2", "bogus")); err != nil {
				t.Fatalf("signal bogus: %v", err)
			}
			_, err = rt.Worker(WorkerConfig{ID: "signal-worker", Shards: []ShardID{33}}).Work(ctx, 10)
			if err == nil {
				t.Fatal("expected invalid signal error")
			}
			if signals := claimSignals(t, ctx, provider, "worker-2", 33, 10, -time.Second); len(signals) != 0 {
				t.Fatalf("dead-lettered signal should not be claimable, got %+v", signals)
			}
		})
	}
}

func TestRenewingEntryLeasePreventsStealDuringLongHandler(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var calls atomic.Int32
			started := make(chan struct{})
			release := make(chan struct{})

			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(ctx context.Context, _ Entry) (HandlerResult, error) {
						if calls.Add(1) == 1 {
							close(started)
						}
						select {
						case <-release:
							return NoNext(), nil
						case <-ctx.Done():
							return NoNext(), ctx.Err()
						}
					},
				},
			},
				WithLeaseDuration(40*time.Millisecond),
				WithLeaseRenewalInterval(10*time.Millisecond),
				WithRetryPolicy(RetryPolicy{}),
			)
			createMachine(t, ctx, rt, "m1", 34, stateIdle)
			processSignal(t, ctx, rt, provider, "signal-worker", 34, NewSignal("s1", "m1", triggerStart))

			done := make(chan error, 1)
			go func() {
				processed, err := rt.Worker(WorkerConfig{ID: "worker-a", Shards: []ShardID{34}}).Work(ctx, 1)
				if err != nil {
					done <- err
					return
				}
				if processed != 1 {
					done <- fmt.Errorf("expected worker-a to process one entry, got %d", processed)
					return
				}
				done <- nil
			}()

			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("handler did not start")
			}

			time.Sleep(90 * time.Millisecond)
			stealCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
			processed, err := rt.Worker(WorkerConfig{ID: "worker-b", Shards: []ShardID{34}}).Work(stealCtx, 1)
			cancel()
			if err != nil {
				t.Fatalf("worker-b should not steal renewed entry: %v", err)
			}
			if processed != 0 {
				t.Fatalf("worker-b should not process renewed entry, got %d", processed)
			}
			if calls.Load() != 1 {
				t.Fatalf("handler should have run once while lease renewed, got %d", calls.Load())
			}

			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("worker-a: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("worker-a did not finish")
			}
		})
	}
}

func TestRenewSignalLeasePreventsExpiredClaim(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			rt := newTestRuntime(t, provider, testDefinition{})
			createMachine(t, ctx, rt, "m1", 35, stateIdle)
			if err := rt.Signal(ctx, NewSignal("s1", "m1", triggerStart)); err != nil {
				t.Fatalf("signal: %v", err)
			}

			claimed := claimSignals(t, ctx, provider, "worker-a", 35, 1, 20*time.Millisecond)
			if len(claimed) != 1 {
				t.Fatalf("expected signal claim, got %+v", claimed)
			}
			if err := provider.RenewSignalLease(ctx, claimed[0].ID, claimed[0].Owner, claimed[0].Attempts, 100*time.Millisecond); err != nil {
				t.Fatalf("renew signal lease: %v", err)
			}
			time.Sleep(40 * time.Millisecond)
			if signals := claimSignals(t, ctx, provider, "worker-b", 35, 1, time.Second); len(signals) != 0 {
				t.Fatalf("renewed signal lease should not be claimable, got %+v", signals)
			}
		})
	}
}

func newTestRuntime(t *testing.T, provider Provider, def Definition, options ...Option) *Runtime {
	t.Helper()
	if err := provider.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate provider: %v", err)
	}
	return NewRuntime(provider, def, options...)
}

func createMachine(t *testing.T, ctx context.Context, rt *Runtime, id string, shard ShardID, state string) {
	t.Helper()
	if err := rt.CreateMachineInShard(ctx, shard, MachineInit{ID: id, State: state}); err != nil {
		t.Fatalf("create machine %s: %v", id, err)
	}
}

func processSignal(t *testing.T, ctx context.Context, rt *Runtime, provider Provider, owner string, shard ShardID, signal Signal) {
	t.Helper()
	if err := rt.Signal(ctx, signal); err != nil {
		t.Fatalf("signal %s: %v", signal.ID, err)
	}
	processClaimedSignal(t, ctx, rt, provider, owner, shard)
}

func processClaimedSignal(t *testing.T, ctx context.Context, rt *Runtime, provider Provider, owner string, shard ShardID) {
	t.Helper()
	signals := claimSignals(t, ctx, provider, owner, shard, 1, time.Second)
	if len(signals) != 1 {
		t.Fatalf("expected one signal, got %+v", signals)
	}
	worker := rt.Worker(WorkerConfig{ID: owner, Shards: []ShardID{shard}})
	if err := worker.processSignal(ctx, signals[0]); err != nil {
		t.Fatalf("process signal: %v", err)
	}
}

func claimSignals(t *testing.T, ctx context.Context, provider Provider, owner string, shard ShardID, limit int, lease time.Duration) []SignalRecord {
	t.Helper()
	signals, err := provider.ClaimSignals(ctx, owner, []ShardID{shard}, limit, lease)
	if err != nil {
		t.Fatalf("claim signals: %v", err)
	}
	return signals
}

func claimEntries(t *testing.T, ctx context.Context, provider Provider, owner string, shard ShardID, limit int, lease time.Duration) []Entry {
	t.Helper()
	entries, err := provider.ClaimEntries(ctx, owner, []ShardID{shard}, limit, lease)
	if err != nil {
		t.Fatalf("claim entries: %v", err)
	}
	return entries
}

func readMachine(t *testing.T, ctx context.Context, provider Provider, id string) *Snapshot {
	t.Helper()
	snap, err := provider.ReadMachine(ctx, id)
	if err != nil {
		t.Fatalf("read machine %s: %v", id, err)
	}
	return snap
}
