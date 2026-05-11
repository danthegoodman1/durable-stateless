package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
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

type panicDefinition struct {
	configure func(*Rules)
	terminal  func(stateless.State) bool
	handler   func(stateless.State) (EntryHandler, bool)
}

func (d panicDefinition) Configure(rules *Rules) {
	if d.configure != nil {
		d.configure(rules)
	}
}

func (d panicDefinition) IsTerminal(state stateless.State) bool {
	if d.terminal == nil {
		return false
	}
	return d.terminal(state)
}

func (d panicDefinition) EntryHandler(state stateless.State) (EntryHandler, bool) {
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

func TestInvalidTriggerDoesNotMutateState(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{})
			createMachine(t, ctx, runner, "m1", 7, stateIdle)

			if err := runner.Fire(ctx, "m1", "bogus"); err == nil {
				t.Fatal("expected invalid trigger error")
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 || snap.Terminal() {
				t.Fatalf("unexpected snapshot after failed fire: %+v", snap)
			}
			entries, err := provider.ClaimEntries(ctx, "worker", []int{7}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim entries: %v", err)
			}
			if len(entries) != 0 {
				t.Fatalf("expected no entries, got %+v", entries)
			}
		})
	}
}

func TestDurableFireCreatesPendingEntry(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			createMachine(t, ctx, runner, "m1", 1, stateIdle)

			if err := runner.Fire(ctx, "m1", triggerStart, "payload"); err != nil {
				t.Fatalf("fire start: %v", err)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateWorking || snap.Version != 1 || snap.Terminal() {
				t.Fatalf("unexpected snapshot after fire: %+v", snap)
			}
			if len(snap.Args) != 1 || snap.Args[0] != "payload" {
				t.Fatalf("unexpected snapshot args: %#v", snap.Args)
			}

			entries, err := provider.ClaimEntries(ctx, "worker", []int{1}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim entries: %v", err)
			}
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

func TestTerminalTransitionCreatesNoEntry(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			createMachine(t, ctx, runner, "m1", 3, stateIdle)

			if err := runner.Fire(ctx, "m1", triggerFinish); err != nil {
				t.Fatalf("fire finish: %v", err)
			}

			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateDone || snap.Version != 1 || !snap.Terminal() {
				t.Fatalf("unexpected terminal snapshot: %+v", snap)
			}
			entries, err := provider.ClaimEntries(ctx, "worker", []int{3}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim entries: %v", err)
			}
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
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{stateWorking: handler},
			})
			createMachine(t, ctx, runner, "m1", 4, stateIdle)
			if err := runner.Fire(ctx, "m1", triggerStart); err != nil {
				t.Fatalf("fire start: %v", err)
			}

			claimed, err := provider.ClaimEntries(ctx, "worker-a", []int{4}, 1, -time.Second)
			if err != nil {
				t.Fatalf("claim worker-a: %v", err)
			}
			if len(claimed) != 1 {
				t.Fatalf("expected one claimed entry, got %d", len(claimed))
			}
			if _, err := handler(ctx, claimed[0]); err != nil {
				t.Fatalf("simulate handler side effect: %v", err)
			}

			processed, err := runner.Recover(ctx, "worker-b", []int{4}, 10)
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

func TestCompletionAndNextTriggerAreAtomic(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return FireNext("bogus"), nil
					},
				},
			})
			createMachine(t, ctx, runner, "m1", 5, stateIdle)
			if err := runner.Fire(ctx, "m1", triggerStart); err != nil {
				t.Fatalf("fire start: %v", err)
			}

			processed, err := runner.Recover(ctx, "worker", []int{5}, 10)
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

			reclaimed, err := provider.ClaimEntries(ctx, "inspector", []int{5}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim failed entry: %v", err)
			}
			if len(reclaimed) != 1 {
				t.Fatalf("expected failed entry to be claimable, got %d", len(reclaimed))
			}
			if reclaimed[0].Key != (EntryKey{MachineID: "m1", Version: 1}) || reclaimed[0].Attempts != 2 {
				t.Fatalf("unexpected reclaimed entry: %+v", reclaimed[0])
			}
		})
	}
}

func TestClaimEntriesHonorsLimitAndLeases(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("m%d", i)
				createMachine(t, ctx, runner, id, 9, stateIdle)
				if err := runner.Fire(ctx, id, triggerStart); err != nil {
					t.Fatalf("fire start %s: %v", id, err)
				}
			}

			first, err := provider.ClaimEntries(ctx, "worker-a", []int{9}, 2, time.Minute)
			if err != nil {
				t.Fatalf("first claim: %v", err)
			}
			if len(first) != 2 {
				t.Fatalf("expected first claim limit of 2, got %d", len(first))
			}

			second, err := provider.ClaimEntries(ctx, "worker-b", []int{9}, 10, time.Minute)
			if err != nil {
				t.Fatalf("second claim: %v", err)
			}
			if len(second) != 1 {
				t.Fatalf("expected only unleased entry, got %d", len(second))
			}
		})
	}
}

func TestFireRejectsOpenCurrentEntry(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			createMachine(t, ctx, runner, "m1", 10, stateIdle)
			if err := runner.Fire(ctx, "m1", triggerStart); err != nil {
				t.Fatalf("fire start: %v", err)
			}

			err := runner.Fire(ctx, "m1", triggerFinish)
			if !errors.Is(err, ErrEntryInProgress) {
				t.Fatalf("expected open entry error, got %v", err)
			}
			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateWorking || snap.Version != 1 || snap.Terminal() {
				t.Fatalf("open entry fire should not mutate state: %+v", snap)
			}

			processed, err := runner.Recover(ctx, "worker", []int{10}, 10)
			if err != nil {
				t.Fatalf("recover entry: %v", err)
			}
			if processed != 1 {
				t.Fatalf("expected one recovered entry, got %d", processed)
			}
			if err := runner.Fire(ctx, "m1", triggerFinish); err != nil {
				t.Fatalf("fire finish after entry completion: %v", err)
			}
			snap = readMachine(t, ctx, provider, "m1")
			if snap.State != stateDone || snap.Version != 2 || !snap.Terminal() {
				t.Fatalf("expected terminal snapshot after completed entry: %+v", snap)
			}
		})
	}
}

func TestCommitTransitionUsesExpectedVersion(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{})
			createMachine(t, ctx, runner, "m1", 15, stateIdle)

			_, _, err := provider.CommitTransition(ctx, CommitTransition{
				MachineID:       "m1",
				ExpectedVersion: 1,
				Record: TransitionRecord{
					SourceState: stateIdle,
					DestState:   stateWorking,
					Trigger:     triggerStart,
				},
			})
			if !errors.Is(err, ErrVersionConflict) {
				t.Fatalf("expected version conflict, got %v", err)
			}
			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 {
				t.Fatalf("version conflict should not mutate state: %+v", snap)
			}
		})
	}
}

func TestIgnoredTriggerIsDurableNoop(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				ignored: map[string][]stateless.Trigger{
					stateIdle: {"ignore"},
				},
			})
			createMachine(t, ctx, runner, "m1", 17, stateIdle)

			if err := runner.Fire(ctx, "m1", "ignore"); err != nil {
				t.Fatalf("ignored trigger should succeed: %v", err)
			}
			snap := readMachine(t, ctx, provider, "m1")
			if snap.State != stateIdle || snap.Version != 0 {
				t.Fatalf("ignored trigger should not mutate: %+v", snap)
			}
			entries, err := provider.ClaimEntries(ctx, "worker", []int{17}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim entries: %v", err)
			}
			if len(entries) != 0 {
				t.Fatalf("ignored trigger should not create entries: %+v", entries)
			}
		})
	}
}

func TestCompleteEntryAndCommitTransitionUsesExpectedVersionAtomically(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), nil
					},
				},
			})
			createMachine(t, ctx, runner, "m1", 16, stateIdle)
			if err := runner.Fire(ctx, "m1", triggerStart); err != nil {
				t.Fatalf("fire start: %v", err)
			}
			claimed, err := provider.ClaimEntries(ctx, "worker", []int{16}, 1, -time.Second)
			if err != nil {
				t.Fatalf("claim entry: %v", err)
			}
			if len(claimed) != 1 {
				t.Fatalf("expected one claimed entry, got %+v", claimed)
			}

			_, _, err = provider.CompleteEntryAndCommitTransition(ctx, CompleteEntryAndCommitTransition{
				Complete: CompleteEntryCommand{
					Key:   claimed[0].Key,
					Owner: "worker",
				},
				Transition: CommitTransition{
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
			reclaimed, err := provider.ClaimEntries(ctx, "worker-2", []int{16}, 1, -time.Second)
			if err != nil {
				t.Fatalf("reclaim entry: %v", err)
			}
			if len(reclaimed) != 1 || reclaimed[0].Key != claimed[0].Key {
				t.Fatalf("entry completion should roll back with failed transition, got %+v", reclaimed)
			}
		})
	}
}

func TestClaimEntriesIgnoresStaleVersions(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{})
			createMachine(t, ctx, runner, "m1", 11, stateIdle)

			_, _, err := provider.CommitTransition(ctx, CommitTransition{
				MachineID:       "m1",
				ExpectedVersion: 0,
				Record: TransitionRecord{
					SourceState: stateIdle,
					DestState:   stateWorking,
					Trigger:     triggerStart,
					CreateEntry: true,
				},
			})
			if err != nil {
				t.Fatalf("seed first transition: %v", err)
			}
			claimed, err := provider.ClaimEntries(ctx, "worker", []int{11}, 1, time.Second)
			if err != nil {
				t.Fatalf("claim first entry: %v", err)
			}
			if len(claimed) != 1 {
				t.Fatalf("expected first entry claim, got %+v", claimed)
			}
			if err := provider.CompleteEntry(ctx, claimed[0].Key, "worker"); err != nil {
				t.Fatalf("complete first entry: %v", err)
			}
			_, _, err = provider.CommitTransition(ctx, CommitTransition{
				MachineID:       "m1",
				ExpectedVersion: 1,
				Record: TransitionRecord{
					SourceState: stateWorking,
					DestState:   "blocked",
					Trigger:     "advance",
					CreateEntry: true,
				},
			})
			if err != nil {
				t.Fatalf("seed second transition: %v", err)
			}

			entries, err := provider.ClaimEntries(ctx, "worker", []int{11}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim entries: %v", err)
			}
			if len(entries) != 1 || entries[0].Key != (EntryKey{MachineID: "m1", Version: 2}) {
				t.Fatalf("expected only current entry version, got %+v", entries)
			}
		})
	}
}

func TestRecoverContinuesAfterEntryFailure(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handlerErr := errors.New("handler failed")
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(_ context.Context, entry Entry) (HandlerResult, error) {
						if entry.Key.MachineID == "m1" {
							return NoNext(), handlerErr
						}
						return NoNext(), nil
					},
				},
			})
			for _, id := range []string{"m1", "m2"} {
				createMachine(t, ctx, runner, id, 12, stateIdle)
				if err := runner.Fire(ctx, id, triggerStart); err != nil {
					t.Fatalf("fire start %s: %v", id, err)
				}
			}

			processed, err := runner.Recover(ctx, "worker", []int{12}, 10)
			if !errors.Is(err, handlerErr) {
				t.Fatalf("expected joined handler error, got %v", err)
			}
			if processed != 1 {
				t.Fatalf("expected one successful entry despite failure, got %d", processed)
			}

			reclaimed, err := provider.ClaimEntries(ctx, "worker-2", []int{12}, 10, time.Second)
			if err != nil {
				t.Fatalf("reclaim failed entry: %v", err)
			}
			if len(reclaimed) != 1 || reclaimed[0].Key.MachineID != "m1" {
				t.Fatalf("expected only failed entry to remain claimable, got %+v", reclaimed)
			}
		})
	}
}

func TestStatelessPanicRollsBackDurableTransaction(t *testing.T) {
	funcConfigure := func(rules *Rules) {
		rules.SetTriggerParameters(triggerStart, reflect.TypeOf(0))
		rules.Configure(stateIdle).Permit(triggerStart, stateWorking)
	}

	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, panicDefinition{
				configure: funcConfigure,
				terminal: func(state stateless.State) bool {
					return state == stateDone
				},
			})
			createMachine(t, ctx, runner, "m1", 13, stateIdle)

			err := runner.Fire(ctx, "m1", triggerStart, "wrong")
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

func TestNilEntryHandlerRollsBackTransition(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := tc.new(t)
			runner := newTestRunner(t, provider, panicDefinition{
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
			createMachine(t, ctx, runner, "m1", 14, stateIdle)

			err := runner.Fire(ctx, "m1", triggerStart)
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

func TestSQLitePersistsMachinesAndEntriesAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "machines.db")

	provider, err := OpenSQLiteProvider(path)
	if err != nil {
		t.Fatalf("open sqlite provider: %v", err)
	}
	runner := newTestRunner(t, provider, testDefinition{
		handlers: map[string]EntryHandler{
			stateWorking: func(context.Context, Entry) (HandlerResult, error) {
				return NoNext(), nil
			},
		},
	})
	createMachine(t, ctx, runner, "m1", 2, stateIdle)
	if err := runner.Fire(ctx, "m1", triggerStart); err != nil {
		t.Fatalf("fire start: %v", err)
	}
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
	entries, err := reopened.ClaimEntries(ctx, "worker", []int{2}, 10, time.Second)
	if err != nil {
		t.Fatalf("claim reopened entry: %v", err)
	}
	if len(entries) != 1 || entries[0].Key != (EntryKey{MachineID: "m1", Version: 1}) {
		t.Fatalf("unexpected persisted entries: %+v", entries)
	}
}

func newTestRunner(t *testing.T, provider Provider, def Definition) *Runner {
	t.Helper()
	if err := provider.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate provider: %v", err)
	}
	return NewRunner(provider, def)
}

func createMachine(t *testing.T, ctx context.Context, runner *Runner, id string, shard int, state string) {
	t.Helper()
	if err := runner.CreateMachine(ctx, MachineInit{ID: id, ShardID: shard, State: state}); err != nil {
		t.Fatalf("create machine %s: %v", id, err)
	}
}

func readMachine(t *testing.T, ctx context.Context, provider Provider, id string) *Snapshot {
	t.Helper()
	snap, err := provider.ReadMachine(ctx, id)
	if err != nil {
		t.Fatalf("read machine %s: %v", id, err)
	}
	return snap
}

func TestProcessEntryFailsEntryWhenHandlerFails(t *testing.T) {
	for _, tc := range providerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handlerErr := errors.New("handler failed")
			provider := tc.new(t)
			runner := newTestRunner(t, provider, testDefinition{
				handlers: map[string]EntryHandler{
					stateWorking: func(context.Context, Entry) (HandlerResult, error) {
						return NoNext(), handlerErr
					},
				},
			})
			createMachine(t, ctx, runner, "m1", 6, stateIdle)
			if err := runner.Fire(ctx, "m1", triggerStart); err != nil {
				t.Fatalf("fire start: %v", err)
			}

			_, err := runner.Recover(ctx, "worker", []int{6}, 10)
			if !errors.Is(err, handlerErr) {
				t.Fatalf("expected handler error, got %v", err)
			}

			reclaimed, err := provider.ClaimEntries(ctx, "worker-2", []int{6}, 10, time.Second)
			if err != nil {
				t.Fatalf("claim failed entry: %v", err)
			}
			if len(reclaimed) != 1 || reclaimed[0].LastError != handlerErr.Error() {
				t.Fatalf("expected failed entry with error, got %+v", reclaimed)
			}
		})
	}
}
