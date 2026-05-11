package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/qmuntal/stateless"
)

// DefaultLeaseDuration is used by workers when no explicit shard lease
// duration is configured. A worker must renew its shard lease before it expires
// to keep completing claimed work.
const DefaultLeaseDuration = 30 * time.Second

var (
	ErrMachineNotFound   = errors.New("durablestateless: machine not found")
	ErrEntryNotFound     = errors.New("durablestateless: entry not found")
	ErrEntryNotOwned     = errors.New("durablestateless: entry is not processing for owner")
	ErrEntryInProgress   = errors.New("durablestateless: current entry is not complete")
	ErrInvalidLease      = errors.New("durablestateless: invalid lease duration")
	ErrInvalidShard      = errors.New("durablestateless: invalid shard configuration")
	ErrInvalidTransition = errors.New("durablestateless: invalid transition record")
	ErrNilEntryHandler   = errors.New("durablestateless: entry handler is nil")
	ErrShardLeaseLost    = errors.New("durablestateless: shard lease is not owned")
	ErrSignalConflict    = errors.New("durablestateless: signal id conflicts with existing signal")
	ErrSignalNotFound    = errors.New("durablestateless: signal not found")
	ErrSignalNotOwned    = errors.New("durablestateless: signal is not processing for owner")
	ErrWorkDeadLettered  = errors.New("durablestateless: work is dead-lettered")
	ErrVersionConflict   = errors.New("durablestateless: machine version conflict")
	ErrTerminalMachine   = errors.New("durablestateless: machine is terminal")
	ErrWrongShard        = errors.New("durablestateless: worker does not own machine shard")
)

// ShardID identifies a partition of machines and work. Workers only claim
// signals and entry work for shards they currently own.
type ShardID int

// ShardLease is a worker's temporary ownership token for a shard. Epoch changes
// every time ownership is reacquired after expiry, which fences stale workers.
type ShardLease struct {
	ShardID    ShardID
	Owner      string
	Epoch      int64
	LeaseUntil time.Time
	UpdatedAt  time.Time
}

// EntryStatus is the durable lifecycle state for entry work and signals.
type EntryStatus string

const (
	// EntryPending means work has been committed and has not been claimed.
	EntryPending EntryStatus = "pending"
	// EntryProcessing means work is claimed under a worker's shard lease.
	EntryProcessing EntryStatus = "processing"
	// EntryDone means work completed successfully.
	EntryDone EntryStatus = "done"
	// EntryFailed means a prior attempt failed and the work can be reclaimed
	// after RetryAt, if any.
	EntryFailed EntryStatus = "failed"
	// EntryDeadLettered means work exhausted its retry policy and is no longer
	// claimable.
	EntryDeadLettered EntryStatus = "dead_lettered"
)

// DefaultRetryPolicy returns the retry policy used by NewRuntime unless
// overridden. The first failure waits one second, then backs off exponentially
// up to one minute, and the tenth failed attempt dead-letters the work.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    10,
		InitialBackoff: time.Second,
		MaxBackoff:     time.Minute,
		Multiplier:     2,
	}
}

// RetryPolicy controls how failed entries and signals are retried. MaxAttempts
// is compared with the current claimed attempt; a value <= 0 means unlimited
// attempts. InitialBackoff <= 0 makes retries immediately claimable.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// Failure is the provider-facing result of a failed work attempt.
type Failure struct {
	Cause      error
	RetryAt    *time.Time
	DeadLetter bool
}

// EntryKey is the idempotency key for a state-entry handler. A machine creates
// at most one entry for each committed version.
type EntryKey struct {
	MachineID string
	Version   int64
}

// String returns the stable machine/version representation of the entry key.
func (k EntryKey) String() string {
	return fmt.Sprintf("%s/%d", k.MachineID, k.Version)
}

// MachineInit is the public input for creating a machine. Runtime.CreateMachine
// chooses the shard with the configured Sharder.
type MachineInit struct {
	ID    string
	State stateless.State
	Args  []any
}

// MachineRecord is the provider-facing machine creation record. Runtime fills
// ShardID and Terminal before calling Provider.CreateMachine.
type MachineRecord struct {
	ID       string
	ShardID  ShardID
	State    stateless.State
	Args     []any
	Terminal bool
}

// Snapshot is the durable current-state projection for a machine.
type Snapshot struct {
	ID         string
	ShardID    ShardID
	State      stateless.State
	Version    int64
	Args       []any
	TerminalAt *time.Time
	UpdatedAt  time.Time
}

// Terminal reports whether the machine is in a terminal state.
func (s Snapshot) Terminal() bool {
	return s.TerminalAt != nil
}

// Entry is durable state-entry work produced by a committed transition.
// Handlers should use Key as the idempotency key for side effects.
type Entry struct {
	Key         EntryKey
	ShardID     ShardID
	SourceState stateless.State
	DestState   stateless.State
	Trigger     stateless.Trigger
	Args        []any
	Status      EntryStatus
	Owner       string
	OwnerEpoch  int64
	LeaseUntil  *time.Time
	RetryAt     *time.Time
	Attempts    int
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	LastError   string
}

// TransitionRecord is the provider-facing result of evaluating a legal
// stateless transition.
type TransitionRecord struct {
	SourceState stateless.State
	DestState   stateless.State
	Trigger     stateless.Trigger
	Args        []any
	Terminal    bool
	CreateEntry bool
}

// CommitTransition is an atomic compare-and-commit request for a machine
// transition. ExpectedVersion protects against stale snapshots.
type CommitTransition struct {
	MachineID       string
	ExpectedVersion int64
	Record          TransitionRecord
}

// CompleteEntryCommand marks a claimed entry complete as part of an
// AtomicCommit. Owner, OwnerEpoch, and Attempt together form the claim token.
type CompleteEntryCommand struct {
	Key        EntryKey
	Owner      string
	OwnerEpoch int64
	Attempt    int
}

// Signal is a durable trigger message for a target machine. ID is the
// idempotency key; reusing the same ID with different content is a conflict.
type Signal struct {
	ID        string
	MachineID string
	Trigger   stateless.Trigger
	Args      []any
}

// NewSignal builds a Signal and clones the provided trigger args.
func NewSignal(id string, machineID string, trigger stateless.Trigger, args ...any) Signal {
	return Signal{
		ID:        id,
		MachineID: machineID,
		Trigger:   trigger,
		Args:      cloneArgs(args),
	}
}

// SignalRecord is the provider-facing form of a Signal after the runtime has
// resolved the target machine's shard.
type SignalRecord struct {
	Signal
	TargetShardID ShardID
	Status        EntryStatus
	Owner         string
	OwnerEpoch    int64
	LeaseUntil    *time.Time
	RetryAt       *time.Time
	Attempts      int
	CreatedAt     time.Time
	StartedAt     *time.Time
	CompletedAt   *time.Time
	LastError     string
}

// CompleteSignalCommand marks a claimed signal complete as part of an
// AtomicCommit. Owner, OwnerEpoch, and Attempt together form the claim token.
type CompleteSignalCommand struct {
	ID         string
	Owner      string
	OwnerEpoch int64
	Attempt    int
}

// AtomicCommit is the provider's durability boundary. Providers must apply all
// requested changes together or none of them.
type AtomicCommit struct {
	CompleteEntry  *CompleteEntryCommand
	CompleteSignal *CompleteSignalCommand
	Transition     *CommitTransition
	Signals        []SignalRecord
}

// CommitResult contains records produced by a successful AtomicCommit.
type CommitResult struct {
	Snapshot *Snapshot
	Entry    *Entry
	Signals  []SignalRecord
}

// Provider is the storage contract for durable machines, signals, entry work,
// leases, and atomic commits.
type Provider interface {
	// Migrate prepares provider storage.
	Migrate(ctx context.Context) error
	// CreateMachine inserts a new durable machine.
	CreateMachine(ctx context.Context, record MachineRecord) error
	// ReadMachine returns the current durable machine snapshot.
	ReadMachine(ctx context.Context, id string) (*Snapshot, error)
	// EnqueueSignal durably inserts a signal, deduplicated by signal ID.
	EnqueueSignal(ctx context.Context, signal SignalRecord) error
	// AcquireShardLeases acquires ownership of the requested shards. Reacquiring
	// a shard for the same owner should advance the epoch to fence older claims.
	AcquireShardLeases(ctx context.Context, owner string, shards []ShardID, lease time.Duration) ([]ShardLease, error)
	// RenewShardLeases extends the current shard ownership tokens. If any token
	// is stale or expired, providers should return ErrShardLeaseLost.
	RenewShardLeases(ctx context.Context, owner string, leases []ShardLease, lease time.Duration) ([]ShardLease, error)
	// ClaimSignals claims work for currently owned shard leases.
	ClaimSignals(ctx context.Context, leases []ShardLease, limit int) ([]SignalRecord, error)
	// ClaimEntries claims entry work for currently owned shard leases.
	ClaimEntries(ctx context.Context, leases []ShardLease, limit int) ([]Entry, error)
	// Commit applies all requested completion, transition, and signal changes
	// atomically.
	Commit(ctx context.Context, cmd AtomicCommit) (*CommitResult, error)
	// FailEntry records a failed entry attempt using the supplied retry decision.
	FailEntry(ctx context.Context, key EntryKey, owner string, ownerEpoch int64, attempt int, failure Failure) error
	// FailSignal records a failed signal attempt using the supplied retry decision.
	FailSignal(ctx context.Context, id string, owner string, ownerEpoch int64, attempt int, failure Failure) error
}

// Definition describes a durable state machine. Configure should define
// transition rules only; side effects belong in EntryHandler.
type Definition interface {
	Configure(rules *Rules)
	IsTerminal(state stateless.State) bool
	EntryHandler(state stateless.State) (EntryHandler, bool)
}

// EntryHandler runs after a transition has committed and created entry work. It
// must be idempotent for entry.Key.
type EntryHandler func(context.Context, Entry) (HandlerResult, error)

// HandlerResult describes durable work to commit after a handler succeeds.
//
// Next is a same-machine continuation: completing the entry and applying this
// trigger happen in one provider commit. Use it when the handler decides the
// next state of the machine that produced the entry.
//
// Signals are durable messages to machines, including machines on other
// shards. Completing the entry and enqueueing these signals happen in one
// provider commit, but the signals are applied later by workers that own the
// target shards.
type HandlerResult struct {
	Next    *NextTrigger
	Signals []Signal
}

// NextTrigger is a trigger to fire on the same machine that produced the entry.
type NextTrigger struct {
	Trigger stateless.Trigger
	Args    []any
}

// NoNext returns an empty HandlerResult.
func NoNext() HandlerResult {
	return HandlerResult{}
}

// Next builds a same-machine continuation trigger for HandlerResult.Next.
func Next(trigger stateless.Trigger, args ...any) *NextTrigger {
	return &NextTrigger{
		Trigger: trigger,
		Args:    cloneArgs(args),
	}
}

// FireNext returns a HandlerResult that applies trigger to the same machine
// atomically with entry completion.
func FireNext(trigger stateless.Trigger, args ...any) HandlerResult {
	return HandlerResult{
		Next: Next(trigger, args...),
	}
}

// EmitSignals returns a HandlerResult that atomically completes the entry and
// enqueues durable signals.
func EmitSignals(signals ...Signal) HandlerResult {
	return HandlerResult{
		Signals: cloneSignals(signals),
	}
}
