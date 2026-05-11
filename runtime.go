package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/qmuntal/stateless"
)

// Option configures a Runtime.
type Option func(*Runtime)

// WithSharder configures how Runtime.CreateMachine assigns machines to shards.
func WithSharder(sharder Sharder) Option {
	return func(r *Runtime) {
		if sharder != nil {
			r.sharder = sharder
		}
	}
}

// WithLeaseDuration configures how long claimed signals and entries are leased
// to a worker before another worker may retry them.
func WithLeaseDuration(lease time.Duration) Option {
	return func(r *Runtime) {
		r.lease = lease
	}
}

// Runtime evaluates stateless rules, resolves machine shards, and coordinates
// durable signals and worker processing through a Provider.
type Runtime struct {
	provider Provider
	def      Definition
	sharder  Sharder
	lease    time.Duration
}

// NewRuntime creates a Runtime. By default all machines are placed on shard 0.
func NewRuntime(provider Provider, def Definition, options ...Option) *Runtime {
	r := &Runtime{
		provider: provider,
		def:      def,
		sharder:  MustHashSharder(1),
		lease:    DefaultLeaseDuration,
	}
	for _, option := range options {
		option(r)
	}
	return r
}

// CreateMachine creates a durable machine and chooses its shard with the
// runtime's Sharder.
func (r *Runtime) CreateMachine(ctx context.Context, init MachineInit) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.CreateMachineInShard(ctx, r.sharder.ShardForMachine(init.ID), init)
}

// CreateMachineInShard creates a durable machine on an explicit shard. It is
// mostly useful for tests or externally managed shard placement.
func (r *Runtime) CreateMachineInShard(ctx context.Context, shard ShardID, init MachineInit) error {
	if err := r.validate(); err != nil {
		return err
	}
	if err := validateShardID(shard); err != nil {
		return err
	}
	return r.provider.CreateMachine(ctx, MachineRecord{
		ID:       init.ID,
		ShardID:  shard,
		State:    init.State,
		Args:     cloneArgs(init.Args),
		Terminal: r.def.IsTerminal(init.State),
	})
}

// ReadMachine returns the provider's durable current-state projection.
func (r *Runtime) ReadMachine(ctx context.Context, id string) (*Snapshot, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r.provider.ReadMachine(ctx, id)
}

// Signal durably enqueues a trigger message for a machine. The signal is not
// applied here; a worker that owns the target machine's shard applies it later.
func (r *Runtime) Signal(ctx context.Context, signal Signal) error {
	if err := r.validate(); err != nil {
		return err
	}
	record, err := r.prepareSignal(ctx, signal)
	if err != nil {
		return err
	}
	return r.provider.EnqueueSignal(ctx, record)
}

// Worker creates a shard worker view over this runtime.
func (r *Runtime) Worker(config WorkerConfig) *Worker {
	lease := r.lease
	if lease == 0 {
		lease = DefaultLeaseDuration
	}
	return &Worker{
		runtime: r,
		id:      config.ID,
		shards:  cloneShards(config.Shards),
		lease:   lease,
	}
}

func (r *Runtime) prepareSignal(ctx context.Context, signal Signal) (SignalRecord, error) {
	if signal.ID == "" {
		return SignalRecord{}, fmt.Errorf("durablestateless: signal id is required")
	}
	if signal.MachineID == "" {
		return SignalRecord{}, fmt.Errorf("durablestateless: signal machine id is required")
	}
	trigger, err := encodeSymbol("trigger", signal.Trigger)
	if err != nil {
		return SignalRecord{}, err
	}
	if _, err := encodeArgs(signal.Args); err != nil {
		return SignalRecord{}, err
	}
	snap, err := r.provider.ReadMachine(ctx, signal.MachineID)
	if err != nil {
		return SignalRecord{}, err
	}
	return SignalRecord{
		Signal: Signal{
			ID:        signal.ID,
			MachineID: signal.MachineID,
			Trigger:   trigger,
			Args:      cloneArgs(signal.Args),
		},
		TargetShardID: snap.ShardID,
		Status:        EntryPending,
	}, nil
}

func (r *Runtime) prepareSignals(ctx context.Context, signals []Signal) ([]SignalRecord, error) {
	if len(signals) == 0 {
		return nil, nil
	}
	records := make([]SignalRecord, 0, len(signals))
	for _, signal := range signals {
		record, err := r.prepareSignal(ctx, signal)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (r *Runtime) buildCommit(ctx context.Context, snap *Snapshot, trigger stateless.Trigger, args ...any) (cmd *CommitTransition, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("durablestateless: stateless panic: %v", recovered)
		}
	}()

	if snap.Terminal() {
		return nil, ErrTerminalMachine
	}

	var transition stateless.Transition
	transitionSeen := false
	var record *TransitionRecord

	sm := stateless.NewStateMachineWithExternalStorageAndArgs(
		func(context.Context) (stateless.State, []any, error) {
			return snap.State, cloneArgs(snap.Args), nil
		},
		func(_ context.Context, next stateless.State, stateArgs ...any) error {
			if !transitionSeen {
				return fmt.Errorf("durablestateless: transition metadata missing for state mutation")
			}
			terminal := r.def.IsTerminal(next)
			handler, hasHandler := r.def.EntryHandler(next)
			if hasHandler && handler == nil {
				return fmt.Errorf("%w for state %v", ErrNilEntryHandler, next)
			}
			nextRecord := TransitionRecord{
				SourceState: transition.Source,
				DestState:   next,
				Trigger:     transition.Trigger,
				Args:        cloneArgs(stateArgs),
				Terminal:    terminal,
				CreateEntry: !terminal && hasHandler,
			}
			record = &nextRecord
			transitionSeen = false
			return nil
		},
		stateless.FiringQueued,
	)

	r.def.Configure(newRules(sm))
	sm.OnTransitioning(func(_ context.Context, tr stateless.Transition) {
		transition = tr
		transitionSeen = true
	})

	if err := sm.FireCtx(ctx, trigger, args...); err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	return &CommitTransition{
		MachineID:       snap.ID,
		ExpectedVersion: snap.Version,
		Record:          *record,
	}, nil
}

func (r *Runtime) validate() error {
	if r == nil {
		return fmt.Errorf("durablestateless: nil runtime")
	}
	if r.provider == nil {
		return fmt.Errorf("durablestateless: provider is required")
	}
	if r.def == nil {
		return fmt.Errorf("durablestateless: definition is required")
	}
	if r.sharder == nil {
		return fmt.Errorf("%w: sharder is required", ErrInvalidShard)
	}
	return nil
}

// WorkerConfig identifies a worker and the shards it currently owns.
type WorkerConfig struct {
	ID     string
	Shards []ShardID
}

// Worker claims and processes durable signals and entry work for owned shards.
type Worker struct {
	runtime *Runtime
	id      string
	shards  []ShardID
	lease   time.Duration
}

// Work processes up to limit successful units of work for the worker's shards.
// A unit is either a completed signal or a completed entry handler. Work first
// recovers unfinished entries, then applies signals, then processes entries
// created by those signals.
func (w *Worker) Work(ctx context.Context, limit int) (int, error) {
	if err := w.validate(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, nil
	}

	processed := 0
	var errs []error
	hadEntryError := false

	processEntries := func(available int) error {
		if available <= 0 {
			return nil
		}
		entries, err := w.runtime.provider.ClaimEntries(ctx, w.id, w.shards, available, w.lease)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := w.processEntry(ctx, entry); err != nil {
				hadEntryError = true
				errs = append(errs, err)
				continue
			}
			processed++
		}
		return nil
	}

	processSignals := func(available int) error {
		if available <= 0 {
			return nil
		}
		signals, err := w.runtime.provider.ClaimSignals(ctx, w.id, w.shards, available, w.lease)
		if err != nil {
			return err
		}
		for _, signal := range signals {
			if err := w.processSignal(ctx, signal); err != nil {
				errs = append(errs, err)
				continue
			}
			processed++
		}
		return nil
	}

	if err := processEntries(limit - processed); err != nil {
		return processed, err
	}
	if err := processSignals(limit - processed); err != nil {
		errs = append(errs, err)
		return processed, errors.Join(errs...)
	}
	if !hadEntryError {
		if err := processEntries(limit - processed); err != nil {
			errs = append(errs, err)
			return processed, errors.Join(errs...)
		}
	}

	return processed, errors.Join(errs...)
}

func (w *Worker) processSignal(ctx context.Context, signal SignalRecord) error {
	if err := w.validate(); err != nil {
		return err
	}
	if signal.Owner == "" {
		return fmt.Errorf("durablestateless: claimed signal %s has no owner", signal.ID)
	}

	snap, err := w.runtime.provider.ReadMachine(ctx, signal.MachineID)
	if err != nil {
		_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.Attempts, err)
		return err
	}
	if !w.owns(snap.ShardID) {
		err := fmt.Errorf("%w: machine %s is on shard %d", ErrWrongShard, snap.ID, snap.ShardID)
		_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.Attempts, err)
		return err
	}

	commit := AtomicCommit{
		CompleteSignal: &CompleteSignalCommand{
			ID:      signal.ID,
			Owner:   signal.Owner,
			Attempt: signal.Attempts,
		},
	}
	cmd, err := w.runtime.buildCommit(ctx, snap, signal.Trigger, signal.Args...)
	if err != nil {
		_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.Attempts, err)
		return err
	}
	if cmd != nil {
		commit.Transition = cmd
	}

	_, err = w.runtime.provider.Commit(ctx, commit)
	if err != nil {
		if shouldFailClaimedWork(err) {
			_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.Attempts, err)
		}
		return err
	}
	return nil
}

func (w *Worker) processEntry(ctx context.Context, entry Entry) error {
	if err := w.validate(); err != nil {
		return err
	}
	if entry.Owner == "" {
		return fmt.Errorf("durablestateless: claimed entry %s has no owner", entry.Key)
	}
	if !w.owns(entry.ShardID) {
		err := fmt.Errorf("%w: machine %s is on shard %d", ErrWrongShard, entry.Key.MachineID, entry.ShardID)
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
		return err
	}

	handler, ok := w.runtime.def.EntryHandler(entry.DestState)
	if !ok {
		_, err := w.runtime.provider.Commit(ctx, AtomicCommit{
			CompleteEntry: &CompleteEntryCommand{
				Key:     entry.Key,
				Owner:   entry.Owner,
				Attempt: entry.Attempts,
			},
		})
		return err
	}
	if handler == nil {
		err := fmt.Errorf("%w for state %v", ErrNilEntryHandler, entry.DestState)
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
		return err
	}

	result, err := handler(ctx, entry)
	if err != nil {
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
		return err
	}

	records, err := w.runtime.prepareSignals(ctx, result.Signals)
	if err != nil {
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
		return err
	}

	commit := AtomicCommit{
		CompleteEntry: &CompleteEntryCommand{
			Key:     entry.Key,
			Owner:   entry.Owner,
			Attempt: entry.Attempts,
		},
		Signals: records,
	}
	if result.Next != nil {
		snap, err := w.runtime.provider.ReadMachine(ctx, entry.Key.MachineID)
		if err != nil {
			_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
			return err
		}
		cmd, err := w.runtime.buildCommit(ctx, snap, result.Next.Trigger, result.Next.Args...)
		if err != nil {
			_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
			return err
		}
		commit.Transition = cmd
	}

	_, err = w.runtime.provider.Commit(ctx, commit)
	if err != nil {
		if shouldFailClaimedWork(err) {
			_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.Attempts, err)
		}
		return err
	}
	return nil
}

func (w *Worker) owns(shard ShardID) bool {
	for _, owned := range w.shards {
		if owned == shard {
			return true
		}
	}
	return false
}

func (w *Worker) validate() error {
	if w == nil || w.runtime == nil {
		return fmt.Errorf("durablestateless: nil worker")
	}
	if err := w.runtime.validate(); err != nil {
		return err
	}
	if w.id == "" {
		return fmt.Errorf("durablestateless: worker id is required")
	}
	if len(w.shards) == 0 {
		return fmt.Errorf("%w: worker must own at least one shard", ErrInvalidShard)
	}
	if err := validateShardIDs(w.shards); err != nil {
		return err
	}
	return nil
}

func shouldFailClaimedWork(err error) bool {
	return !errors.Is(err, ErrEntryInProgress) && !errors.Is(err, ErrVersionConflict)
}

func cloneShards(shards []ShardID) []ShardID {
	if len(shards) == 0 {
		return nil
	}
	out := make([]ShardID, len(shards))
	copy(out, shards)
	return out
}
