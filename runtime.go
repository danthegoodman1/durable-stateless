package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// WithLeaseDuration configures how long a worker owns its shard leases before
// another worker may acquire those shards.
func WithLeaseDuration(lease time.Duration) Option {
	return func(r *Runtime) {
		r.lease = lease
	}
}

// WithLeaseRenewalInterval configures how often workers renew shard leases. A
// zero interval derives from the lease duration; a negative interval disables
// automatic renewal.
func WithLeaseRenewalInterval(interval time.Duration) Option {
	return func(r *Runtime) {
		r.renewInterval = interval
	}
}

// WithRetryPolicy configures failure backoff and dead-lettering for entries and
// signals processed by this runtime.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(r *Runtime) {
		r.retryPolicy = policy
	}
}

// Runtime evaluates stateless rules, resolves machine shards, and coordinates
// durable signals and worker processing through a Provider.
type Runtime struct {
	provider      Provider
	def           Definition
	sharder       Sharder
	lease         time.Duration
	renewInterval time.Duration
	retryPolicy   RetryPolicy
	shardMu       sync.RWMutex
	machineShards map[string]ShardID
}

// NewRuntime creates a Runtime. By default all machines are placed on shard 0.
func NewRuntime(provider Provider, def Definition, options ...Option) *Runtime {
	r := &Runtime{
		provider:      provider,
		def:           def,
		sharder:       MustHashSharder(1),
		lease:         DefaultLeaseDuration,
		retryPolicy:   DefaultRetryPolicy(),
		machineShards: make(map[string]ShardID),
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
	err := r.provider.CreateMachine(ctx, MachineRecord{
		ID:       init.ID,
		ShardID:  shard,
		State:    init.State,
		Args:     cloneArgs(init.Args),
		Terminal: r.def.IsTerminal(init.State),
	})
	if err != nil {
		return err
	}
	r.rememberMachineShard(init.ID, shard)
	return nil
}

// ReadMachine returns the provider's durable current-state projection.
func (r *Runtime) ReadMachine(ctx context.Context, id string) (*Snapshot, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	snap, err := r.provider.ReadMachine(ctx, id)
	if err != nil {
		return nil, err
	}
	r.rememberMachineShard(snap.ID, snap.ShardID)
	return snap, nil
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
	if lease <= 0 {
		lease = DefaultLeaseDuration
	}
	renewInterval := r.renewInterval
	if renewInterval == 0 && lease > 0 {
		renewInterval = lease / 3
		if renewInterval <= 0 {
			renewInterval = lease
		}
	}
	return &Worker{
		runtime:       r,
		id:            config.ID,
		shards:        cloneShards(config.Shards),
		lease:         lease,
		renewInterval: renewInterval,
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
	shard, ok := r.cachedMachineShard(signal.MachineID)
	if !ok {
		snap, err := r.provider.ReadMachine(ctx, signal.MachineID)
		if err != nil {
			return SignalRecord{}, err
		}
		shard = snap.ShardID
		r.rememberMachineShard(signal.MachineID, shard)
	}
	return SignalRecord{
		Signal: Signal{
			ID:        signal.ID,
			MachineID: signal.MachineID,
			Trigger:   trigger,
			Args:      cloneArgs(signal.Args),
		},
		TargetShardID: shard,
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

func (r *Runtime) cachedMachineShard(id string) (ShardID, bool) {
	r.shardMu.RLock()
	defer r.shardMu.RUnlock()
	shard, ok := r.machineShards[id]
	return shard, ok
}

func (r *Runtime) rememberMachineShard(id string, shard ShardID) {
	if id == "" {
		return
	}
	r.shardMu.Lock()
	defer r.shardMu.Unlock()
	if r.machineShards == nil {
		r.machineShards = make(map[string]ShardID)
	}
	r.machineShards[id] = shard
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

// Worker claims shard leases and processes durable signals and entry work for
// those owned shards.
type Worker struct {
	runtime       *Runtime
	id            string
	shards        []ShardID
	lease         time.Duration
	renewInterval time.Duration
	leases        []ShardLease
}

// Work processes up to limit successful units of work for the worker's shards.
// A unit is either a completed signal or a completed entry handler. Work first
// acquires shard leases, renews them while it runs, recovers unfinished entries,
// applies signals, then processes entries created by those signals. Do not call
// Work concurrently on the same Worker.
func (w *Worker) Work(ctx context.Context, limit int) (int, error) {
	if err := w.validate(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, nil
	}

	leases, err := w.leasesForWork(ctx)
	if err != nil {
		return 0, err
	}
	if len(leases) == 0 {
		return 0, nil
	}
	renewer := newWorkerLeaseRenewer(w, leases)

	processed := 0
	var errs []error
	hadEntryError := false
	finish := func(err error) (int, error) {
		return processed, err
	}

	processEntries := func(available int) error {
		if available <= 0 {
			return nil
		}
		if err := renewer.renewIfNeeded(ctx); err != nil {
			return err
		}
		entries, err := w.runtime.provider.ClaimEntries(ctx, renewer.leases, available)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			err := renewer.runWithBackground(ctx, func(workCtx context.Context) error {
				return w.processEntry(workCtx, entry)
			})
			if err != nil {
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
		if err := renewer.renewIfNeeded(ctx); err != nil {
			return err
		}
		signals, err := w.runtime.provider.ClaimSignals(ctx, renewer.leases, available)
		if err != nil {
			return err
		}
		for _, signal := range signals {
			if err := renewer.renewIfNeeded(ctx); err != nil {
				errs = append(errs, err)
				continue
			}
			if err := w.processSignal(ctx, signal); err != nil {
				errs = append(errs, err)
				continue
			}
			processed++
		}
		return nil
	}

	if err := processEntries(limit - processed); err != nil {
		return finish(err)
	}
	if err := processSignals(limit - processed); err != nil {
		errs = append(errs, err)
		return finish(errors.Join(errs...))
	}
	if !hadEntryError {
		if err := processEntries(limit - processed); err != nil {
			errs = append(errs, err)
			return finish(errors.Join(errs...))
		}
	}

	return finish(errors.Join(errs...))
}

func (w *Worker) leasesForWork(ctx context.Context) ([]ShardLease, error) {
	now := nowUTC()
	if cachedLeasesUsable(w.leases, w.shards, now, w.renewInterval) {
		return w.leases, nil
	}

	if len(w.leases) > 0 {
		renewed, err := w.runtime.provider.RenewShardLeases(ctx, w.id, w.leases, w.lease)
		if err == nil && leasesCoverShards(renewed, w.shards) {
			w.leases = renewed
			return w.leases, nil
		}
		w.leases = nil
	}

	leases, err := w.runtime.provider.AcquireShardLeases(ctx, w.id, w.shards, w.lease)
	if err != nil {
		return nil, err
	}
	w.leases = leases
	return w.leases, nil
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
		_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.OwnerEpoch, signal.Attempts, w.runtime.failure(signal.Attempts, err))
		return err
	}
	if !w.owns(snap.ShardID) {
		err := fmt.Errorf("%w: machine %s is on shard %d", ErrWrongShard, snap.ID, snap.ShardID)
		_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.OwnerEpoch, signal.Attempts, w.runtime.failure(signal.Attempts, err))
		return err
	}

	commit := AtomicCommit{
		CompleteSignal: &CompleteSignalCommand{
			ID:         signal.ID,
			Owner:      signal.Owner,
			OwnerEpoch: signal.OwnerEpoch,
			Attempt:    signal.Attempts,
		},
	}
	if snap.Terminal() {
		_, err = w.runtime.provider.Commit(ctx, commit)
		return err
	}

	cmd, err := w.runtime.buildCommit(ctx, snap, signal.Trigger, signal.Args...)
	if err != nil {
		_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.OwnerEpoch, signal.Attempts, w.runtime.failure(signal.Attempts, err))
		return err
	}
	if cmd != nil {
		commit.Transition = cmd
	}

	_, err = w.runtime.provider.Commit(ctx, commit)
	if err != nil {
		if shouldFailClaimedWork(err) {
			_ = w.runtime.provider.FailSignal(ctx, signal.ID, signal.Owner, signal.OwnerEpoch, signal.Attempts, w.runtime.failure(signal.Attempts, err))
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
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
		return err
	}

	handler, ok := w.runtime.def.EntryHandler(entry.DestState)
	if !ok {
		_, err := w.runtime.provider.Commit(ctx, AtomicCommit{
			CompleteEntry: &CompleteEntryCommand{
				Key:        entry.Key,
				Owner:      entry.Owner,
				OwnerEpoch: entry.OwnerEpoch,
				Attempt:    entry.Attempts,
			},
		})
		return err
	}
	if handler == nil {
		err := fmt.Errorf("%w for state %v", ErrNilEntryHandler, entry.DestState)
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
		return err
	}

	result, err := handler(ctx, entry)
	if err != nil {
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
		return err
	}

	records, err := w.runtime.prepareSignals(ctx, result.Signals)
	if err != nil {
		_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
		return err
	}

	commit := AtomicCommit{
		CompleteEntry: &CompleteEntryCommand{
			Key:        entry.Key,
			Owner:      entry.Owner,
			OwnerEpoch: entry.OwnerEpoch,
			Attempt:    entry.Attempts,
		},
		Signals: records,
	}
	if result.Next != nil {
		snap, err := w.runtime.provider.ReadMachine(ctx, entry.Key.MachineID)
		if err != nil {
			_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
			return err
		}
		cmd, err := w.runtime.buildCommit(ctx, snap, result.Next.Trigger, result.Next.Args...)
		if err != nil {
			_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
			return err
		}
		commit.Transition = cmd
	}

	_, err = w.runtime.provider.Commit(ctx, commit)
	if err != nil {
		if shouldFailClaimedWork(err) {
			_ = w.runtime.provider.FailEntry(ctx, entry.Key, entry.Owner, entry.OwnerEpoch, entry.Attempts, w.runtime.failure(entry.Attempts, err))
		}
		return err
	}
	return nil
}

type workerLeaseRenewer struct {
	worker      *Worker
	leases      []ShardLease
	nextRenewAt time.Time
}

func newWorkerLeaseRenewer(worker *Worker, leases []ShardLease) *workerLeaseRenewer {
	renewer := &workerLeaseRenewer{
		worker: worker,
		leases: leases,
	}
	renewer.scheduleNextRenewal(nowUTC())
	return renewer
}

func (r *workerLeaseRenewer) renewIfNeeded(ctx context.Context) error {
	if r.worker.lease <= 0 || r.worker.renewInterval <= 0 || r.nextRenewAt.IsZero() {
		return nil
	}
	if nowUTC().Before(r.nextRenewAt) {
		return nil
	}
	return r.renew(ctx)
}

func (r *workerLeaseRenewer) renew(ctx context.Context) error {
	now := nowUTC()
	renewed, err := r.worker.runtime.provider.RenewShardLeases(ctx, r.worker.id, r.leases, r.worker.lease)
	if err != nil {
		return err
	}
	r.leases = renewed
	r.worker.leases = renewed
	r.scheduleNextRenewal(now)
	return nil
}

func (r *workerLeaseRenewer) scheduleNextRenewal(now time.Time) {
	if r.worker.lease <= 0 || r.worker.renewInterval <= 0 {
		r.nextRenewAt = time.Time{}
		return
	}
	r.nextRenewAt = now.Add(r.worker.renewInterval)
}

func (r *workerLeaseRenewer) runWithBackground(ctx context.Context, fn func(context.Context) error) error {
	if r.worker.lease <= 0 || r.worker.renewInterval <= 0 {
		return fn(ctx)
	}

	workCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	errCh := make(chan error, 1)
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(r.worker.renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := r.renew(ctx); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	err := fn(workCtx)
	close(done)
	cancel()
	<-stopped
	select {
	case renewErr := <-errCh:
		return errors.Join(err, renewErr)
	default:
		return err
	}
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
	return !errors.Is(err, ErrEntryInProgress) &&
		!errors.Is(err, ErrVersionConflict) &&
		!errors.Is(err, ErrShardLeaseLost) &&
		!errors.Is(err, ErrEntryNotOwned) &&
		!errors.Is(err, ErrSignalNotOwned)
}

func (r *Runtime) failure(attempt int, cause error) Failure {
	policy := r.retryPolicy
	failure := Failure{Cause: cause}
	if policy.MaxAttempts > 0 && attempt >= policy.MaxAttempts {
		failure.DeadLetter = true
		return failure
	}
	backoff := retryBackoff(policy, attempt)
	if backoff > 0 {
		retryAt := nowUTC().Add(backoff)
		failure.RetryAt = &retryAt
	}
	return failure
}

func retryBackoff(policy RetryPolicy, attempt int) time.Duration {
	if policy.InitialBackoff <= 0 {
		return 0
	}
	backoff := policy.InitialBackoff
	multiplier := policy.Multiplier
	if multiplier < 1 {
		multiplier = 1
	}
	for i := 1; i < attempt; i++ {
		next := time.Duration(float64(backoff) * multiplier)
		if next <= backoff {
			break
		}
		backoff = next
		if policy.MaxBackoff > 0 && backoff >= policy.MaxBackoff {
			return policy.MaxBackoff
		}
	}
	if policy.MaxBackoff > 0 && backoff > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return backoff
}

func cloneShards(shards []ShardID) []ShardID {
	if len(shards) == 0 {
		return nil
	}
	out := make([]ShardID, len(shards))
	copy(out, shards)
	return out
}

func cachedLeasesUsable(leases []ShardLease, shards []ShardID, now time.Time, refreshWindow time.Duration) bool {
	if !leasesCoverShards(leases, shards) {
		return false
	}
	refreshAt := now
	if refreshWindow > 0 {
		refreshAt = now.Add(refreshWindow)
	}
	for _, lease := range leases {
		if !lease.LeaseUntil.After(refreshAt) {
			return false
		}
	}
	return true
}

func leasesCoverShards(leases []ShardLease, shards []ShardID) bool {
	if len(shards) == 0 || len(leases) < len(shards) {
		return false
	}
	if len(shards) == 1 {
		for _, lease := range leases {
			if lease.ShardID == shards[0] {
				return true
			}
		}
		return false
	}
	owned := make(map[ShardID]struct{}, len(leases))
	for _, lease := range leases {
		owned[lease.ShardID] = struct{}{}
	}
	for _, shard := range shards {
		if _, ok := owned[shard]; !ok {
			return false
		}
	}
	return true
}
