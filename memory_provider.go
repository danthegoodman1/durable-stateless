package durablestateless

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryProvider is an in-memory Provider implementation intended for tests and
// local demos. It is not durable across process restarts.
type MemoryProvider struct {
	mu               sync.Mutex
	machines         map[string]*Snapshot
	entries          map[EntryKey]*Entry
	entryKeysByShard map[ShardID]map[EntryKey]struct{}
	signals          map[string]*SignalRecord
	signalIDsByShard map[ShardID]map[string]struct{}
	shardLeases      map[ShardID]*ShardLease
}

// NewMemoryProvider creates an empty in-memory Provider.
func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{
		machines:         make(map[string]*Snapshot),
		entries:          make(map[EntryKey]*Entry),
		entryKeysByShard: make(map[ShardID]map[EntryKey]struct{}),
		signals:          make(map[string]*SignalRecord),
		signalIDsByShard: make(map[ShardID]map[string]struct{}),
		shardLeases:      make(map[ShardID]*ShardLease),
	}
}

func (p *MemoryProvider) Migrate(context.Context) error {
	return nil
}

func (p *MemoryProvider) CreateMachine(_ context.Context, record MachineRecord) error {
	if record.ID == "" {
		return fmt.Errorf("durablestateless: machine id is required")
	}
	if err := validateShardID(record.ShardID); err != nil {
		return err
	}
	state, err := encodeSymbol("state", record.State)
	if err != nil {
		return err
	}
	if _, err := encodeArgs(record.Args); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.machines[record.ID]; exists {
		return fmt.Errorf("durablestateless: machine %q already exists", record.ID)
	}

	now := nowUTC()
	var terminalAt *time.Time
	if record.Terminal {
		terminalAt = &now
	}
	p.machines[record.ID] = &Snapshot{
		ID:         record.ID,
		ShardID:    record.ShardID,
		State:      state,
		Version:    0,
		Args:       cloneArgs(record.Args),
		TerminalAt: cloneTime(terminalAt),
		UpdatedAt:  now,
	}
	return nil
}

func (p *MemoryProvider) ReadMachine(_ context.Context, id string) (*Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	snap, ok := p.machines[id]
	if !ok {
		return nil, ErrMachineNotFound
	}
	return cloneSnapshot(snap), nil
}

func (p *MemoryProvider) EnqueueSignal(_ context.Context, signal SignalRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return enqueueSignal(p.signals, p.signalIDsByShard, signal)
}

func (p *MemoryProvider) AcquireShardLeases(_ context.Context, owner string, shards []ShardID, lease time.Duration) ([]ShardLease, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if err := validateLeaseDuration(lease); err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, nil
	}
	if err := validateShardIDs(shards); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := nowUTC()
	leaseUntil := now.Add(lease)
	leases := make([]ShardLease, 0, len(shards))
	for _, shard := range shards {
		current := p.shardLeases[shard]
		if current != nil && current.LeaseUntil.After(now) && current.Owner != owner {
			continue
		}
		epoch := int64(1)
		if current != nil {
			epoch = current.Epoch + 1
		}
		next := &ShardLease{
			ShardID:    shard,
			Owner:      owner,
			Epoch:      epoch,
			LeaseUntil: leaseUntil,
			UpdatedAt:  now,
		}
		p.shardLeases[shard] = next
		leases = append(leases, *next)
	}
	return leases, nil
}

func (p *MemoryProvider) RenewShardLeases(_ context.Context, owner string, leases []ShardLease, lease time.Duration) ([]ShardLease, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if err := validateLeaseDuration(lease); err != nil {
		return nil, err
	}
	if len(leases) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := nowUTC()
	for _, leaseToken := range leases {
		current := p.shardLeases[leaseToken.ShardID]
		if current == nil || current.Owner != owner || current.Epoch != leaseToken.Epoch || !current.LeaseUntil.After(now) {
			return nil, ErrShardLeaseLost
		}
	}

	leaseUntil := now.Add(lease)
	renewed := make([]ShardLease, 0, len(leases))
	for _, leaseToken := range leases {
		current := p.shardLeases[leaseToken.ShardID]
		current.LeaseUntil = leaseUntil
		current.UpdatedAt = now
		renewed = append(renewed, *current)
	}
	return renewed, nil
}

func (p *MemoryProvider) ClaimSignals(_ context.Context, leases []ShardLease, limit int) ([]SignalRecord, error) {
	if limit <= 0 || len(leases) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := nowUTC()
	leaseSet := p.currentLeaseSet(leases, now)
	if len(leaseSet) == 0 {
		return nil, nil
	}

	var ids []string
	for shard, leaseToken := range leaseSet {
		for id := range p.signalIDsByShard[shard] {
			signal := p.signals[id]
			if signal == nil {
				continue
			}
			if claimableByShardLease(signal.Status, signal.Owner, signal.OwnerEpoch, signal.RetryAt, leaseToken, now) {
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		left := p.signals[ids[i]]
		right := p.signals[ids[j]]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.ID < right.ID
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
	if len(ids) > limit {
		ids = ids[:limit]
	}

	claimed := make([]SignalRecord, 0, len(ids))
	for _, id := range ids {
		signal := p.signals[id]
		leaseToken := leaseSet[signal.TargetShardID]
		signal.Status = EntryProcessing
		signal.Owner = leaseToken.Owner
		signal.OwnerEpoch = leaseToken.Epoch
		signal.LeaseUntil = nil
		signal.RetryAt = nil
		signal.Attempts++
		signal.StartedAt = &now
		claimed = append(claimed, *cloneSignalRecord(signal))
	}
	return claimed, nil
}

func (p *MemoryProvider) ClaimEntries(_ context.Context, leases []ShardLease, limit int) ([]Entry, error) {
	if limit <= 0 || len(leases) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := nowUTC()
	leaseSet := p.currentLeaseSet(leases, now)
	if len(leaseSet) == 0 {
		return nil, nil
	}

	var keys []EntryKey
	for shard, leaseToken := range leaseSet {
		for key := range p.entryKeysByShard[shard] {
			entry := p.entries[key]
			if entry == nil {
				continue
			}
			machine := p.machines[key.MachineID]
			if machine == nil || machine.Terminal() {
				continue
			}
			if key.Version != machine.Version {
				continue
			}
			if claimableByShardLease(entry.Status, entry.Owner, entry.OwnerEpoch, entry.RetryAt, leaseToken, now) {
				keys = append(keys, key)
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		left := p.entries[keys[i]]
		right := p.entries[keys[j]]
		if left.CreatedAt.Equal(right.CreatedAt) {
			if left.Key.MachineID == right.Key.MachineID {
				return left.Key.Version < right.Key.Version
			}
			return left.Key.MachineID < right.Key.MachineID
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}

	claimed := make([]Entry, 0, len(keys))
	for _, key := range keys {
		entry := p.entries[key]
		leaseToken := leaseSet[entry.ShardID]
		entry.Status = EntryProcessing
		entry.Owner = leaseToken.Owner
		entry.OwnerEpoch = leaseToken.Epoch
		entry.LeaseUntil = nil
		entry.RetryAt = nil
		entry.Attempts++
		entry.StartedAt = &now
		claimed = append(claimed, *cloneEntry(entry))
	}
	return claimed, nil
}

func (p *MemoryProvider) Commit(_ context.Context, cmd AtomicCommit) (*CommitResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := nowUTC()

	if cmd.CompleteSignal != nil {
		if err := validateCompleteSignal(p.signals, p.shardLeases, cmd.CompleteSignal.ID, cmd.CompleteSignal.Owner, cmd.CompleteSignal.OwnerEpoch, cmd.CompleteSignal.Attempt, now); err != nil {
			return nil, err
		}
	}
	if cmd.CompleteEntry != nil {
		if err := validateCompleteEntry(p.entries, p.machines, p.shardLeases, cmd.CompleteEntry.Key, cmd.CompleteEntry.Owner, cmd.CompleteEntry.OwnerEpoch, cmd.CompleteEntry.Attempt, now); err != nil {
			return nil, err
		}
	}

	var transition *preparedMemoryTransition
	var err error
	if cmd.Transition != nil {
		var completedEntry *EntryKey
		if cmd.CompleteEntry != nil {
			completedEntry = &cmd.CompleteEntry.Key
		}
		transition, err = prepareTransition(p.machines, p.entries, completedEntry, *cmd.Transition, now)
		if err != nil {
			return nil, err
		}
	}

	preparedSignals, err := prepareSignalsForMemory(p.signals, cmd.Signals, now)
	if err != nil {
		return nil, err
	}

	if cmd.CompleteSignal != nil {
		completeSignal(p.signals, p.signalIDsByShard, cmd.CompleteSignal.ID, cmd.CompleteSignal.Owner, cmd.CompleteSignal.OwnerEpoch, cmd.CompleteSignal.Attempt, now)
	}
	if cmd.CompleteEntry != nil {
		completeEntry(p.entries, p.entryKeysByShard, cmd.CompleteEntry.Key, cmd.CompleteEntry.Owner, cmd.CompleteEntry.OwnerEpoch, cmd.CompleteEntry.Attempt, now)
	}

	var snap *Snapshot
	var entry *Entry
	if transition != nil {
		snap, entry = applyTransition(p.machines, p.entries, p.entryKeysByShard, transition)
	}
	for i := range preparedSignals {
		applyPreparedSignal(p.signals, p.signalIDsByShard, preparedSignals[i])
	}

	return &CommitResult{
		Snapshot: cloneSnapshot(snap),
		Entry:    cloneEntry(entry),
		Signals:  cloneSignalRecordValues(cmd.Signals),
	}, nil
}

func (p *MemoryProvider) FailEntry(_ context.Context, key EntryKey, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return failEntry(p.entries, p.machines, p.entryKeysByShard, p.shardLeases, key, owner, ownerEpoch, attempt, failure)
}

func (p *MemoryProvider) FailSignal(_ context.Context, id string, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return failSignal(p.signals, p.signalIDsByShard, p.shardLeases, id, owner, ownerEpoch, attempt, failure)
}

func (p *MemoryProvider) currentLeaseSet(leases []ShardLease, now time.Time) map[ShardID]ShardLease {
	leaseSet := make(map[ShardID]ShardLease, len(leases))
	for _, leaseToken := range leases {
		current := p.shardLeases[leaseToken.ShardID]
		if current == nil || current.Owner != leaseToken.Owner || current.Epoch != leaseToken.Epoch || !current.LeaseUntil.After(now) {
			continue
		}
		leaseSet[leaseToken.ShardID] = *current
	}
	return leaseSet
}

type preparedMemoryTransition struct {
	snap       *Snapshot
	newVersion int64
	state      string
	args       []any
	terminalAt *time.Time
	updatedAt  time.Time
	entry      *Entry
}

func prepareTransition(
	machines map[string]*Snapshot,
	entries map[EntryKey]*Entry,
	completedEntry *EntryKey,
	cmd CommitTransition,
	now time.Time,
) (*preparedMemoryTransition, error) {
	snap := machines[cmd.MachineID]
	if snap == nil {
		return nil, ErrMachineNotFound
	}
	if snap.Terminal() {
		return nil, ErrTerminalMachine
	}
	if snap.Version != cmd.ExpectedVersion {
		return nil, ErrVersionConflict
	}
	if err := assertNoOpenEntry(entries, cmd.MachineID, cmd.ExpectedVersion, completedEntry); err != nil {
		return nil, err
	}
	if cmd.Record.Terminal && cmd.Record.CreateEntry {
		return nil, fmt.Errorf("%w: terminal transitions cannot create entry work", ErrInvalidTransition)
	}

	source, dest, trigger, err := validateRecordSymbols(cmd.Record)
	if err != nil {
		return nil, err
	}
	if _, err := encodeArgs(cmd.Record.Args); err != nil {
		return nil, err
	}

	newVersion := cmd.ExpectedVersion + 1
	prepared := &preparedMemoryTransition{
		snap:       snap,
		newVersion: newVersion,
		state:      dest,
		args:       cloneArgs(cmd.Record.Args),
		updatedAt:  now,
	}
	if cmd.Record.Terminal {
		prepared.terminalAt = &now
	}

	if cmd.Record.CreateEntry {
		key := EntryKey{MachineID: cmd.MachineID, Version: newVersion}
		if _, exists := entries[key]; exists {
			return nil, fmt.Errorf("durablestateless: entry %s already exists", key)
		}
		prepared.entry = &Entry{
			Key:         key,
			ShardID:     snap.ShardID,
			SourceState: source,
			DestState:   dest,
			Trigger:     trigger,
			Args:        cloneArgs(cmd.Record.Args),
			Status:      EntryPending,
			CreatedAt:   now,
		}
	}
	return prepared, nil
}

func applyTransition(
	machines map[string]*Snapshot,
	entries map[EntryKey]*Entry,
	entryKeysByShard map[ShardID]map[EntryKey]struct{},
	prepared *preparedMemoryTransition,
) (*Snapshot, *Entry) {
	_ = machines
	snap := prepared.snap
	snap.Version = prepared.newVersion
	snap.State = prepared.state
	snap.Args = cloneArgs(prepared.args)
	snap.UpdatedAt = prepared.updatedAt
	snap.TerminalAt = cloneTime(prepared.terminalAt)
	if prepared.entry == nil {
		return snap, nil
	}
	entry := cloneEntry(prepared.entry)
	entries[entry.Key] = entry
	addEntryIndex(entryKeysByShard, entry.ShardID, entry.Key)
	return snap, entry
}

type preparedMemorySignal struct {
	record   SignalRecord
	argsJSON string
}

func enqueueSignal(signals map[string]*SignalRecord, signalIDsByShard map[ShardID]map[string]struct{}, signal SignalRecord) error {
	prepared, err := prepareSignalForMemory(signal, nowUTC())
	if err != nil {
		return err
	}
	if existing := signals[prepared.record.ID]; existing != nil {
		if signalRecordsEqual(existing, prepared) {
			return nil
		}
		return ErrSignalConflict
	}
	applyPreparedSignal(signals, signalIDsByShard, prepared)
	return nil
}

func prepareSignalForMemory(signal SignalRecord, now time.Time) (preparedMemorySignal, error) {
	if signal.ID == "" {
		return preparedMemorySignal{}, fmt.Errorf("durablestateless: signal id is required")
	}
	if signal.MachineID == "" {
		return preparedMemorySignal{}, fmt.Errorf("durablestateless: signal machine id is required")
	}
	if err := validateShardID(signal.TargetShardID); err != nil {
		return preparedMemorySignal{}, err
	}
	trigger, err := encodeSymbol("trigger", signal.Trigger)
	if err != nil {
		return preparedMemorySignal{}, err
	}
	argsJSON, err := encodeArgs(signal.Args)
	if err != nil {
		return preparedMemorySignal{}, err
	}
	next := signal
	next.Trigger = trigger
	next.Args = cloneArgs(signal.Args)
	next.Status = EntryPending
	next.Owner = ""
	next.OwnerEpoch = 0
	next.LeaseUntil = nil
	next.RetryAt = nil
	next.StartedAt = nil
	next.CompletedAt = nil
	next.LastError = ""
	if next.CreatedAt.IsZero() {
		next.CreatedAt = now
	}
	return preparedMemorySignal{record: next, argsJSON: argsJSON}, nil
}

func prepareSignalsForMemory(signals map[string]*SignalRecord, inputs []SignalRecord, now time.Time) ([]preparedMemorySignal, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	prepared := make([]preparedMemorySignal, 0, len(inputs))
	staged := make(map[string]preparedMemorySignal, len(inputs))
	for _, input := range inputs {
		next, err := prepareSignalForMemory(input, now)
		if err != nil {
			return nil, err
		}
		if existing := signals[next.record.ID]; existing != nil {
			if !signalRecordsEqual(existing, next) {
				return nil, ErrSignalConflict
			}
			continue
		}
		if existing, ok := staged[next.record.ID]; ok {
			if !preparedSignalsEqual(existing, next) {
				return nil, ErrSignalConflict
			}
			continue
		}
		staged[next.record.ID] = next
		prepared = append(prepared, next)
	}
	return prepared, nil
}

func signalRecordsEqual(existing *SignalRecord, next preparedMemorySignal) bool {
	existingArgs, _ := encodeArgs(existing.Args)
	return existing.MachineID == next.record.MachineID &&
		existing.TargetShardID == next.record.TargetShardID &&
		existing.Trigger == next.record.Trigger &&
		existingArgs == next.argsJSON
}

func preparedSignalsEqual(left preparedMemorySignal, right preparedMemorySignal) bool {
	return left.record.MachineID == right.record.MachineID &&
		left.record.TargetShardID == right.record.TargetShardID &&
		left.record.Trigger == right.record.Trigger &&
		left.argsJSON == right.argsJSON
}

func applyPreparedSignal(signals map[string]*SignalRecord, signalIDsByShard map[ShardID]map[string]struct{}, prepared preparedMemorySignal) {
	if _, exists := signals[prepared.record.ID]; exists {
		return
	}
	record := cloneSignalRecord(&prepared.record)
	signals[record.ID] = record
	addSignalIndex(signalIDsByShard, record.TargetShardID, record.ID)
}

func assertNoOpenEntry(entries map[EntryKey]*Entry, machineID string, version int64, completedEntry *EntryKey) error {
	key := EntryKey{MachineID: machineID, Version: version}
	entry := entries[key]
	if entry == nil || entry.Status == EntryDone {
		return nil
	}
	if completedEntry != nil && *completedEntry == key {
		return nil
	}
	return fmt.Errorf("%w: %s is %s", ErrEntryInProgress, entry.Key, entry.Status)
}

func validateRecordSymbols(record TransitionRecord) (string, string, string, error) {
	source, err := encodeSymbol("source state", record.SourceState)
	if err != nil {
		return "", "", "", err
	}
	dest, err := encodeSymbol("destination state", record.DestState)
	if err != nil {
		return "", "", "", err
	}
	trigger, err := encodeSymbol("trigger", record.Trigger)
	if err != nil {
		return "", "", "", err
	}
	return source, dest, trigger, nil
}

func claimableByShardLease(status EntryStatus, owner string, ownerEpoch int64, retryAt *time.Time, lease ShardLease, now time.Time) bool {
	switch status {
	case EntryPending:
		return true
	case EntryFailed:
		return retryAt == nil || !retryAt.After(now)
	case EntryProcessing:
		return owner != lease.Owner || ownerEpoch != lease.Epoch
	default:
		return false
	}
}

func validateCompleteEntry(
	entries map[EntryKey]*Entry,
	machines map[string]*Snapshot,
	shardLeases map[ShardID]*ShardLease,
	key EntryKey,
	owner string,
	ownerEpoch int64,
	attempt int,
	now time.Time,
) error {
	entry := entries[key]
	if entry == nil {
		return ErrEntryNotFound
	}
	if entry.Status == EntryDone {
		if entry.Owner == owner && entry.OwnerEpoch == ownerEpoch && entry.Attempts == attempt {
			return nil
		}
		return ErrEntryNotOwned
	}
	if entry.Status == EntryDeadLettered {
		return ErrWorkDeadLettered
	}
	if entry.Status != EntryProcessing || entry.Owner != owner || entry.OwnerEpoch != ownerEpoch || entry.Attempts != attempt {
		return ErrEntryNotOwned
	}
	machine := machines[key.MachineID]
	if machine == nil {
		return ErrMachineNotFound
	}
	if !shardLeaseOwned(shardLeases, machine.ShardID, owner, ownerEpoch, now) {
		return ErrShardLeaseLost
	}
	return nil
}

func completeEntry(entries map[EntryKey]*Entry, entryKeysByShard map[ShardID]map[EntryKey]struct{}, key EntryKey, owner string, ownerEpoch int64, attempt int, now time.Time) {
	entry := entries[key]
	if entry == nil || entry.Status == EntryDone {
		return
	}
	entry.Status = EntryDone
	entry.LeaseUntil = nil
	entry.RetryAt = nil
	entry.CompletedAt = &now
	removeEntryIndex(entryKeysByShard, entry.ShardID, key)
}

func validateCompleteSignal(signals map[string]*SignalRecord, shardLeases map[ShardID]*ShardLease, id string, owner string, ownerEpoch int64, attempt int, now time.Time) error {
	signal := signals[id]
	if signal == nil {
		return ErrSignalNotFound
	}
	if signal.Status == EntryDone {
		if signal.Owner == owner && signal.OwnerEpoch == ownerEpoch && signal.Attempts == attempt {
			return nil
		}
		return ErrSignalNotOwned
	}
	if signal.Status == EntryDeadLettered {
		return ErrWorkDeadLettered
	}
	if signal.Status != EntryProcessing || signal.Owner != owner || signal.OwnerEpoch != ownerEpoch || signal.Attempts != attempt {
		return ErrSignalNotOwned
	}
	if !shardLeaseOwned(shardLeases, signal.TargetShardID, owner, ownerEpoch, now) {
		return ErrShardLeaseLost
	}
	return nil
}

func completeSignal(signals map[string]*SignalRecord, signalIDsByShard map[ShardID]map[string]struct{}, id string, owner string, ownerEpoch int64, attempt int, now time.Time) {
	signal := signals[id]
	if signal == nil || signal.Status == EntryDone {
		return
	}
	signal.Status = EntryDone
	signal.LeaseUntil = nil
	signal.RetryAt = nil
	signal.CompletedAt = &now
	removeSignalIndex(signalIDsByShard, signal.TargetShardID, id)
}

func failEntry(
	entries map[EntryKey]*Entry,
	machines map[string]*Snapshot,
	entryKeysByShard map[ShardID]map[EntryKey]struct{},
	shardLeases map[ShardID]*ShardLease,
	key EntryKey,
	owner string,
	ownerEpoch int64,
	attempt int,
	failure Failure,
) error {
	entry := entries[key]
	if entry == nil {
		return ErrEntryNotFound
	}
	if entry.Status != EntryProcessing || entry.Owner != owner || entry.OwnerEpoch != ownerEpoch || entry.Attempts != attempt {
		if entry.Status == EntryDeadLettered {
			return ErrWorkDeadLettered
		}
		return ErrEntryNotOwned
	}
	machine := machines[key.MachineID]
	if machine == nil {
		return ErrMachineNotFound
	}
	if !shardLeaseOwned(shardLeases, machine.ShardID, owner, ownerEpoch, nowUTC()) {
		return ErrShardLeaseLost
	}
	if failure.DeadLetter {
		entry.Status = EntryDeadLettered
		entry.RetryAt = nil
		removeEntryIndex(entryKeysByShard, entry.ShardID, key)
	} else {
		entry.Status = EntryFailed
		entry.RetryAt = cloneTime(failure.RetryAt)
	}
	entry.Owner = ""
	entry.OwnerEpoch = 0
	entry.LeaseUntil = nil
	if failure.Cause != nil {
		entry.LastError = failure.Cause.Error()
	}
	return nil
}

func failSignal(signals map[string]*SignalRecord, signalIDsByShard map[ShardID]map[string]struct{}, shardLeases map[ShardID]*ShardLease, id string, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	signal := signals[id]
	if signal == nil {
		return ErrSignalNotFound
	}
	if signal.Status != EntryProcessing || signal.Owner != owner || signal.OwnerEpoch != ownerEpoch || signal.Attempts != attempt {
		if signal.Status == EntryDeadLettered {
			return ErrWorkDeadLettered
		}
		return ErrSignalNotOwned
	}
	if !shardLeaseOwned(shardLeases, signal.TargetShardID, owner, ownerEpoch, nowUTC()) {
		return ErrShardLeaseLost
	}
	if failure.DeadLetter {
		signal.Status = EntryDeadLettered
		signal.RetryAt = nil
		removeSignalIndex(signalIDsByShard, signal.TargetShardID, id)
	} else {
		signal.Status = EntryFailed
		signal.RetryAt = cloneTime(failure.RetryAt)
	}
	signal.Owner = ""
	signal.OwnerEpoch = 0
	signal.LeaseUntil = nil
	if failure.Cause != nil {
		signal.LastError = failure.Cause.Error()
	}
	return nil
}

func shardLeaseOwned(shardLeases map[ShardID]*ShardLease, shard ShardID, owner string, ownerEpoch int64, now time.Time) bool {
	lease := shardLeases[shard]
	return lease != nil &&
		lease.Owner == owner &&
		lease.Epoch == ownerEpoch &&
		lease.LeaseUntil.After(now)
}

func validateLeaseDuration(lease time.Duration) error {
	if lease <= 0 {
		return ErrInvalidLease
	}
	return nil
}

func addEntryIndex(index map[ShardID]map[EntryKey]struct{}, shard ShardID, key EntryKey) {
	if index[shard] == nil {
		index[shard] = make(map[EntryKey]struct{})
	}
	index[shard][key] = struct{}{}
}

func removeEntryIndex(index map[ShardID]map[EntryKey]struct{}, shard ShardID, key EntryKey) {
	keys := index[shard]
	if keys == nil {
		return
	}
	delete(keys, key)
	if len(keys) == 0 {
		delete(index, shard)
	}
}

func addSignalIndex(index map[ShardID]map[string]struct{}, shard ShardID, id string) {
	if index[shard] == nil {
		index[shard] = make(map[string]struct{})
	}
	index[shard][id] = struct{}{}
}

func removeSignalIndex(index map[ShardID]map[string]struct{}, shard ShardID, id string) {
	ids := index[shard]
	if ids == nil {
		return
	}
	delete(ids, id)
	if len(ids) == 0 {
		delete(index, shard)
	}
}

func cloneSnapshots(in map[string]*Snapshot) map[string]*Snapshot {
	out := make(map[string]*Snapshot, len(in))
	for key, snap := range in {
		out[key] = cloneSnapshot(snap)
	}
	return out
}

func cloneEntries(in map[EntryKey]*Entry) map[EntryKey]*Entry {
	out := make(map[EntryKey]*Entry, len(in))
	for key, entry := range in {
		out[key] = cloneEntry(entry)
	}
	return out
}

func cloneSignalRecords(in map[string]*SignalRecord) map[string]*SignalRecord {
	out := make(map[string]*SignalRecord, len(in))
	for key, signal := range in {
		out[key] = cloneSignalRecord(signal)
	}
	return out
}

func cloneShardLeases(in map[ShardID]*ShardLease) map[ShardID]*ShardLease {
	out := make(map[ShardID]*ShardLease, len(in))
	for key, lease := range in {
		if lease == nil {
			continue
		}
		next := *lease
		out[key] = &next
	}
	return out
}

func cloneSnapshot(snap *Snapshot) *Snapshot {
	if snap == nil {
		return nil
	}
	return &Snapshot{
		ID:         snap.ID,
		ShardID:    snap.ShardID,
		State:      snap.State,
		Version:    snap.Version,
		Args:       cloneArgs(snap.Args),
		TerminalAt: cloneTime(snap.TerminalAt),
		UpdatedAt:  snap.UpdatedAt,
	}
}

func cloneEntry(entry *Entry) *Entry {
	if entry == nil {
		return nil
	}
	return &Entry{
		Key:         entry.Key,
		ShardID:     entry.ShardID,
		SourceState: entry.SourceState,
		DestState:   entry.DestState,
		Trigger:     entry.Trigger,
		Args:        cloneArgs(entry.Args),
		Status:      entry.Status,
		Owner:       entry.Owner,
		OwnerEpoch:  entry.OwnerEpoch,
		LeaseUntil:  cloneTime(entry.LeaseUntil),
		RetryAt:     cloneTime(entry.RetryAt),
		Attempts:    entry.Attempts,
		CreatedAt:   entry.CreatedAt,
		StartedAt:   cloneTime(entry.StartedAt),
		CompletedAt: cloneTime(entry.CompletedAt),
		LastError:   entry.LastError,
	}
}

func cloneSignalRecord(signal *SignalRecord) *SignalRecord {
	if signal == nil {
		return nil
	}
	return &SignalRecord{
		Signal: Signal{
			ID:        signal.ID,
			MachineID: signal.MachineID,
			Trigger:   signal.Trigger,
			Args:      cloneArgs(signal.Args),
		},
		TargetShardID: signal.TargetShardID,
		Status:        signal.Status,
		Owner:         signal.Owner,
		OwnerEpoch:    signal.OwnerEpoch,
		LeaseUntil:    cloneTime(signal.LeaseUntil),
		RetryAt:       cloneTime(signal.RetryAt),
		Attempts:      signal.Attempts,
		CreatedAt:     signal.CreatedAt,
		StartedAt:     cloneTime(signal.StartedAt),
		CompletedAt:   cloneTime(signal.CompletedAt),
		LastError:     signal.LastError,
	}
}

func cloneSignalRecordValues(signals []SignalRecord) []SignalRecord {
	if len(signals) == 0 {
		return nil
	}
	out := make([]SignalRecord, len(signals))
	for i := range signals {
		out[i] = *cloneSignalRecord(&signals[i])
	}
	return out
}
