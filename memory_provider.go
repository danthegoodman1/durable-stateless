package durablestateless

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type MemoryProvider struct {
	mu       sync.Mutex
	machines map[string]*Snapshot
	entries  map[EntryKey]*Entry
}

func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{
		machines: make(map[string]*Snapshot),
		entries:  make(map[EntryKey]*Entry),
	}
}

func (p *MemoryProvider) Migrate(context.Context) error {
	return nil
}

func (p *MemoryProvider) CreateMachine(_ context.Context, init MachineInit) error {
	if init.ID == "" {
		return fmt.Errorf("durablestateless: machine id is required")
	}
	state, err := encodeSymbol("state", init.State)
	if err != nil {
		return err
	}
	if _, err := encodeArgs(init.Args); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.machines[init.ID]; exists {
		return fmt.Errorf("durablestateless: machine %q already exists", init.ID)
	}

	now := nowUTC()
	var terminalAt *time.Time
	if init.Terminal {
		terminalAt = &now
	}
	p.machines[init.ID] = &Snapshot{
		ID:         init.ID,
		ShardID:    init.ShardID,
		State:      state,
		Version:    0,
		Args:       cloneArgs(init.Args),
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

func (p *MemoryProvider) CommitTransition(_ context.Context, cmd CommitTransition) (*Snapshot, *Entry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	snap, entry, err := p.commitTransitionLocked(cmd)
	if err != nil {
		return nil, nil, err
	}
	return cloneSnapshot(snap), cloneEntry(entry), nil
}

func (p *MemoryProvider) ClaimEntries(_ context.Context, owner string, shards []int, limit int, lease time.Duration) ([]Entry, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if limit <= 0 || len(shards) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	shardSet := make(map[int]struct{}, len(shards))
	for _, shard := range shards {
		shardSet[shard] = struct{}{}
	}

	now := nowUTC()
	keys := make([]EntryKey, 0, len(p.entries))
	for key, entry := range p.entries {
		machine := p.machines[key.MachineID]
		if machine == nil || machine.Terminal() {
			continue
		}
		if key.Version != machine.Version {
			continue
		}
		if _, ok := shardSet[machine.ShardID]; !ok {
			continue
		}
		if claimable(entry, now) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return p.entries[keys[i]].CreatedAt.Before(p.entries[keys[j]].CreatedAt)
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}

	claimed := make([]Entry, 0, len(keys))
	leaseUntil := now.Add(lease)
	for _, key := range keys {
		entry := p.entries[key]
		entry.Status = EntryProcessing
		entry.Owner = owner
		entry.LeaseUntil = &leaseUntil
		entry.Attempts++
		entry.StartedAt = &now
		claimed = append(claimed, *cloneEntry(entry))
	}
	return claimed, nil
}

func (p *MemoryProvider) CompleteEntry(_ context.Context, key EntryKey, owner string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return completeEntry(p.entries, key, owner)
}

func (p *MemoryProvider) CompleteEntryAndCommitTransition(_ context.Context, cmd CompleteEntryAndCommitTransition) (*Snapshot, *Entry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	machines := cloneSnapshots(p.machines)
	entries := cloneEntries(p.entries)
	if err := completeEntry(entries, cmd.Complete.Key, cmd.Complete.Owner); err != nil {
		return nil, nil, err
	}
	snap, entry, err := commitTransition(machines, entries, cmd.Transition)
	if err != nil {
		return nil, nil, err
	}
	p.machines = machines
	p.entries = entries
	return cloneSnapshot(snap), cloneEntry(entry), nil
}

func (p *MemoryProvider) FailEntry(_ context.Context, key EntryKey, owner string, cause error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return failEntry(p.entries, key, owner, cause)
}

func (p *MemoryProvider) commitTransitionLocked(cmd CommitTransition) (*Snapshot, *Entry, error) {
	machines := cloneSnapshots(p.machines)
	entries := cloneEntries(p.entries)
	snap, entry, err := commitTransition(machines, entries, cmd)
	if err != nil {
		return nil, nil, err
	}
	p.machines = machines
	p.entries = entries
	return snap, entry, nil
}

func commitTransition(machines map[string]*Snapshot, entries map[EntryKey]*Entry, cmd CommitTransition) (*Snapshot, *Entry, error) {
	snap := machines[cmd.MachineID]
	if snap == nil {
		return nil, nil, ErrMachineNotFound
	}
	if snap.Terminal() {
		return nil, nil, ErrTerminalMachine
	}
	if snap.Version != cmd.ExpectedVersion {
		return nil, nil, ErrVersionConflict
	}
	if err := assertNoOpenEntry(entries, cmd.MachineID, cmd.ExpectedVersion); err != nil {
		return nil, nil, err
	}

	source, dest, trigger, err := validateRecordSymbols(cmd.Record)
	if err != nil {
		return nil, nil, err
	}
	if _, err := encodeArgs(cmd.Record.Args); err != nil {
		return nil, nil, err
	}

	now := nowUTC()
	snap.Version++
	snap.State = dest
	snap.Args = cloneArgs(cmd.Record.Args)
	snap.UpdatedAt = now
	if cmd.Record.Terminal {
		snap.TerminalAt = &now
	} else {
		snap.TerminalAt = nil
	}

	var entry *Entry
	if cmd.Record.CreateEntry {
		key := EntryKey{MachineID: cmd.MachineID, Version: snap.Version}
		if _, exists := entries[key]; exists {
			return nil, nil, fmt.Errorf("durablestateless: entry %s already exists", key)
		}
		entry = &Entry{
			Key:         key,
			ShardID:     snap.ShardID,
			SourceState: source,
			DestState:   dest,
			Trigger:     trigger,
			Args:        cloneArgs(cmd.Record.Args),
			Status:      EntryPending,
			CreatedAt:   now,
		}
		entries[key] = entry
	}
	return snap, entry, nil
}

func assertNoOpenEntry(entries map[EntryKey]*Entry, machineID string, version int64) error {
	entry := entries[EntryKey{MachineID: machineID, Version: version}]
	if entry == nil || entry.Status == EntryDone {
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

func claimable(entry *Entry, now time.Time) bool {
	switch entry.Status {
	case EntryPending, EntryFailed:
		return true
	case EntryProcessing:
		return entry.LeaseUntil == nil || entry.LeaseUntil.Before(now)
	default:
		return false
	}
}

func completeEntry(entries map[EntryKey]*Entry, key EntryKey, owner string) error {
	entry := entries[key]
	if entry == nil {
		return ErrEntryNotFound
	}
	if entry.Status == EntryDone {
		return nil
	}
	if entry.Status != EntryProcessing || entry.Owner != owner {
		return ErrEntryNotOwned
	}
	now := nowUTC()
	entry.Status = EntryDone
	entry.LeaseUntil = nil
	entry.CompletedAt = &now
	return nil
}

func failEntry(entries map[EntryKey]*Entry, key EntryKey, owner string, cause error) error {
	entry := entries[key]
	if entry == nil {
		return ErrEntryNotFound
	}
	if entry.Status != EntryProcessing || entry.Owner != owner {
		return ErrEntryNotOwned
	}
	entry.Status = EntryFailed
	entry.Owner = ""
	entry.LeaseUntil = nil
	if cause != nil {
		entry.LastError = cause.Error()
	}
	return nil
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
		LeaseUntil:  cloneTime(entry.LeaseUntil),
		Attempts:    entry.Attempts,
		CreatedAt:   entry.CreatedAt,
		StartedAt:   cloneTime(entry.StartedAt),
		CompletedAt: cloneTime(entry.CompletedAt),
		LastError:   entry.LastError,
	}
}
