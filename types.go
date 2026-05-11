package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/qmuntal/stateless"
)

const DefaultLeaseDuration = 30 * time.Second

var (
	ErrMachineNotFound = errors.New("durablestateless: machine not found")
	ErrEntryNotFound   = errors.New("durablestateless: entry not found")
	ErrEntryNotOwned   = errors.New("durablestateless: entry is not processing for owner")
	ErrEntryInProgress = errors.New("durablestateless: current entry is not complete")
	ErrNilEntryHandler = errors.New("durablestateless: entry handler is nil")
	ErrVersionConflict = errors.New("durablestateless: machine version conflict")
	ErrTerminalMachine = errors.New("durablestateless: machine is terminal")
)

type EntryStatus string

const (
	EntryPending    EntryStatus = "pending"
	EntryProcessing EntryStatus = "processing"
	EntryDone       EntryStatus = "done"
	EntryFailed     EntryStatus = "failed"
)

type EntryKey struct {
	MachineID string
	Version   int64
}

func (k EntryKey) String() string {
	return fmt.Sprintf("%s/%d", k.MachineID, k.Version)
}

type MachineInit struct {
	ID       string
	ShardID  int
	State    stateless.State
	Args     []any
	Terminal bool
}

type Snapshot struct {
	ID         string
	ShardID    int
	State      stateless.State
	Version    int64
	Args       []any
	TerminalAt *time.Time
	UpdatedAt  time.Time
}

func (s Snapshot) Terminal() bool {
	return s.TerminalAt != nil
}

type Entry struct {
	Key         EntryKey
	ShardID     int
	SourceState stateless.State
	DestState   stateless.State
	Trigger     stateless.Trigger
	Args        []any
	Status      EntryStatus
	Owner       string
	LeaseUntil  *time.Time
	Attempts    int
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	LastError   string
}

type TransitionRecord struct {
	SourceState stateless.State
	DestState   stateless.State
	Trigger     stateless.Trigger
	Args        []any
	Terminal    bool
	CreateEntry bool
}

type CommitTransition struct {
	MachineID       string
	ExpectedVersion int64
	Record          TransitionRecord
}

type CompleteEntryCommand struct {
	Key   EntryKey
	Owner string
}

type CompleteEntryAndCommitTransition struct {
	Complete   CompleteEntryCommand
	Transition CommitTransition
}

type Provider interface {
	Migrate(ctx context.Context) error
	CreateMachine(ctx context.Context, init MachineInit) error
	ReadMachine(ctx context.Context, id string) (*Snapshot, error)
	CommitTransition(ctx context.Context, cmd CommitTransition) (*Snapshot, *Entry, error)
	ClaimEntries(ctx context.Context, owner string, shards []int, limit int, lease time.Duration) ([]Entry, error)
	CompleteEntry(ctx context.Context, key EntryKey, owner string) error
	CompleteEntryAndCommitTransition(ctx context.Context, cmd CompleteEntryAndCommitTransition) (*Snapshot, *Entry, error)
	FailEntry(ctx context.Context, key EntryKey, owner string, cause error) error
}

type Definition interface {
	Configure(rules *Rules)
	IsTerminal(state stateless.State) bool
	EntryHandler(state stateless.State) (EntryHandler, bool)
}

type EntryHandler func(context.Context, Entry) (HandlerResult, error)

type HandlerResult struct {
	Next *NextTrigger
}

type NextTrigger struct {
	Trigger stateless.Trigger
	Args    []any
}

func NoNext() HandlerResult {
	return HandlerResult{}
}

func FireNext(trigger stateless.Trigger, args ...any) HandlerResult {
	return HandlerResult{
		Next: &NextTrigger{
			Trigger: trigger,
			Args:    cloneArgs(args),
		},
	}
}
