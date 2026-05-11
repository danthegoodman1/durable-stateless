package durablestateless

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/qmuntal/stateless"
)

type Runner struct {
	provider Provider
	def      Definition
	lease    time.Duration
}

func NewRunner(provider Provider, def Definition) *Runner {
	return &Runner{
		provider: provider,
		def:      def,
		lease:    DefaultLeaseDuration,
	}
}

func (r *Runner) WithLeaseDuration(lease time.Duration) *Runner {
	r.lease = lease
	return r
}

func (r *Runner) CreateMachine(ctx context.Context, init MachineInit) error {
	if err := r.validate(); err != nil {
		return err
	}
	init.Terminal = r.def.IsTerminal(init.State)
	return r.provider.CreateMachine(ctx, init)
}

func (r *Runner) ReadMachine(ctx context.Context, id string) (*Snapshot, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r.provider.ReadMachine(ctx, id)
}

func (r *Runner) Fire(ctx context.Context, machineID string, trigger stateless.Trigger, args ...any) error {
	if err := r.validate(); err != nil {
		return err
	}
	snap, err := r.provider.ReadMachine(ctx, machineID)
	if err != nil {
		return err
	}
	cmd, err := r.buildCommit(ctx, snap, trigger, args...)
	if err != nil {
		return err
	}
	if cmd == nil {
		return nil
	}
	_, _, err = r.provider.CommitTransition(ctx, *cmd)
	return err
}

func (r *Runner) Recover(ctx context.Context, owner string, shards []int, limit int) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	lease := r.lease
	if lease == 0 {
		lease = DefaultLeaseDuration
	}
	entries, err := r.provider.ClaimEntries(ctx, owner, shards, limit, lease)
	if err != nil {
		return 0, err
	}
	processed := 0
	var errs []error
	for _, entry := range entries {
		if err := r.ProcessEntry(ctx, entry); err != nil {
			errs = append(errs, err)
			continue
		}
		processed++
	}
	return processed, errors.Join(errs...)
}

func (r *Runner) ProcessEntry(ctx context.Context, entry Entry) error {
	if err := r.validate(); err != nil {
		return err
	}
	if entry.Owner == "" {
		return fmt.Errorf("durablestateless: claimed entry %s has no owner", entry.Key)
	}

	handler, ok := r.def.EntryHandler(entry.DestState)
	if !ok {
		return r.provider.CompleteEntry(ctx, entry.Key, entry.Owner)
	}
	if handler == nil {
		err := fmt.Errorf("%w for state %v", ErrNilEntryHandler, entry.DestState)
		_ = r.provider.FailEntry(ctx, entry.Key, entry.Owner, err)
		return err
	}

	result, err := handler(ctx, entry)
	if err != nil {
		_ = r.provider.FailEntry(ctx, entry.Key, entry.Owner, err)
		return err
	}

	if result.Next == nil {
		return r.provider.CompleteEntry(ctx, entry.Key, entry.Owner)
	}

	snap, err := r.provider.ReadMachine(ctx, entry.Key.MachineID)
	if err != nil {
		_ = r.provider.FailEntry(ctx, entry.Key, entry.Owner, err)
		return err
	}
	cmd, err := r.buildCommit(ctx, snap, result.Next.Trigger, result.Next.Args...)
	if err != nil {
		_ = r.provider.FailEntry(ctx, entry.Key, entry.Owner, err)
		return err
	}
	if cmd == nil {
		return r.provider.CompleteEntry(ctx, entry.Key, entry.Owner)
	}
	_, _, err = r.provider.CompleteEntryAndCommitTransition(ctx, CompleteEntryAndCommitTransition{
		Complete: CompleteEntryCommand{
			Key:   entry.Key,
			Owner: entry.Owner,
		},
		Transition: *cmd,
	})
	if err != nil {
		_ = r.provider.FailEntry(ctx, entry.Key, entry.Owner, err)
		return err
	}
	return nil
}

func (r *Runner) buildCommit(ctx context.Context, snap *Snapshot, trigger stateless.Trigger, args ...any) (cmd *CommitTransition, err error) {
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

func (r *Runner) validate() error {
	if r == nil {
		return fmt.Errorf("durablestateless: nil runner")
	}
	if r.provider == nil {
		return fmt.Errorf("durablestateless: provider is required")
	}
	if r.def == nil {
		return fmt.Errorf("durablestateless: definition is required")
	}
	return nil
}
