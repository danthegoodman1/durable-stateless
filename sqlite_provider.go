package durablestateless

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteProvider stores durable machines, signals, and entry work in SQLite.
// It uses short internal transactions to implement Provider.Commit.
type SQLiteProvider struct {
	db *sql.DB
}

// OpenSQLiteProvider opens a SQLite-backed Provider with the given DSN.
func OpenSQLiteProvider(dsn string) (*SQLiteProvider, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &SQLiteProvider{db: db}, nil
}

// Close closes the underlying SQLite database handle.
func (p *SQLiteProvider) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}

func (p *SQLiteProvider) Migrate(ctx context.Context) error {
	stmts := []string{
		`pragma foreign_keys = on`,
		`pragma busy_timeout = 5000`,
		`pragma journal_mode = wal`,
		`create table if not exists machines (
			id text primary key,
			shard_id integer not null,
			state text not null,
			version integer not null,
			args_json text not null,
			terminal_at text,
			updated_at text not null
		)`,
		`create table if not exists machine_entries (
			id text primary key,
			machine_id text not null,
			version integer not null,
			source_state text not null,
			dest_state text not null,
			trigger text not null,
			args_json text not null,
			status text not null,
			owner text,
			lease_until text,
			attempts integer not null default 0,
			created_at text not null,
			started_at text,
			completed_at text,
			last_error text,
			unique(machine_id, version),
			foreign key(machine_id) references machines(id)
		)`,
		`create table if not exists machine_signals (
			id text primary key,
			machine_id text not null,
			target_shard_id integer not null,
			trigger text not null,
			args_json text not null,
			status text not null,
			owner text,
			lease_until text,
			attempts integer not null default 0,
			created_at text not null,
			started_at text,
			completed_at text,
			last_error text,
			foreign key(machine_id) references machines(id)
		)`,
		`create index if not exists machine_entries_claim_idx
			on machine_entries(status, lease_until, created_at)`,
		`create index if not exists machine_signals_claim_idx
			on machine_signals(target_shard_id, status, lease_until, created_at)`,
		`create index if not exists machines_shard_idx
			on machines(shard_id, terminal_at)`,
	}
	for _, stmt := range stmts {
		if _, err := p.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (p *SQLiteProvider) CreateMachine(ctx context.Context, record MachineRecord) error {
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
	argsJSON, err := encodeArgs(record.Args)
	if err != nil {
		return err
	}
	now := nowUTC()
	var terminalAt *time.Time
	if record.Terminal {
		terminalAt = &now
	}
	_, err = p.db.ExecContext(ctx,
		`insert into machines(id, shard_id, state, version, args_json, terminal_at, updated_at)
		 values(?, ?, ?, 0, ?, ?, ?)`,
		record.ID, record.ShardID, state, argsJSON, formatOptionalTime(terminalAt), formatTime(now),
	)
	return err
}

func (p *SQLiteProvider) ReadMachine(ctx context.Context, id string) (*Snapshot, error) {
	return readMachineSQL(ctx, p.db, id)
}

func (p *SQLiteProvider) EnqueueSignal(ctx context.Context, signal SignalRecord) error {
	return enqueueSignalSQL(ctx, p.db, signal)
}

func (p *SQLiteProvider) ClaimSignals(ctx context.Context, owner string, shards []ShardID, limit int, lease time.Duration) ([]SignalRecord, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if limit <= 0 || len(shards) == 0 {
		return nil, nil
	}
	if err := validateShardIDs(shards); err != nil {
		return nil, err
	}

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := beginImmediate(ctx, conn); err != nil {
		return nil, err
	}
	committed := false
	defer rollbackUnlessCommitted(conn, &committed)

	placeholders := make([]string, len(shards))
	args := make([]any, 0, len(shards)+2)
	for i, shard := range shards {
		placeholders[i] = "?"
		args = append(args, shard)
	}
	now := nowUTC()
	args = append(args, formatTime(now), limit)
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`select id
		   from machine_signals
		  where target_shard_id in (%s)
		    and (status in ('pending', 'failed')
		      or (status = 'processing' and (lease_until is null or lease_until < ?)))
		  order by created_at, id
		  limit ?`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	leaseUntil := now.Add(lease)
	for _, id := range ids {
		res, err := conn.ExecContext(ctx,
			`update machine_signals
			    set status = 'processing',
			        owner = ?,
			        lease_until = ?,
			        attempts = attempts + 1,
			        started_at = ?
			  where id = ?`,
			owner, formatTime(leaseUntil), formatTime(now), id,
		)
		if err != nil {
			return nil, err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return nil, ErrSignalNotFound
		}
	}

	claimed := make([]SignalRecord, 0, len(ids))
	for _, id := range ids {
		signal, err := readSignalSQL(ctx, conn, id)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, *signal)
	}

	if err := commitSQL(ctx, conn); err != nil {
		return nil, err
	}
	committed = true
	return claimed, nil
}

func (p *SQLiteProvider) ClaimEntries(ctx context.Context, owner string, shards []ShardID, limit int, lease time.Duration) ([]Entry, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if limit <= 0 || len(shards) == 0 {
		return nil, nil
	}
	if err := validateShardIDs(shards); err != nil {
		return nil, err
	}

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := beginImmediate(ctx, conn); err != nil {
		return nil, err
	}
	committed := false
	defer rollbackUnlessCommitted(conn, &committed)

	placeholders := make([]string, len(shards))
	args := make([]any, 0, len(shards)+2)
	for i, shard := range shards {
		placeholders[i] = "?"
		args = append(args, shard)
	}
	now := nowUTC()
	args = append(args, formatTime(now), limit)
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`select e.machine_id, e.version
		   from machine_entries e
		   join machines m on m.id = e.machine_id
		  where m.shard_id in (%s)
		    and m.terminal_at is null
		    and e.version = m.version
		    and (e.status in ('pending', 'failed')
		      or (e.status = 'processing' and (e.lease_until is null or e.lease_until < ?)))
		  order by e.created_at, e.machine_id, e.version
		  limit ?`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, err
	}
	var keys []EntryKey
	for rows.Next() {
		var key EntryKey
		if err := rows.Scan(&key.MachineID, &key.Version); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	leaseUntil := now.Add(lease)
	for _, key := range keys {
		res, err := conn.ExecContext(ctx,
			`update machine_entries
			    set status = 'processing',
			        owner = ?,
			        lease_until = ?,
			        attempts = attempts + 1,
			        started_at = ?
			  where machine_id = ? and version = ?`,
			owner, formatTime(leaseUntil), formatTime(now), key.MachineID, key.Version,
		)
		if err != nil {
			return nil, err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return nil, ErrEntryNotFound
		}
	}

	claimed := make([]Entry, 0, len(keys))
	for _, key := range keys {
		entry, err := readEntrySQL(ctx, conn, key)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, *entry)
	}

	if err := commitSQL(ctx, conn); err != nil {
		return nil, err
	}
	committed = true
	return claimed, nil
}

func (p *SQLiteProvider) Commit(ctx context.Context, cmd AtomicCommit) (*CommitResult, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := beginImmediate(ctx, conn); err != nil {
		return nil, err
	}
	committed := false
	defer rollbackUnlessCommitted(conn, &committed)

	if cmd.CompleteSignal != nil {
		if err := completeSignalSQL(ctx, conn, cmd.CompleteSignal.ID, cmd.CompleteSignal.Owner, cmd.CompleteSignal.Attempt); err != nil {
			return nil, err
		}
	}
	if cmd.CompleteEntry != nil {
		if err := completeEntrySQL(ctx, conn, cmd.CompleteEntry.Key, cmd.CompleteEntry.Owner, cmd.CompleteEntry.Attempt); err != nil {
			return nil, err
		}
	}

	var snap *Snapshot
	var entry *Entry
	if cmd.Transition != nil {
		snap, entry, err = commitTransitionSQL(ctx, conn, *cmd.Transition)
		if err != nil {
			return nil, err
		}
	}

	for _, signal := range cmd.Signals {
		if err := enqueueSignalSQL(ctx, conn, signal); err != nil {
			return nil, err
		}
	}

	if err := commitSQL(ctx, conn); err != nil {
		return nil, err
	}
	committed = true
	return &CommitResult{
		Snapshot: snap,
		Entry:    entry,
		Signals:  cloneSignalRecordValues(cmd.Signals),
	}, nil
}

func (p *SQLiteProvider) FailEntry(ctx context.Context, key EntryKey, owner string, attempt int, cause error) error {
	return failEntrySQL(ctx, p.db, key, owner, attempt, cause)
}

func (p *SQLiteProvider) FailSignal(ctx context.Context, id string, owner string, attempt int, cause error) error {
	return failSignalSQL(ctx, p.db, id, owner, attempt, cause)
}

type sqlRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func beginImmediate(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, `begin immediate`)
	return err
}

func commitSQL(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, `commit`)
	return err
}

func rollbackUnlessCommitted(conn *sql.Conn, committed *bool) {
	if !*committed {
		_, _ = conn.ExecContext(context.Background(), `rollback`)
	}
}

func enqueueSignalSQL(ctx context.Context, q sqlRunner, signal SignalRecord) error {
	if signal.ID == "" {
		return fmt.Errorf("durablestateless: signal id is required")
	}
	if signal.MachineID == "" {
		return fmt.Errorf("durablestateless: signal machine id is required")
	}
	if err := validateShardID(signal.TargetShardID); err != nil {
		return err
	}
	trigger, err := encodeSymbol("trigger", signal.Trigger)
	if err != nil {
		return err
	}
	argsJSON, err := encodeArgs(signal.Args)
	if err != nil {
		return err
	}
	now := nowUTC()
	createdAt := signal.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	_, err = q.ExecContext(ctx,
		`insert into machine_signals(
			id, machine_id, target_shard_id, trigger, args_json,
			status, attempts, created_at
		) values(?, ?, ?, ?, ?, 'pending', 0, ?)`,
		signal.ID, signal.MachineID, signal.TargetShardID, trigger, argsJSON, formatTime(createdAt),
	)
	if err == nil {
		return nil
	}

	existing, readErr := readSignalSQL(ctx, q, signal.ID)
	if readErr != nil {
		return err
	}
	existingArgs, _ := encodeArgs(existing.Args)
	if existing.MachineID == signal.MachineID &&
		existing.TargetShardID == signal.TargetShardID &&
		existing.Trigger == trigger &&
		existingArgs == argsJSON {
		return nil
	}
	return ErrSignalConflict
}

func commitTransitionSQL(ctx context.Context, q sqlRunner, cmd CommitTransition) (*Snapshot, *Entry, error) {
	source, dest, trigger, err := validateRecordSymbols(cmd.Record)
	if err != nil {
		return nil, nil, err
	}
	argsJSON, err := encodeArgs(cmd.Record.Args)
	if err != nil {
		return nil, nil, err
	}

	snap, err := readMachineSQL(ctx, q, cmd.MachineID)
	if err != nil {
		return nil, nil, err
	}
	if snap.Terminal() {
		return nil, nil, ErrTerminalMachine
	}
	if snap.Version != cmd.ExpectedVersion {
		return nil, nil, ErrVersionConflict
	}
	if err := assertNoOpenEntrySQL(ctx, q, cmd.MachineID, cmd.ExpectedVersion); err != nil {
		return nil, nil, err
	}
	if cmd.Record.Terminal && cmd.Record.CreateEntry {
		return nil, nil, fmt.Errorf("%w: terminal transitions cannot create entry work", ErrInvalidTransition)
	}

	now := nowUTC()
	newVersion := cmd.ExpectedVersion + 1
	var terminalAt *time.Time
	if cmd.Record.Terminal {
		terminalAt = &now
	}
	res, err := q.ExecContext(ctx,
		`update machines
		    set state = ?,
		        version = ?,
		        args_json = ?,
		        terminal_at = ?,
		        updated_at = ?
		  where id = ? and version = ?`,
		dest, newVersion, argsJSON, formatOptionalTime(terminalAt), formatTime(now), cmd.MachineID, cmd.ExpectedVersion,
	)
	if err != nil {
		return nil, nil, err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return nil, nil, ErrVersionConflict
	}

	snap.State = dest
	snap.Version = newVersion
	snap.Args = cloneArgs(cmd.Record.Args)
	snap.TerminalAt = cloneTime(terminalAt)
	snap.UpdatedAt = now

	var entry *Entry
	if cmd.Record.CreateEntry {
		key := EntryKey{MachineID: cmd.MachineID, Version: newVersion}
		_, err = q.ExecContext(ctx,
			`insert into machine_entries(
				id, machine_id, version, source_state, dest_state, trigger,
				args_json, status, attempts, created_at
			) values(?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?)`,
			key.String(), cmd.MachineID, newVersion, source, dest, trigger, argsJSON, formatTime(now),
		)
		if err != nil {
			return nil, nil, err
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
	}

	return snap, entry, nil
}

func assertNoOpenEntrySQL(ctx context.Context, q sqlRunner, machineID string, version int64) error {
	var status EntryStatus
	err := q.QueryRowContext(ctx,
		`select status from machine_entries where machine_id = ? and version = ?`,
		machineID, version,
	).Scan(&status)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if status == EntryDone {
		return nil
	}
	return fmt.Errorf("%w: %s/%d is %s", ErrEntryInProgress, machineID, version, status)
}

func readMachineSQL(ctx context.Context, q sqlRunner, id string) (*Snapshot, error) {
	row := q.QueryRowContext(ctx,
		`select id, shard_id, state, version, args_json, terminal_at, updated_at
		   from machines
		  where id = ?`, id)

	var snap Snapshot
	var argsJSON string
	var state string
	var shard int
	var terminalRaw sql.NullString
	var updatedRaw string
	err := row.Scan(&snap.ID, &shard, &state, &snap.Version, &argsJSON, &terminalRaw, &updatedRaw)
	if err == sql.ErrNoRows {
		return nil, ErrMachineNotFound
	}
	if err != nil {
		return nil, err
	}
	args, err := decodeArgs(argsJSON)
	if err != nil {
		return nil, err
	}
	terminalAt, err := parseOptionalTime(terminalRaw)
	if err != nil {
		return nil, err
	}
	updatedAt, err := parseRequiredTime(updatedRaw)
	if err != nil {
		return nil, err
	}
	snap.Args = args
	snap.ShardID = ShardID(shard)
	snap.State = state
	snap.TerminalAt = terminalAt
	snap.UpdatedAt = updatedAt
	return &snap, nil
}

func readEntrySQL(ctx context.Context, q sqlRunner, key EntryKey) (*Entry, error) {
	row := q.QueryRowContext(ctx,
		`select e.machine_id, e.version, m.shard_id,
		        e.source_state, e.dest_state, e.trigger, e.args_json,
		        e.status, coalesce(e.owner, ''), e.lease_until, e.attempts,
		        e.created_at, e.started_at, e.completed_at, coalesce(e.last_error, '')
		   from machine_entries e
		   join machines m on m.id = e.machine_id
		  where e.machine_id = ? and e.version = ?`, key.MachineID, key.Version)

	var entry Entry
	var argsJSON string
	var source, dest, trigger string
	var shard int
	var leaseRaw, startedRaw, completedRaw sql.NullString
	var createdRaw string
	err := row.Scan(
		&entry.Key.MachineID, &entry.Key.Version, &shard,
		&source, &dest, &trigger, &argsJSON,
		&entry.Status, &entry.Owner, &leaseRaw, &entry.Attempts,
		&createdRaw, &startedRaw, &completedRaw, &entry.LastError,
	)
	if err == sql.ErrNoRows {
		return nil, ErrEntryNotFound
	}
	if err != nil {
		return nil, err
	}
	args, err := decodeArgs(argsJSON)
	if err != nil {
		return nil, err
	}
	createdAt, err := parseRequiredTime(createdRaw)
	if err != nil {
		return nil, err
	}
	leaseUntil, err := parseOptionalTime(leaseRaw)
	if err != nil {
		return nil, err
	}
	startedAt, err := parseOptionalTime(startedRaw)
	if err != nil {
		return nil, err
	}
	completedAt, err := parseOptionalTime(completedRaw)
	if err != nil {
		return nil, err
	}
	entry.ShardID = ShardID(shard)
	entry.Args = args
	entry.SourceState = source
	entry.DestState = dest
	entry.Trigger = trigger
	entry.CreatedAt = createdAt
	entry.LeaseUntil = leaseUntil
	entry.StartedAt = startedAt
	entry.CompletedAt = completedAt
	return &entry, nil
}

func readSignalSQL(ctx context.Context, q sqlRunner, id string) (*SignalRecord, error) {
	row := q.QueryRowContext(ctx,
		`select id, machine_id, target_shard_id, trigger, args_json,
		        status, coalesce(owner, ''), lease_until, attempts,
		        created_at, started_at, completed_at, coalesce(last_error, '')
		   from machine_signals
		  where id = ?`, id)

	var signal SignalRecord
	var argsJSON string
	var trigger string
	var shard int
	var leaseRaw, startedRaw, completedRaw sql.NullString
	var createdRaw string
	err := row.Scan(
		&signal.ID, &signal.MachineID, &shard, &trigger, &argsJSON,
		&signal.Status, &signal.Owner, &leaseRaw, &signal.Attempts,
		&createdRaw, &startedRaw, &completedRaw, &signal.LastError,
	)
	if err == sql.ErrNoRows {
		return nil, ErrSignalNotFound
	}
	if err != nil {
		return nil, err
	}
	args, err := decodeArgs(argsJSON)
	if err != nil {
		return nil, err
	}
	createdAt, err := parseRequiredTime(createdRaw)
	if err != nil {
		return nil, err
	}
	leaseUntil, err := parseOptionalTime(leaseRaw)
	if err != nil {
		return nil, err
	}
	startedAt, err := parseOptionalTime(startedRaw)
	if err != nil {
		return nil, err
	}
	completedAt, err := parseOptionalTime(completedRaw)
	if err != nil {
		return nil, err
	}
	signal.TargetShardID = ShardID(shard)
	signal.Trigger = trigger
	signal.Args = args
	signal.CreatedAt = createdAt
	signal.LeaseUntil = leaseUntil
	signal.StartedAt = startedAt
	signal.CompletedAt = completedAt
	return &signal, nil
}

func completeEntrySQL(ctx context.Context, q sqlRunner, key EntryKey, owner string, attempt int) error {
	now := nowUTC()
	res, err := q.ExecContext(ctx,
		`update machine_entries
		    set status = 'done',
		        lease_until = null,
		        completed_at = ?
		  where machine_id = ?
		    and version = ?
		    and status = 'processing'
		    and owner = ?
		    and attempts = ?`,
		formatTime(now), key.MachineID, key.Version, owner, attempt,
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveEntryCompleteMiss(ctx, q, key, owner, attempt, true)
}

func completeSignalSQL(ctx context.Context, q sqlRunner, id string, owner string, attempt int) error {
	now := nowUTC()
	res, err := q.ExecContext(ctx,
		`update machine_signals
		    set status = 'done',
		        lease_until = null,
		        completed_at = ?
		  where id = ?
		    and status = 'processing'
		    and owner = ?
		    and attempts = ?`,
		formatTime(now), id, owner, attempt,
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveSignalCompleteMiss(ctx, q, id, owner, attempt, true)
}

func failEntrySQL(ctx context.Context, q sqlRunner, key EntryKey, owner string, attempt int, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	res, err := q.ExecContext(ctx,
		`update machine_entries
		    set status = 'failed',
		        owner = null,
		        lease_until = null,
		        last_error = ?
		  where machine_id = ?
		    and version = ?
		    and status = 'processing'
		    and owner = ?
		    and attempts = ?`,
		message, key.MachineID, key.Version, owner, attempt,
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveEntryCompleteMiss(ctx, q, key, owner, attempt, false)
}

func failSignalSQL(ctx context.Context, q sqlRunner, id string, owner string, attempt int, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	res, err := q.ExecContext(ctx,
		`update machine_signals
		    set status = 'failed',
		        owner = null,
		        lease_until = null,
		        last_error = ?
		  where id = ?
		    and status = 'processing'
		    and owner = ?
		    and attempts = ?`,
		message, id, owner, attempt,
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveSignalCompleteMiss(ctx, q, id, owner, attempt, false)
}

func resolveEntryCompleteMiss(ctx context.Context, q sqlRunner, key EntryKey, requestedOwner string, requestedAttempt int, allowDone bool) error {
	var status EntryStatus
	var owner string
	var attempt int
	err := q.QueryRowContext(ctx,
		`select status, coalesce(owner, ''), attempts from machine_entries where machine_id = ? and version = ?`,
		key.MachineID, key.Version,
	).Scan(&status, &owner, &attempt)
	if err == sql.ErrNoRows {
		return ErrEntryNotFound
	}
	if err != nil {
		return err
	}
	if status == EntryDone && allowDone && owner == requestedOwner && attempt == requestedAttempt {
		return nil
	}
	return ErrEntryNotOwned
}

func resolveSignalCompleteMiss(ctx context.Context, q sqlRunner, id string, requestedOwner string, requestedAttempt int, allowDone bool) error {
	var status EntryStatus
	var owner string
	var attempt int
	err := q.QueryRowContext(ctx,
		`select status, coalesce(owner, ''), attempts from machine_signals where id = ?`, id,
	).Scan(&status, &owner, &attempt)
	if err == sql.ErrNoRows {
		return ErrSignalNotFound
	}
	if err != nil {
		return err
	}
	if status == EntryDone && allowDone && owner == requestedOwner && attempt == requestedAttempt {
		return nil
	}
	return ErrSignalNotOwned
}
