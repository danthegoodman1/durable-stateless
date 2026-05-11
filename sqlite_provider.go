package durablestateless

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteProvider struct {
	db *sql.DB
}

func OpenSQLiteProvider(dsn string) (*SQLiteProvider, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &SQLiteProvider{db: db}, nil
}

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
		`create index if not exists machine_entries_claim_idx
			on machine_entries(status, lease_until, created_at)`,
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

func (p *SQLiteProvider) CreateMachine(ctx context.Context, init MachineInit) error {
	if init.ID == "" {
		return fmt.Errorf("durablestateless: machine id is required")
	}
	state, err := encodeSymbol("state", init.State)
	if err != nil {
		return err
	}
	argsJSON, err := encodeArgs(init.Args)
	if err != nil {
		return err
	}
	now := nowUTC()
	var terminalAt *time.Time
	if init.Terminal {
		terminalAt = &now
	}
	_, err = p.db.ExecContext(ctx,
		`insert into machines(id, shard_id, state, version, args_json, terminal_at, updated_at)
		 values(?, ?, ?, 0, ?, ?, ?)`,
		init.ID, init.ShardID, state, argsJSON, formatOptionalTime(terminalAt), formatTime(now),
	)
	return err
}

func (p *SQLiteProvider) ReadMachine(ctx context.Context, id string) (*Snapshot, error) {
	return readMachineSQL(ctx, p.db, id)
}

func (p *SQLiteProvider) CommitTransition(ctx context.Context, cmd CommitTransition) (*Snapshot, *Entry, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	if err := beginImmediate(ctx, conn); err != nil {
		return nil, nil, err
	}
	committed := false
	defer rollbackUnlessCommitted(conn, &committed)

	snap, entry, err := commitTransitionSQL(ctx, conn, cmd)
	if err != nil {
		return nil, nil, err
	}
	if err := commitSQL(ctx, conn); err != nil {
		return nil, nil, err
	}
	committed = true
	return snap, entry, nil
}

func (p *SQLiteProvider) ClaimEntries(ctx context.Context, owner string, shards []int, limit int, lease time.Duration) ([]Entry, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if limit <= 0 || len(shards) == 0 {
		return nil, nil
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
		  order by e.created_at
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

func (p *SQLiteProvider) CompleteEntry(ctx context.Context, key EntryKey, owner string) error {
	return completeEntrySQL(ctx, p.db, key, owner)
}

func (p *SQLiteProvider) CompleteEntryAndCommitTransition(ctx context.Context, cmd CompleteEntryAndCommitTransition) (*Snapshot, *Entry, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	if err := beginImmediate(ctx, conn); err != nil {
		return nil, nil, err
	}
	committed := false
	defer rollbackUnlessCommitted(conn, &committed)

	if err := completeEntrySQL(ctx, conn, cmd.Complete.Key, cmd.Complete.Owner); err != nil {
		return nil, nil, err
	}
	snap, entry, err := commitTransitionSQL(ctx, conn, cmd.Transition)
	if err != nil {
		return nil, nil, err
	}
	if err := commitSQL(ctx, conn); err != nil {
		return nil, nil, err
	}
	committed = true
	return snap, entry, nil
}

func (p *SQLiteProvider) FailEntry(ctx context.Context, key EntryKey, owner string, cause error) error {
	return failEntrySQL(ctx, p.db, key, owner, cause)
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
	var terminalRaw sql.NullString
	var updatedRaw string
	err := row.Scan(&snap.ID, &snap.ShardID, &state, &snap.Version, &argsJSON, &terminalRaw, &updatedRaw)
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
	var leaseRaw, startedRaw, completedRaw sql.NullString
	var createdRaw string
	err := row.Scan(
		&entry.Key.MachineID, &entry.Key.Version, &entry.ShardID,
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

func completeEntrySQL(ctx context.Context, q sqlRunner, key EntryKey, owner string) error {
	now := nowUTC()
	res, err := q.ExecContext(ctx,
		`update machine_entries
		    set status = 'done',
		        lease_until = null,
		        completed_at = ?
		  where machine_id = ?
		    and version = ?
		    and status = 'processing'
		    and owner = ?`,
		formatTime(now), key.MachineID, key.Version, owner,
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveCompleteMiss(ctx, q, key)
}

func failEntrySQL(ctx context.Context, q sqlRunner, key EntryKey, owner string, cause error) error {
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
		    and owner = ?`,
		message, key.MachineID, key.Version, owner,
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveCompleteMiss(ctx, q, key)
}

func resolveCompleteMiss(ctx context.Context, q sqlRunner, key EntryKey) error {
	var status EntryStatus
	err := q.QueryRowContext(ctx,
		`select status from machine_entries where machine_id = ? and version = ?`,
		key.MachineID, key.Version,
	).Scan(&status)
	if err == sql.ErrNoRows {
		return ErrEntryNotFound
	}
	if err != nil {
		return err
	}
	if status == EntryDone {
		return nil
	}
	return ErrEntryNotOwned
}
