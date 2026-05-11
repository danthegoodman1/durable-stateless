package durablestateless

import (
	"context"
	"database/sql"
	"errors"
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
			owner_epoch integer not null default 0,
			lease_until text,
			retry_at text,
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
			owner_epoch integer not null default 0,
			lease_until text,
			retry_at text,
			attempts integer not null default 0,
			created_at text not null,
			started_at text,
			completed_at text,
			last_error text,
			foreign key(machine_id) references machines(id)
		)`,
		`create table if not exists shard_leases (
			shard_id integer primary key,
			owner text not null,
			epoch integer not null,
			lease_until text not null,
			updated_at text not null
		)`,
	}
	for _, stmt := range stmts {
		if _, err := p.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := addColumnIfMissing(ctx, p.db, "machine_entries", "retry_at", "text"); err != nil {
		return err
	}
	if err := addColumnIfMissing(ctx, p.db, "machine_signals", "retry_at", "text"); err != nil {
		return err
	}
	if err := addColumnIfMissing(ctx, p.db, "machine_entries", "owner_epoch", "integer not null default 0"); err != nil {
		return err
	}
	if err := addColumnIfMissing(ctx, p.db, "machine_signals", "owner_epoch", "integer not null default 0"); err != nil {
		return err
	}
	indexes := []string{
		`create index if not exists machine_entries_claim_idx
			on machine_entries(status, lease_until, created_at)`,
		`create index if not exists machine_entries_retry_claim_idx
			on machine_entries(status, retry_at, lease_until, created_at)`,
		`create index if not exists machine_signals_claim_idx
			on machine_signals(target_shard_id, status, lease_until, created_at)`,
		`create index if not exists machine_signals_retry_claim_idx
			on machine_signals(target_shard_id, status, retry_at, lease_until, created_at)`,
		`create index if not exists machines_shard_idx
			on machines(shard_id, terminal_at)`,
	}
	for _, stmt := range indexes {
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

func (p *SQLiteProvider) AcquireShardLeases(ctx context.Context, owner string, shards []ShardID, lease time.Duration) ([]ShardLease, error) {
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

	now := nowUTC()
	leaseUntil := now.Add(lease)
	leases := make([]ShardLease, 0, len(shards))
	for _, shard := range shards {
		current, err := readShardLeaseSQL(ctx, conn, shard)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if current != nil && current.LeaseUntil.After(now) && current.Owner != owner {
			continue
		}

		epoch := int64(1)
		if current != nil {
			epoch = current.Epoch + 1
			_, err = conn.ExecContext(ctx,
				`update shard_leases
				    set owner = ?,
				        epoch = ?,
				        lease_until = ?,
				        updated_at = ?
				  where shard_id = ?`,
				owner, epoch, formatTime(leaseUntil), formatTime(now), shard,
			)
		} else {
			_, err = conn.ExecContext(ctx,
				`insert into shard_leases(shard_id, owner, epoch, lease_until, updated_at)
				 values(?, ?, ?, ?, ?)`,
				shard, owner, epoch, formatTime(leaseUntil), formatTime(now),
			)
		}
		if err != nil {
			return nil, err
		}
		leases = append(leases, ShardLease{
			ShardID:    shard,
			Owner:      owner,
			Epoch:      epoch,
			LeaseUntil: leaseUntil,
			UpdatedAt:  now,
		})
	}

	if err := commitSQL(ctx, conn); err != nil {
		return nil, err
	}
	committed = true
	return leases, nil
}

func (p *SQLiteProvider) RenewShardLeases(ctx context.Context, owner string, leases []ShardLease, lease time.Duration) ([]ShardLease, error) {
	if owner == "" {
		return nil, fmt.Errorf("durablestateless: owner is required")
	}
	if err := validateLeaseDuration(lease); err != nil {
		return nil, err
	}
	if len(leases) == 0 {
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

	now := nowUTC()
	leaseUntil := now.Add(lease)
	renewed := make([]ShardLease, 0, len(leases))
	for _, leaseToken := range leases {
		res, err := conn.ExecContext(ctx,
			`update shard_leases
			    set lease_until = ?,
			        updated_at = ?
			  where shard_id = ?
			    and owner = ?
			    and epoch = ?
			    and lease_until > ?`,
			formatTime(leaseUntil), formatTime(now), leaseToken.ShardID, owner, leaseToken.Epoch, formatTime(now),
		)
		if err != nil {
			return nil, err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return nil, ErrShardLeaseLost
		}
		renewed = append(renewed, ShardLease{
			ShardID:    leaseToken.ShardID,
			Owner:      owner,
			Epoch:      leaseToken.Epoch,
			LeaseUntil: leaseUntil,
			UpdatedAt:  now,
		})
	}

	if err := commitSQL(ctx, conn); err != nil {
		return nil, err
	}
	committed = true
	return renewed, nil
}

func (p *SQLiteProvider) ClaimSignals(ctx context.Context, leases []ShardLease, limit int) ([]SignalRecord, error) {
	if limit <= 0 || len(leases) == 0 {
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

	now := nowUTC()
	leaseSet, err := currentLeaseSetSQL(ctx, conn, leases, now)
	if err != nil {
		return nil, err
	}
	if len(leaseSet) == 0 {
		if err := commitSQL(ctx, conn); err != nil {
			return nil, err
		}
		committed = true
		return nil, nil
	}

	placeholders, args := placeholdersForLeaseSet(leaseSet)
	args = append(args, formatTime(now), formatTime(now), limit)
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`select s.id
		   from machine_signals s
		   join shard_leases l on l.shard_id = s.target_shard_id
		  where s.target_shard_id in (%s)
		    and l.lease_until > ?
		    and (s.status = 'pending'
		      or (s.status = 'failed' and (s.retry_at is null or s.retry_at <= ?))
		      or (s.status = 'processing' and (coalesce(s.owner, '') != l.owner or s.owner_epoch != l.epoch)))
		  order by s.created_at, s.id
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

	claimedIDs := make([]string, 0, minInt(limit, len(ids)))
	for _, id := range ids {
		if len(claimedIDs) >= limit {
			break
		}
		signal, err := readSignalSQL(ctx, conn, id)
		if err != nil {
			return nil, err
		}
		leaseToken := leaseSet[signal.TargetShardID]
		if !claimableByShardLease(signal.Status, signal.Owner, signal.OwnerEpoch, signal.RetryAt, leaseToken, now) {
			continue
		}
		res, err := conn.ExecContext(ctx,
			`update machine_signals
			    set status = 'processing',
			        owner = ?,
			        owner_epoch = ?,
			        lease_until = null,
			        retry_at = null,
			        attempts = attempts + 1,
			        started_at = ?
			  where id = ?`,
			leaseToken.Owner, leaseToken.Epoch, formatTime(now), id,
		)
		if err != nil {
			return nil, err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return nil, ErrSignalNotFound
		}
		claimedIDs = append(claimedIDs, id)
	}

	claimed := make([]SignalRecord, 0, len(claimedIDs))
	for _, id := range claimedIDs {
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

func (p *SQLiteProvider) ClaimEntries(ctx context.Context, leases []ShardLease, limit int) ([]Entry, error) {
	if limit <= 0 || len(leases) == 0 {
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

	now := nowUTC()
	leaseSet, err := currentLeaseSetSQL(ctx, conn, leases, now)
	if err != nil {
		return nil, err
	}
	if len(leaseSet) == 0 {
		if err := commitSQL(ctx, conn); err != nil {
			return nil, err
		}
		committed = true
		return nil, nil
	}

	placeholders, args := placeholdersForLeaseSet(leaseSet)
	args = append(args, formatTime(now), formatTime(now), limit)
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`select e.machine_id, e.version
		   from machine_entries e
		   join machines m on m.id = e.machine_id
		   join shard_leases l on l.shard_id = m.shard_id
		  where m.shard_id in (%s)
		    and m.terminal_at is null
		    and e.version = m.version
		    and l.lease_until > ?
		    and (e.status = 'pending'
		      or (e.status = 'failed' and (e.retry_at is null or e.retry_at <= ?))
		      or (e.status = 'processing' and (coalesce(e.owner, '') != l.owner or e.owner_epoch != l.epoch)))
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

	claimedKeys := make([]EntryKey, 0, minInt(limit, len(keys)))
	for _, key := range keys {
		if len(claimedKeys) >= limit {
			break
		}
		entry, err := readEntrySQL(ctx, conn, key)
		if err != nil {
			return nil, err
		}
		leaseToken := leaseSet[entry.ShardID]
		if !claimableByShardLease(entry.Status, entry.Owner, entry.OwnerEpoch, entry.RetryAt, leaseToken, now) {
			continue
		}
		res, err := conn.ExecContext(ctx,
			`update machine_entries
			    set status = 'processing',
			        owner = ?,
			        owner_epoch = ?,
			        lease_until = null,
			        retry_at = null,
			        attempts = attempts + 1,
			        started_at = ?
			  where machine_id = ? and version = ?`,
			leaseToken.Owner, leaseToken.Epoch, formatTime(now), key.MachineID, key.Version,
		)
		if err != nil {
			return nil, err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return nil, ErrEntryNotFound
		}
		claimedKeys = append(claimedKeys, key)
	}

	claimed := make([]Entry, 0, len(claimedKeys))
	for _, key := range claimedKeys {
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
		if err := completeSignalSQL(ctx, conn, cmd.CompleteSignal.ID, cmd.CompleteSignal.Owner, cmd.CompleteSignal.OwnerEpoch, cmd.CompleteSignal.Attempt); err != nil {
			return nil, err
		}
	}
	if cmd.CompleteEntry != nil {
		if err := completeEntrySQL(ctx, conn, cmd.CompleteEntry.Key, cmd.CompleteEntry.Owner, cmd.CompleteEntry.OwnerEpoch, cmd.CompleteEntry.Attempt); err != nil {
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

func (p *SQLiteProvider) FailEntry(ctx context.Context, key EntryKey, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	return failEntrySQL(ctx, p.db, key, owner, ownerEpoch, attempt, failure)
}

func (p *SQLiteProvider) FailSignal(ctx context.Context, id string, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	return failSignalSQL(ctx, p.db, id, owner, ownerEpoch, attempt, failure)
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

func addColumnIfMissing(ctx context.Context, db *sql.DB, table string, column string, definition string) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`pragma table_info(%s)`, table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`alter table %s add column %s %s`, table, column, definition))
	return err
}

func readShardLeaseSQL(ctx context.Context, q sqlRunner, shard ShardID) (*ShardLease, error) {
	row := q.QueryRowContext(ctx,
		`select shard_id, owner, epoch, lease_until, updated_at
		   from shard_leases
		  where shard_id = ?`, shard)

	var lease ShardLease
	var rawShard int
	var leaseUntilRaw, updatedRaw string
	if err := row.Scan(&rawShard, &lease.Owner, &lease.Epoch, &leaseUntilRaw, &updatedRaw); err != nil {
		return nil, err
	}
	leaseUntil, err := parseRequiredTime(leaseUntilRaw)
	if err != nil {
		return nil, err
	}
	updatedAt, err := parseRequiredTime(updatedRaw)
	if err != nil {
		return nil, err
	}
	lease.ShardID = ShardID(rawShard)
	lease.LeaseUntil = leaseUntil
	lease.UpdatedAt = updatedAt
	return &lease, nil
}

func currentLeaseSetSQL(ctx context.Context, q sqlRunner, leases []ShardLease, now time.Time) (map[ShardID]ShardLease, error) {
	leaseSet := make(map[ShardID]ShardLease, len(leases))
	for _, leaseToken := range leases {
		current, err := readShardLeaseSQL(ctx, q, leaseToken.ShardID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if current.Owner == leaseToken.Owner && current.Epoch == leaseToken.Epoch && current.LeaseUntil.After(now) {
			leaseSet[current.ShardID] = *current
		}
	}
	return leaseSet, nil
}

func shardLeaseOwnedSQL(ctx context.Context, q sqlRunner, shard ShardID, owner string, epoch int64, now time.Time) (bool, error) {
	var count int
	err := q.QueryRowContext(ctx,
		`select count(*)
		   from shard_leases
		  where shard_id = ?
		    and owner = ?
		    and epoch = ?
		    and lease_until > ?`,
		shard, owner, epoch, formatTime(now),
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count == 1, nil
}

func placeholdersForLeaseSet(leaseSet map[ShardID]ShardLease) ([]string, []any) {
	placeholders := make([]string, 0, len(leaseSet))
	args := make([]any, 0, len(leaseSet))
	for shard := range leaseSet {
		placeholders = append(placeholders, "?")
		args = append(args, shard)
	}
	return placeholders, args
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
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
		        e.status, coalesce(e.owner, ''), e.owner_epoch, e.lease_until, e.retry_at, e.attempts,
		        e.created_at, e.started_at, e.completed_at, coalesce(e.last_error, '')
		   from machine_entries e
		   join machines m on m.id = e.machine_id
		  where e.machine_id = ? and e.version = ?`, key.MachineID, key.Version)

	var entry Entry
	var argsJSON string
	var source, dest, trigger string
	var shard int
	var leaseRaw, retryRaw, startedRaw, completedRaw sql.NullString
	var createdRaw string
	err := row.Scan(
		&entry.Key.MachineID, &entry.Key.Version, &shard,
		&source, &dest, &trigger, &argsJSON,
		&entry.Status, &entry.Owner, &entry.OwnerEpoch, &leaseRaw, &retryRaw, &entry.Attempts,
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
	retryAt, err := parseOptionalTime(retryRaw)
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
	entry.RetryAt = retryAt
	entry.StartedAt = startedAt
	entry.CompletedAt = completedAt
	return &entry, nil
}

func readSignalSQL(ctx context.Context, q sqlRunner, id string) (*SignalRecord, error) {
	row := q.QueryRowContext(ctx,
		`select id, machine_id, target_shard_id, trigger, args_json,
		        status, coalesce(owner, ''), owner_epoch, lease_until, retry_at, attempts,
		        created_at, started_at, completed_at, coalesce(last_error, '')
		   from machine_signals
		  where id = ?`, id)

	var signal SignalRecord
	var argsJSON string
	var trigger string
	var shard int
	var leaseRaw, retryRaw, startedRaw, completedRaw sql.NullString
	var createdRaw string
	err := row.Scan(
		&signal.ID, &signal.MachineID, &shard, &trigger, &argsJSON,
		&signal.Status, &signal.Owner, &signal.OwnerEpoch, &leaseRaw, &retryRaw, &signal.Attempts,
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
	retryAt, err := parseOptionalTime(retryRaw)
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
	signal.RetryAt = retryAt
	signal.StartedAt = startedAt
	signal.CompletedAt = completedAt
	return &signal, nil
}

func completeEntrySQL(ctx context.Context, q sqlRunner, key EntryKey, owner string, ownerEpoch int64, attempt int) error {
	now := nowUTC()
	res, err := q.ExecContext(ctx,
		`update machine_entries
		    set status = 'done',
		        lease_until = null,
		        retry_at = null,
		        completed_at = ?
		  where machine_id = ?
		    and version = ?
		    and status = 'processing'
		    and owner = ?
		    and owner_epoch = ?
		    and attempts = ?
		    and exists (
		    	select 1
		    	  from machines m
		    	  join shard_leases l on l.shard_id = m.shard_id
		    	 where m.id = machine_entries.machine_id
		    	   and l.owner = ?
		    	   and l.epoch = ?
		    	   and l.lease_until > ?
		    )`,
		formatTime(now), key.MachineID, key.Version, owner, ownerEpoch, attempt,
		owner, ownerEpoch, formatTime(now),
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveEntryCompleteMiss(ctx, q, key, owner, ownerEpoch, attempt, true)
}

func completeSignalSQL(ctx context.Context, q sqlRunner, id string, owner string, ownerEpoch int64, attempt int) error {
	now := nowUTC()
	res, err := q.ExecContext(ctx,
		`update machine_signals
		    set status = 'done',
		        lease_until = null,
		        retry_at = null,
		        completed_at = ?
		  where id = ?
		    and status = 'processing'
		    and owner = ?
		    and owner_epoch = ?
		    and attempts = ?
		    and exists (
		    	select 1
		    	  from shard_leases l
		    	 where l.shard_id = machine_signals.target_shard_id
		    	   and l.owner = ?
		    	   and l.epoch = ?
		    	   and l.lease_until > ?
		    )`,
		formatTime(now), id, owner, ownerEpoch, attempt, owner, ownerEpoch, formatTime(now),
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveSignalCompleteMiss(ctx, q, id, owner, ownerEpoch, attempt, true)
}

func failEntrySQL(ctx context.Context, q sqlRunner, key EntryKey, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	message := ""
	if failure.Cause != nil {
		message = failure.Cause.Error()
	}
	status := EntryFailed
	retryAt := formatOptionalTime(failure.RetryAt)
	if failure.DeadLetter {
		status = EntryDeadLettered
		retryAt = sql.NullString{}
	}
	res, err := q.ExecContext(ctx,
		`update machine_entries
		    set status = ?,
		        owner = null,
		        owner_epoch = 0,
		        lease_until = null,
		        retry_at = ?,
		        last_error = ?
		  where machine_id = ?
		    and version = ?
		    and status = 'processing'
		    and owner = ?
		    and owner_epoch = ?
		    and attempts = ?
		    and exists (
		    	select 1
		    	  from machines m
		    	  join shard_leases l on l.shard_id = m.shard_id
		    	 where m.id = machine_entries.machine_id
		    	   and l.owner = ?
		    	   and l.epoch = ?
		    	   and l.lease_until > ?
		    )`,
		status, retryAt, message, key.MachineID, key.Version, owner, ownerEpoch, attempt,
		owner, ownerEpoch, formatTime(nowUTC()),
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveEntryCompleteMiss(ctx, q, key, owner, ownerEpoch, attempt, false)
}

func failSignalSQL(ctx context.Context, q sqlRunner, id string, owner string, ownerEpoch int64, attempt int, failure Failure) error {
	message := ""
	if failure.Cause != nil {
		message = failure.Cause.Error()
	}
	status := EntryFailed
	retryAt := formatOptionalTime(failure.RetryAt)
	if failure.DeadLetter {
		status = EntryDeadLettered
		retryAt = sql.NullString{}
	}
	res, err := q.ExecContext(ctx,
		`update machine_signals
		    set status = ?,
		        owner = null,
		        owner_epoch = 0,
		        lease_until = null,
		        retry_at = ?,
		        last_error = ?
		  where id = ?
		    and status = 'processing'
		    and owner = ?
		    and owner_epoch = ?
		    and attempts = ?
		    and exists (
		    	select 1
		    	  from shard_leases l
		    	 where l.shard_id = machine_signals.target_shard_id
		    	   and l.owner = ?
		    	   and l.epoch = ?
		    	   and l.lease_until > ?
		    )`,
		status, retryAt, message, id, owner, ownerEpoch, attempt, owner, ownerEpoch, formatTime(nowUTC()),
	)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	return resolveSignalCompleteMiss(ctx, q, id, owner, ownerEpoch, attempt, false)
}

func resolveEntryCompleteMiss(ctx context.Context, q sqlRunner, key EntryKey, requestedOwner string, requestedOwnerEpoch int64, requestedAttempt int, allowDone bool) error {
	var status EntryStatus
	var owner string
	var ownerEpoch int64
	var attempt int
	var shard int
	err := q.QueryRowContext(ctx,
		`select e.status, coalesce(e.owner, ''), e.owner_epoch, e.attempts, m.shard_id
		   from machine_entries e
		   join machines m on m.id = e.machine_id
		  where e.machine_id = ? and e.version = ?`,
		key.MachineID, key.Version,
	).Scan(&status, &owner, &ownerEpoch, &attempt, &shard)
	if err == sql.ErrNoRows {
		return ErrEntryNotFound
	}
	if err != nil {
		return err
	}
	if status == EntryDone && allowDone && owner == requestedOwner && ownerEpoch == requestedOwnerEpoch && attempt == requestedAttempt {
		return nil
	}
	if status == EntryDeadLettered {
		return ErrWorkDeadLettered
	}
	if status == EntryProcessing && owner == requestedOwner && ownerEpoch == requestedOwnerEpoch && attempt == requestedAttempt {
		owned, err := shardLeaseOwnedSQL(ctx, q, ShardID(shard), requestedOwner, requestedOwnerEpoch, nowUTC())
		if err != nil {
			return err
		}
		if !owned {
			return ErrShardLeaseLost
		}
	}
	return ErrEntryNotOwned
}

func resolveSignalCompleteMiss(ctx context.Context, q sqlRunner, id string, requestedOwner string, requestedOwnerEpoch int64, requestedAttempt int, allowDone bool) error {
	var status EntryStatus
	var owner string
	var ownerEpoch int64
	var attempt int
	var shard int
	err := q.QueryRowContext(ctx,
		`select status, coalesce(owner, ''), owner_epoch, attempts, target_shard_id
		   from machine_signals
		  where id = ?`, id,
	).Scan(&status, &owner, &ownerEpoch, &attempt, &shard)
	if err == sql.ErrNoRows {
		return ErrSignalNotFound
	}
	if err != nil {
		return err
	}
	if status == EntryDone && allowDone && owner == requestedOwner && ownerEpoch == requestedOwnerEpoch && attempt == requestedAttempt {
		return nil
	}
	if status == EntryDeadLettered {
		return ErrWorkDeadLettered
	}
	if status == EntryProcessing && owner == requestedOwner && ownerEpoch == requestedOwnerEpoch && attempt == requestedAttempt {
		owned, err := shardLeaseOwnedSQL(ctx, q, ShardID(shard), requestedOwner, requestedOwnerEpoch, nowUTC())
		if err != nil {
			return err
		}
		if !owned {
			return ErrShardLeaseLost
		}
	}
	return ErrSignalNotOwned
}
