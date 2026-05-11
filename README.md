# durable-stateless

`durable-stateless` is a small Go proof of concept for running
[`github.com/qmuntal/stateless`](https://github.com/qmuntal/stateless) with
durable state, retryable entry work, and shard-aware recovery.

Use it when you want the clarity of a finite state machine, but you need the
machine's current state and state-entry work to survive process crashes.

The important idea is simple:

```text
stateless decides whether a transition is legal
the provider atomically commits the new durable state
entry handlers run after commit and are retried until marked done
```

The database is the source of truth. `stateless` is rebuilt from the stored
snapshot whenever a transition needs to be evaluated.

Small taste:

```go
provider := durablestateless.NewMemoryProvider()
_ = provider.Migrate(ctx)

runner := durablestateless.NewRunner(provider, OrderMachine{})

_ = runner.CreateMachine(ctx, durablestateless.MachineInit{
	ID:      "order-123",
	ShardID: 7,
	State:   "new",
})

_ = runner.Fire(ctx, "order-123", "start")

// Usually called when a shard worker starts, then in a polling loop.
processed, err := runner.Recover(ctx, "worker-a", []int{7}, 100)
```

## Mental Model

Each machine has one current row:

```text
machine id -> shard id, state, version, args, terminal_at
```

When a transition commits, the provider:

1. checks the expected machine version
2. refuses to advance if the current version has unfinished entry work
3. updates the machine state and version
4. inserts a durable `machine_entries` row when the destination state has work

The entry key is:

```text
machine_id + version
```

That key is the idempotency key for handler side effects.

## Recovery Semantics

Recovery is lazy for idle states, not for unfinished work.

If a transition already committed and created an entry row, a worker should
recover it as soon as it comes online:

```go
processed, err := runner.Recover(ctx, "worker-a", []int{3, 7}, 100)
```

That claims `pending`, `failed`, and expired `processing` entries for the
worker's shards.

What the runtime does **not** do is scan every non-terminal machine and ask
whether its current state should be doing something. A non-terminal machine
with no open entry is idle until it receives a trigger/message.

Shard ownership is therefore a worker concern:

```text
worker owns shards [3, 7]
worker calls Recover(..., []int{3, 7}, ...)
provider only leases entries whose machine rows belong to those shards
```

Shard assignment is explicit today: the caller sets `MachineInit.ShardID` when
creating a machine. A provider persists that value and uses it for recovery
claims; it should not invent a different placement policy behind the runtime's
back.

Crash behavior:

```text
before transition commit              -> no state change exists
after commit, before handler starts   -> pending entry is recovered
during handler                        -> lease expires; entry is retried
after side effect, before done mark   -> same entry key is retried
handler returns next trigger          -> done mark + next transition commit atomically
```

## Defining A Machine

Implement `Definition`:

```go
type Definition interface {
	Configure(rules *Rules)
	IsTerminal(state stateless.State) bool
	EntryHandler(state stateless.State) (EntryHandler, bool)
}
```

`Configure` defines transition legality only. The `Rules` wrapper deliberately
does not expose `OnEntry`/`OnExit`, because durable work belongs in
`EntryHandler`.

Example:

```go
type OrderMachine struct{}

func (OrderMachine) Configure(r *durablestateless.Rules) {
	r.Configure("new").Permit("start", "charging")
	r.Configure("charging").Permit("paid", "done")
}

func (OrderMachine) IsTerminal(state stateless.State) bool {
	return state == "done"
}

func (OrderMachine) EntryHandler(state stateless.State) (durablestateless.EntryHandler, bool) {
	if state != "charging" {
		return nil, false
	}
	return func(ctx context.Context, entry durablestateless.Entry) (durablestateless.HandlerResult, error) {
		// Use entry.Key as the idempotency key for external side effects.
		if err := chargeCustomer(ctx, entry.Key.String()); err != nil {
			return durablestateless.NoNext(), err
		}
		return durablestateless.FireNext("paid"), nil
	}, true
}
```

## Running It

Create a provider and runner:

```go
provider, err := durablestateless.OpenSQLiteProvider("machines.db")
if err != nil {
	return err
}
defer provider.Close()

if err := provider.Migrate(ctx); err != nil {
	return err
}

runner := durablestateless.NewRunner(provider, OrderMachine{})
```

Create and fire a machine:

```go
err = runner.CreateMachine(ctx, durablestateless.MachineInit{
	ID:      "order-123",
	ShardID: 7,
	State:   "new",
})

err = runner.Fire(ctx, "order-123", "start")
```

Run recovery for the shards this worker owns:

```go
for {
	processed, err := runner.Recover(ctx, "worker-a", []int{7}, 100)
	if err != nil {
		log.Printf("recover: %v", err)
	}
	if processed == 0 {
		time.Sleep(time.Second)
	}
}
```

There is also an in-memory provider for tests:

```go
provider := durablestateless.NewMemoryProvider()
runner := durablestateless.NewRunner(provider, OrderMachine{})
```

## Provider Contract

Providers expose atomic commands, not interactive transactions:

```go
CommitTransition(ctx, cmd)
CompleteEntryAndCommitTransition(ctx, cmd)
```

Those commands must atomically update the current machine projection and entry
work. SQLite uses a short internal transaction to do this, but that is an
implementation detail. Other providers can use conditional writes, CAS, batch
writes, or an append log plus projection.

## Current Scope

This is still a PoC.

- states and triggers must be strings or string aliases
- args must be JSON-compatible
- SQLite uses `github.com/mattn/go-sqlite3`, so CGO is required
- shard ownership is enforced by recovery claims, not by `Fire`
- cross-shard messaging should be modeled as a durable signal/outbox layer
  above this package
# durable-stateless
