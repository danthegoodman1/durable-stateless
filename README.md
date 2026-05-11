# durable-stateless

`durable-stateless` is a small Go proof of concept for running
[`github.com/qmuntal/stateless`](https://github.com/qmuntal/stateless) with
durable state, retryable entry work, and shard-aware recovery.

Use it when you want the clarity of a finite state machine, but you need the
machine's current state and state-entry work to survive process crashes.

The important idea is simple:

```text
stateless decides whether a transition is legal
the provider atomically commits durable state and outgoing signals
entry handlers run after commit and are retried until marked done
```

The database is the source of truth. `stateless` is rebuilt from the stored
snapshot whenever a transition needs to be evaluated.

Small taste:

```go
provider := durablestateless.NewMemoryProvider()
_ = provider.Migrate(ctx)

sharder := durablestateless.MustHashSharder(1024)
rt := durablestateless.NewRuntime(
	provider,
	OrderMachine{},
	durablestateless.WithSharder(sharder),
)

_ = rt.CreateMachine(ctx, durablestateless.MachineInit{
	ID:    "order-123",
	State: "new",
})

_ = rt.Signal(ctx, durablestateless.NewSignal(
	"request-abc123",
	"order-123",
	"start",
	"customer-42",
))

worker := rt.Worker(durablestateless.WorkerConfig{
	ID:     "worker-a",
	Shards: []durablestateless.ShardID{sharder.ShardForMachine("order-123")},
})

processed, err := worker.Work(ctx, 100)
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
processed, err := worker.Work(ctx, 100)
```

That claims durable signals and entry work for the worker's shards.
Workers recover unfinished entry work before applying queued signals, so a
signal does not get stuck behind a lease for work the same worker could have
completed first.

What the runtime does **not** do is scan every non-terminal machine and ask
whether its current state should be doing something. A non-terminal machine
with no open entry is idle until it receives a trigger/message.

Shard ownership is therefore a worker concern:

```text
worker owns shards [3, 7]
worker calls Work(...)
provider only leases signals and entries for those shards
```

Shard assignment is a runtime concern. By default all machines live on shard
zero; pass `WithSharder(MustHashSharder(n))` to place machines by
`hash(machine_id) % n`. A provider persists the shard chosen by the runtime and
uses it for recovery claims.

Crash behavior:

```text
before transition commit              -> no state change exists
after commit, before handler starts   -> pending entry is recovered
during handler                        -> lease expires; entry is retried
after side effect, before done mark   -> same entry key is retried
handler returns next trigger/signals  -> done mark + outputs commit atomically
```

## Retries And Leases

Failed entries and signals are not retried in a tight loop. The runtime turns a
failure into durable retry metadata:

```go
rt := durablestateless.NewRuntime(
	provider,
	OrderMachine{},
	durablestateless.WithRetryPolicy(durablestateless.RetryPolicy{
		MaxAttempts:    10,
		InitialBackoff: time.Second,
		MaxBackoff:     time.Minute,
		Multiplier:     2,
	}),
)
```

`MaxAttempts` is checked against the claimed attempt number. When it is
exhausted, the row becomes `dead_lettered` and is no longer claimable. A
dead-lettered entry still blocks the machine version it belongs to; that is
intentional, because the state-entry work never completed safely.

Workers also renew entry-handler leases while the handler is running:

```go
rt := durablestateless.NewRuntime(
	provider,
	OrderMachine{},
	durablestateless.WithLeaseDuration(30*time.Second),
	durablestateless.WithLeaseRenewalInterval(10*time.Second),
)
```

If `WithLeaseRenewalInterval` is omitted, the worker derives an interval from
the lease duration. A negative renewal interval disables automatic renewal.
Signal processing is expected to be short; automatic renewal is for entry
handlers.

## Next Vs Signals

Entry handlers can return two kinds of durable outputs:

```go
return durablestateless.HandlerResult{
	Next: durablestateless.Next("paid"),
	Signals: []durablestateless.Signal{
		durablestateless.NewSignal(entry.Key.String()+":notify", "notification-123", "send"),
	},
}, nil
```

`Next` means "continue this same machine now." When the handler succeeds, the
runtime commits the entry's done mark and applies `Next` to the machine that
created the entry in the same provider commit. Use `Next` when the handler's
result decides the current machine's next state, such as `charging -> paid`.

`Signals` means "enqueue durable messages for machines to process later." The
runtime commits the entry's done mark and the signal rows in the same provider
commit, but those signals are claimed and applied later by workers that own the
target machines' shards. Use signals for cross-shard messages, fan-out, and
decoupled follow-up work.

Short version:

```text
Next    -> same machine, same commit, immediate transition
Signals -> any machine, same commit to enqueue, later transition
```

Do not call `rt.Signal` directly from inside a handler when the signal must be
atomic with handler completion. Return it in `HandlerResult.Signals` instead.

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
		// Return outputs instead of calling rt.Signal here. The runtime commits
		// the done mark and these outputs atomically.
		return durablestateless.HandlerResult{
			Next: durablestateless.Next("paid"),
			Signals: []durablestateless.Signal{
				durablestateless.NewSignal(entry.Key.String()+":notify", "notification-123", "send"),
			},
		}, nil
	}, true
}
```

## Running It

Create a provider and runtime:

```go
provider, err := durablestateless.OpenSQLiteProvider("machines.db")
if err != nil {
	return err
}
defer provider.Close()

if err := provider.Migrate(ctx); err != nil {
	return err
}

sharder := durablestateless.MustHashSharder(1024)
rt := durablestateless.NewRuntime(
	provider,
	OrderMachine{},
	durablestateless.WithSharder(sharder),
)
```

Create a machine and send it a durable signal:

```go
err = rt.CreateMachine(ctx, durablestateless.MachineInit{
	ID:    "order-123",
	State: "new",
})

err = rt.Signal(ctx, durablestateless.NewSignal("request-abc123", "order-123", "start"))
```

Run work for the shards this worker owns:

```go
worker := rt.Worker(durablestateless.WorkerConfig{
	ID:     "worker-a",
	Shards: []durablestateless.ShardID{sharder.ShardForMachine("order-123")},
})

for {
	processed, err := worker.Work(ctx, 100)
	if err != nil {
		log.Printf("work: %v", err)
	}
	if processed == 0 {
		time.Sleep(time.Second)
	}
}
```

There is also an in-memory provider for tests:

```go
provider := durablestateless.NewMemoryProvider()
rt := durablestateless.NewRuntime(provider, OrderMachine{})
```

## Provider Contract

Providers expose atomic commands, not interactive transactions:

```go
EnqueueSignal(ctx, signal)
ClaimSignals(ctx, owner, shards, limit, lease)
ClaimEntries(ctx, owner, shards, limit, lease)
Commit(ctx, atomicCommit)
```

`Commit` atomically completes claimed work, advances the current machine
projection, creates entry work, and appends outgoing signals. SQLite uses a
short internal transaction to do this, but that is an implementation detail.
Other providers can use conditional writes, CAS, batch writes, or an append log
plus projection.

Completion and failure use the claim's `(owner, attempt)` pair, not just owner,
so a stale process cannot finish work after the same worker ID has reclaimed an
expired lease.

Failed rows carry a `retry_at` timestamp. Providers should only claim failed
rows after that time, and should never claim `dead_lettered` rows. Lease renewal
must also be guarded by `(owner, attempt)`.

## Current Scope

This is still a PoC.

- states and triggers must be strings or string aliases
- args must be JSON-compatible
- SQLite uses `github.com/mattn/go-sqlite3`, so CGO is required
- shard ownership is enforced by worker claims; public callers send signals
