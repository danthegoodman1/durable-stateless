package durablestateless

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/qmuntal/stateless"
)

type benchmarkProviderCase struct {
	name string
	new  func(*testing.B) Provider
}

func benchmarkProviderCases() []benchmarkProviderCase {
	return []benchmarkProviderCase{
		{
			name: "memory",
			new: func(b *testing.B) Provider {
				b.Helper()
				return NewMemoryProvider()
			},
		},
		{
			name: "sqlite",
			new: func(b *testing.B) Provider {
				b.Helper()
				provider, err := OpenSQLiteProvider(filepath.Join(b.TempDir(), "machines.db"))
				if err != nil {
					b.Fatalf("open sqlite provider: %v", err)
				}
				b.Cleanup(func() {
					if err := provider.Close(); err != nil {
						b.Fatalf("close sqlite provider: %v", err)
					}
				})
				return provider
			},
		},
	}
}

func BenchmarkProviderSignalEnqueue(b *testing.B) {
	for _, tc := range benchmarkProviderCases() {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			rt := newBenchmarkRuntime(b, tc.new(b))
			createBenchmarkMachine(b, ctx, rt, "m1")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := rt.Signal(ctx, NewSignal(fmt.Sprintf("s-%d", i), "m1", triggerStart)); err != nil {
					b.Fatalf("signal: %v", err)
				}
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "signals/s")
		})
	}
}

func BenchmarkProviderSignalToTransition(b *testing.B) {
	for _, batch := range []int{1, 100} {
		b.Run(fmt.Sprintf("batch_%d", batch), func(b *testing.B) {
			for _, tc := range benchmarkProviderCases() {
				b.Run(tc.name, func(b *testing.B) {
					ctx := context.Background()
					rt := newBenchmarkRuntime(b, tc.new(b))
					createBenchmarkMachine(b, ctx, rt, "m1")
					worker := rt.Worker(WorkerConfig{ID: "worker", Shards: []ShardID{0}})

					b.ReportAllocs()
					b.ResetTimer()
					for processed := 0; processed < b.N; {
						size := batch
						if remaining := b.N - processed; remaining < size {
							size = remaining
						}
						for i := 0; i < size; i++ {
							id := processed + i
							if err := rt.Signal(ctx, NewSignal(fmt.Sprintf("s-%d", id), "m1", triggerStart)); err != nil {
								b.Fatalf("signal: %v", err)
							}
						}
						n, err := worker.Work(ctx, size)
						if err != nil {
							b.Fatalf("work: %v", err)
						}
						if n != size {
							b.Fatalf("expected to process %d signals, got %d", size, n)
						}
						processed += size
					}
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "signal_transitions/s")
				})
			}
		})
	}
}

func newBenchmarkRuntime(b *testing.B, provider Provider) *Runtime {
	b.Helper()
	if err := provider.Migrate(context.Background()); err != nil {
		b.Fatalf("migrate provider: %v", err)
	}
	return NewRuntime(provider, benchmarkDefinition{}, WithLeaseDuration(time.Minute))
}

func createBenchmarkMachine(b *testing.B, ctx context.Context, rt *Runtime, id string) {
	b.Helper()
	if err := rt.CreateMachineInShard(ctx, 0, MachineInit{ID: id, State: stateIdle}); err != nil {
		b.Fatalf("create machine: %v", err)
	}
}

type benchmarkDefinition struct{}

func (benchmarkDefinition) Configure(rules *Rules) {
	rules.Configure(stateIdle).PermitReentry(triggerStart)
}

func (benchmarkDefinition) IsTerminal(stateless.State) bool {
	return false
}

func (benchmarkDefinition) EntryHandler(stateless.State) (EntryHandler, bool) {
	return nil, false
}
