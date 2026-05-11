package durablestateless

import (
	"hash/fnv"
)

// Sharder maps a machine ID to the shard that owns that machine.
type Sharder interface {
	ShardForMachine(machineID string) ShardID
}

// HashSharder assigns machines by FNV-1a hash modulo the configured shard
// count.
type HashSharder struct {
	numShards ShardID
}

// NewHashSharder creates a hash sharder with numShards shards.
func NewHashSharder(numShards int) (HashSharder, error) {
	if numShards <= 0 {
		return HashSharder{}, ErrInvalidShard
	}
	return HashSharder{numShards: ShardID(numShards)}, nil
}

// MustHashSharder returns a HashSharder or panics for an invalid shard count.
func MustHashSharder(numShards int) HashSharder {
	sharder, err := NewHashSharder(numShards)
	if err != nil {
		panic(err)
	}
	return sharder
}

// ShardForMachine returns hash(machineID) modulo the configured shard count.
func (s HashSharder) ShardForMachine(machineID string) ShardID {
	if s.numShards <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(machineID))
	return ShardID(h.Sum32() % uint32(s.numShards))
}

func validateShardID(shard ShardID) error {
	if shard < 0 {
		return ErrInvalidShard
	}
	return nil
}

func validateShardIDs(shards []ShardID) error {
	seen := make(map[ShardID]struct{}, len(shards))
	for _, shard := range shards {
		if err := validateShardID(shard); err != nil {
			return err
		}
		if _, ok := seen[shard]; ok {
			return ErrInvalidShard
		}
		seen[shard] = struct{}{}
	}
	return nil
}
