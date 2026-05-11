package durablestateless

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

const storageTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

func encodeSymbol(kind string, value any) (string, error) {
	if value == nil {
		return "", fmt.Errorf("durablestateless: %s must be a string", kind)
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.String {
		return "", fmt.Errorf("durablestateless: %s must be a string or string alias, got %T", kind, value)
	}
	return rv.String(), nil
}

func encodeArgs(args []any) (string, error) {
	if args == nil {
		args = []any{}
	}
	raw, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("durablestateless: encode args: %w", err)
	}
	return string(raw), nil
}

func decodeArgs(raw string) ([]any, error) {
	if raw == "" {
		return []any{}, nil
	}
	var args []any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, fmt.Errorf("durablestateless: decode args: %w", err)
	}
	if args == nil {
		args = []any{}
	}
	return args, nil
}

func cloneArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	raw, err := json.Marshal(args)
	if err == nil {
		var out []any
		if err := json.Unmarshal(raw, &out); err == nil {
			return out
		}
	}
	out := make([]any, len(args))
	copy(out, args)
	return out
}

func cloneSignals(signals []Signal) []Signal {
	if len(signals) == 0 {
		return nil
	}
	out := make([]Signal, len(signals))
	for i, signal := range signals {
		out[i] = signal
		out[i].Args = cloneArgs(signal.Args)
	}
	return out
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	copied := *t
	return &copied
}

func nowUTC() time.Time {
	return time.Now().UTC().Round(0)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(storageTimeLayout)
}

func formatOptionalTime(t *time.Time) sql.NullString {
	if t == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: formatTime(*t), Valid: true}
}

func parseOptionalTime(raw sql.NullString) (*time.Time, error) {
	if !raw.Valid || raw.String == "" {
		return nil, nil
	}
	parsed, err := parseRequiredTime(raw.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseRequiredTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(storageTimeLayout, raw)
	if err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}
