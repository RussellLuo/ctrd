package crdt

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/RussellLuo/ctrd/syncmap"
)

// GCounter represent a G-counter in CRDT, which is a
// state-based grow-only counter that only supports increments.
type GCounter struct {
	// identity provides a unique identity to each replica.
	identity string

	// counter maps identity of each replica to their
	// entry values i.e. the counter value they individually
	// have.
	counter *syncmap.Map
}

// NewGCounter returns a *GCounter by pre-assigning a unique
// identity to it.
func NewGCounter(identity string) *GCounter {
	return &GCounter{
		identity: identity,
		counter:  new(syncmap.Map),
	}
}

// Incr increments the GCounter by the value of an arbitrary times.
// Only positive values are accepted.
// If a non-positive value is provided the implementation will panic.
func (g *GCounter) Incr(times int64) error {
	if times < 1 {
		return errors.New("cannot decrement a gcounter")
	}

	g.counter.UpdateOrStore(g.identity, func(old interface{}) interface{} {
		if old == nil {
			return int64(times) // initialized to times
		} else {
			return old.(int64) + times // incremented by times
		}
	})

	return nil
}

// Count returns the total count of this counter across all the
// present replicas.
func (g *GCounter) Count() (total int64) {
	for _, count := range g.Items() {
		total += count
	}
	return total
}

// Merge combines the counter values across multiple replicas.
// The property of idempotency is preserved here across
// multiple merges as when no state is changed across any replicas,
// the result should be exactly the same everytime.
func (g *GCounter) Merge(c *GCounter) {
	c.counter.Range(func(identity, value interface{}) bool {
		if v, ok := g.counter.Load(identity); !ok || v.(int64) < value.(int64) {
			g.counter.Store(identity, value)
		}
		return true
	})
}

func (g *GCounter) Items() map[string]int64 {
	c := make(map[string]int64)
	g.counter.Range(func(identity, count interface{}) bool {
		c[identity.(string)] = count.(int64)
		return true
	})
	return c
}

// codecGCounter is a helper for encoding/decoding GCounter.
type codecGCounter struct {
	I string
	C map[string]int64
}

// UnmarshalGCounter creates a GCounter from the serialized bytes.
func UnmarshalGCounter(in []byte) (*GCounter, error) {
	b := bytes.NewBuffer(in)
	dec := gob.NewDecoder(b)

	var v codecGCounter
	err := dec.Decode(&v)
	if err != nil {
		return nil, err
	}

	counter := new(syncmap.Map)
	for identity, count := range v.C {
		counter.Store(identity, count)
	}

	return &GCounter{identity: v.I, counter: counter}, nil
}

// Marshal serializes the given GCounter into bytes.
func (g *GCounter) Marshal() ([]byte, error) {
	var b bytes.Buffer

	enc := gob.NewEncoder(&b)
	err := enc.Encode(&codecGCounter{
		I: g.identity,
		C: g.Items(),
	})

	return b.Bytes(), err
}
