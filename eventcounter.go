package ctrd

import (
	"bytes"
	"encoding/gob"

	"github.com/RussellLuo/ctrd/crdt"
	"github.com/RussellLuo/ctrd/syncmap"
	"github.com/satori/go.uuid"
)

// The counter for multiple events, which manages a GCounter for each event.
type multiEventCounter struct {
	counters *syncmap.Map
}

func newMultiEventCounter() *multiEventCounter {
	return &multiEventCounter{counters: new(syncmap.Map)}
}

func (m *multiEventCounter) Counter(event string) *crdt.GCounter {
	counter, ok := m.counters.Load(event)
	if !ok {
		newCounter := crdt.NewGCounter(uuid.NewV4().String())
		counter, _ = m.counters.LoadOrStore(event, newCounter)
	}
	return counter.(*crdt.GCounter)
}

func (m *multiEventCounter) Incr(event string, amount int64) error {
	return m.Counter(event).Incr(amount)
}

func (m *multiEventCounter) Merge(event string, c *crdt.GCounter) {
	m.Counter(event).Merge(c)
}

func (m *multiEventCounter) Count(event string) (total int64) {
	return m.Counter(event).Count()
}

func (m *multiEventCounter) Items() map[string]*crdt.GCounter {
	items := make(map[string]*crdt.GCounter)
	m.counters.Range(func(event, counter interface{}) bool {
		items[event.(string)] = counter.(*crdt.GCounter)
		return true
	})
	return items
}

// codecMultiEventCounter is a helper for encoding/decoding multiEventCounter.
type codecMultiEventCounter struct {
	C map[string][]byte
}

// UnmarshalMultiEventCounter creates a multiEventCounter from the serialized bytes.
func UnmarshalMultiEventCounter(in []byte) (*multiEventCounter, error) {
	b := bytes.NewBuffer(in)
	dec := gob.NewDecoder(b)

	var v codecMultiEventCounter
	err := dec.Decode(&v)
	if err != nil {
		return nil, err
	}

	counters := new(syncmap.Map)
	for event, ctrBytes := range v.C {
		ctr, err := crdt.UnmarshalGCounter(ctrBytes)
		if err != nil {
			return nil, err
		}
		counters.Store(event, ctr)
	}

	return &multiEventCounter{counters: counters}, nil
}

// Marshal serializes the given multiEventCounter into bytes.
func (m *multiEventCounter) Marshal() ([]byte, error) {
	c := make(map[string][]byte)
	for event, ctr := range m.Items() {
		ctrBytes, err := ctr.Marshal()
		if err != nil {
			return nil, err
		}
		c[event] = ctrBytes
	}

	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(&codecMultiEventCounter{C: c})

	return b.Bytes(), err
}
