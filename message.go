package ctrd

import (
	"bytes"
	"encoding/gob"
)

type message struct {
	Type  string // merge
	Event string // which event happens
	Body  []byte // crdt.gcounter
}

// UnmarshalMessage creates a message from the serialized bytes.
func UnmarshalMessage(in []byte) (*message, error) {
	b := bytes.NewBuffer(in)
	dec := gob.NewDecoder(b)

	m := new(message)
	err := dec.Decode(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Marshal serializes the given message into bytes.
func (m *message) Marshal() ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(m)
	return b.Bytes(), err
}
