package ctrd

import (
	"log"
	"os"

	"github.com/hashicorp/memberlist"
	uuid "github.com/satori/go.uuid"
)

type Config memberlist.Config

type Node struct {
	Name string
	Addr string
	Port uint16
}

// The counter daemon.
type CTRD struct {
	counter    *multiEventCounter
	list       *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	logger     *log.Logger
}

func DefaultConfig() *Config {
	return (*Config)(memberlist.DefaultLANConfig())
}

// NewCTRD creates a new CTRD with the given configuration.
func NewCTRD(conf *Config, logger *log.Logger) (*CTRD, error) {
	counter := newMultiEventCounter()

	broadcasts := &memberlist.TransmitLimitedQueue{
		RetransmitMult: conf.RetransmitMult,
	}
	conf.Delegate = &delegate{
		counter:    counter,
		broadcasts: broadcasts,
		logger:     logger,
	}

	hostname, _ := os.Hostname()
	conf.Name = hostname + "-" + uuid.NewV4().String()

	list, err := memberlist.Create((*memberlist.Config)(conf))
	if err != nil {
		return nil, err
	}

	broadcasts.NumNodes = func() int {
		return list.NumMembers()
	}

	return &CTRD{
		counter:    counter,
		list:       list,
		broadcasts: broadcasts,
		logger:     logger,
	}, nil
}

func (c *CTRD) Join(existing []string) (int, error) {
	return c.list.Join(existing)
}

func (c *CTRD) LocalNode() *Node {
	n := c.list.LocalNode()
	return &Node{Name: n.Name, Addr: n.Addr.String(), Port: n.Port}
}

func (c *CTRD) Incr(event string, times int64) error {
	return c.counter.Incr(event, times)
}

func (c *CTRD) Count(event string) int64 {
	return c.counter.Count(event)
}

func (c *CTRD) Counts() map[string]int64 {
	counts := make(map[string]int64)
	for event, ctr := range c.counter.Items() {
		counts[event] = ctr.Count()
	}
	return counts
}

func (c *CTRD) Nodes() []*Node {
	ns := c.list.Members()
	nodes := make([]*Node, len(ns))
	for i, n := range ns {
		nodes[i] = &Node{Name: n.Name, Addr: n.Addr.String(), Port: n.Port}
	}
	return nodes
}

// BroadcastState broadcasts the local counter state of
// the given event to a few random cluster members.
func (c *CTRD) BroadcastState(event string) {
	c.logger.Println(" === Broadcasting Local State === ")

	counterB, err := c.counter.Counter(event).Marshal()
	if err != nil {
		panic("Failed to marshal counter state in BroadcastState()")
	}

	m := &message{Type: "merge", Event: event, Body: counterB}
	b, err := m.Marshal()
	if err != nil {
		panic("Failed to marshal broadcast message in BroadcastState()")
	}

	c.broadcasts.QueueBroadcast(&broadcast{msg: b})
}
