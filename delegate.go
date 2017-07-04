package ctrd

import (
	"log"

	"github.com/RussellLuo/ctrd/crdt"
	"github.com/hashicorp/memberlist"
)

type delegate struct {
	counter    *multiEventCounter
	broadcasts *memberlist.TransmitLimitedQueue
	logger     *log.Logger
}

func (d *delegate) NodeMeta(limit int) []byte {
	return nil
}

// NotifyMsg is called when a user-data message is received.
func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	message, err := UnmarshalMessage(b)
	if err != nil {
		return
	}

	switch message.Type {
	case "merge":
		d.logger.Println(" === Received Broadcast of Remote State === ")

		remoteCtr, _ := crdt.UnmarshalGCounter(message.Body)
		d.counter.Merge(message.Event, remoteCtr)
	default:
		panic("unsupported update action")
	}

}

// GetBroadcasts is called when user data messages can be broadcast.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState is used for a TCP Push/Pull.
// The 'join' boolean indicates this is for a join instead of a push/pull.
func (d *delegate) LocalState(join bool) []byte {
	d.logger.Println(" === Sharing Remote State for push/pull sync === ")

	b, err := d.counter.Marshal()
	if err != nil {
		panic(err)
	}

	return b
}

// MergeRemoteState is invoked after a TCP Push/Pull.
// The 'join' boolean indicates this is for a join instead of a push/pull.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}

	d.logger.Println(" === Merging Remote State for push/pull sync === ")

	remoteMECtr, err := UnmarshalMultiEventCounter(buf)
	if err != nil {
		panic(err)
	}

	for event, ctr := range remoteMECtr.Items() {
		d.counter.Merge(event, ctr)
	}
}
