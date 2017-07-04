package server

import (
	"log"

	"github.com/RussellLuo/ctrd"
	pb "github.com/RussellLuo/ctrd/server/pb"
	context "golang.org/x/net/context"
)

type CTRDServer struct {
	ctrd *ctrd.CTRD
}

func NewCTRDServer(conf *ctrd.Config, logger *log.Logger, existing []string) (*CTRDServer, error) {
	c, err := ctrd.NewCTRD(conf, logger)
	if err != nil {
		return nil, err
	}

	if len(existing) > 0 {
		if _, err := c.Join(existing); err != nil {
			panic(err)
		}
	}

	node := c.LocalNode()
	logger.Printf("Local node state %s:%d\n", node.Addr, node.Port)

	return &CTRDServer{ctrd: c}, nil
}

func (s *CTRDServer) Incr(ctx context.Context, in *pb.IncrRequest) (*pb.Empty, error) {
	err := s.ctrd.Incr(in.Event, in.Times)
	if err != nil {
		return nil, err
	}
	s.ctrd.BroadcastState(in.Event)
	return &pb.Empty{}, nil
}

func (s *CTRDServer) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply, error) {
	count := s.ctrd.Count(in.Event)
	return &pb.GetReply{Count: count}, nil
}

func (s *CTRDServer) GetAll(ctx context.Context, in *pb.Empty) (*pb.GetAllReply, error) {
	counts := s.ctrd.Counts()
	return &pb.GetAllReply{Counts: counts}, nil
}

func (s *CTRDServer) Nodes(ctx context.Context, in *pb.Empty) (*pb.NodesReply, error) {
	nodes := s.ctrd.Nodes()

	replyNodes := make([]*pb.Node, len(nodes))
	for i, n := range nodes {
		replyNodes[i] = &pb.Node{Name: n.Name, Addr: n.Addr, Port: int32(n.Port)}
	}

	return &pb.NodesReply{Nodes: replyNodes}, nil
}
