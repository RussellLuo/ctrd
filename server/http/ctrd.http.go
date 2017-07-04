package http

import (
	"io"
	"net/http"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	context "golang.org/x/net/context"
	pb "github.com/RussellLuo/ctrd/server/pb"
)

var (
	marshaler   = &jsonpb.Marshaler{EnumsAsInts: true, EmitDefaults: true}
	unmarshaler = &jsonpb.Unmarshaler{}
)

type Method func(context.Context, proto.Message) (proto.Message, error)

func MakeHandler(method Method, in proto.Message) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if err := unmarshaler.Unmarshal(r.Body, in); err != nil {
			if err != io.EOF {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		out, err := method(nil, in)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := marshaler.Marshal(w, out); err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

type CTRD struct {
	srv pb.CTRDServer
}

func NewCTRD(srv pb.CTRDServer) *CTRD {
	return &CTRD{srv: srv}
}

func (c *CTRD) HandlerMap() map[string]http.HandlerFunc {
	m := make(map[string]http.HandlerFunc)
	m["/ctrd/incr"] = MakeHandler(c.Incr, new(pb.IncrRequest))
	m["/ctrd/get"] = MakeHandler(c.Get, new(pb.GetRequest))
	m["/ctrd/get_all"] = MakeHandler(c.GetAll, new(pb.Empty))
	m["/ctrd/nodes"] = MakeHandler(c.Nodes, new(pb.Empty))
	return m
}

func (c *CTRD) Incr(ctx context.Context, in proto.Message) (proto.Message, error) {
	return c.srv.Incr(ctx, in.(*pb.IncrRequest))
}

func (c *CTRD) Get(ctx context.Context, in proto.Message) (proto.Message, error) {
	return c.srv.Get(ctx, in.(*pb.GetRequest))
}

func (c *CTRD) GetAll(ctx context.Context, in proto.Message) (proto.Message, error) {
	return c.srv.GetAll(ctx, in.(*pb.Empty))
}

func (c *CTRD) Nodes(ctx context.Context, in proto.Message) (proto.Message, error) {
	return c.srv.Nodes(ctx, in.(*pb.Empty))
}

type Server struct {
	mux *http.ServeMux
}

func NewServer(srvCTRD pb.CTRDServer) *Server {
	mux := http.NewServeMux()
	for pattern, handler := range NewCTRD(srvCTRD).HandlerMap() {
		mux.Handle(pattern, handler)
	}
	return &Server{mux: mux}
}

func (s *Server) Serve(addr string) error {
	return http.ListenAndServe(addr, s.mux)
}
