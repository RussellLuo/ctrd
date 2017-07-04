package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/RussellLuo/ctrd"
	"github.com/RussellLuo/ctrd/server"
	"github.com/RussellLuo/ctrd/server/http"
	"github.com/RussellLuo/ctrd/server/pb"
	"google.golang.org/grpc"
)

func getLogger(out io.Writer, logger *log.Logger) (*log.Logger, error) {
	if out != nil && logger != nil {
		return nil, fmt.Errorf("Cannot specify both LogOutput and Logger. Please choose a single log configuration setting.")
	}

	if out == nil {
		out = os.Stderr
	}

	if logger == nil {
		logger = log.New(out, "", log.LstdFlags)
	}

	return logger, nil
}

func main() {
	join := flag.String("join", "", "comma separated existing nodes to join")
	grpcAddr := flag.String("grpc-addr", ":50051", "gRPC address")
	httpAddr := flag.String("http-addr", ":50052", "HTTP address")
	swimPort := flag.Int("swim-port", 0, "SWIM protocol port (0 = auto select)")
	flag.Parse()

	conf := ctrd.DefaultConfig()
	conf.BindPort = *swimPort
	conf.ProbeInterval = 1 * time.Second
	conf.PushPullInterval = 10 * time.Second
	conf.GossipNodes = 3
	conf.GossipInterval = 200 * time.Millisecond

	logger, err := getLogger(conf.LogOutput, conf.Logger)
	if err != nil {
		panic(err)
	}

	var existing []string
	if len(*join) > 0 {
		existing = strings.Split(*join, ",")
	}

	srv, err := server.NewCTRDServer(conf, logger, existing)
	if err != nil {
		panic(err)
	}

	h := http.NewServer(srv)
	go func() {
		logger.Printf("starting the HTTP server (listening on %s)\n", *httpAddr)
		if err := h.Serve(*httpAddr); err != nil {
			logger.Fatalf("failed to start the HTTP server: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", *grpcAddr)
	logger.Printf("starting the gRPC server (listening on %s)\n", *grpcAddr)
	if err != nil {
		logger.Fatalf("failed to start the gRPC server: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterCTRDServer(grpcServer, srv)
	grpcServer.Serve(listener)
}
