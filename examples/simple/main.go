package main

import (
	"context"

	"flag"
	"log"
	"net"

	"github.com/u2takey/mq-gateway/runtime"
	"google.golang.org/grpc"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
)

func main() {
	flag.Parse()

	rpcAddress := "localhost:9008"
	// Establish gateways for incoming HTTP requests.
	mux := runtime.NewServeMux(*uri, *exchangeName, *exchangeType)
	ctx := context.Background()
	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	err := RegisterEchoServiceHandlerFromEndpoint(ctx, mux, rpcAddress, dialOpts)
	if err != nil {
		log.Panic(err)
	}

	go mux.Start()

	runRPCServer(rpcAddress)
}

// RunRPCServer ...
func runRPCServer(rpcAddress string) {
	listen, err := net.Listen("tcp", rpcAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	rpcs := grpc.NewServer()
	RegisterEchoServiceServer(rpcs, NewEchoService())
	err = rpcs.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
