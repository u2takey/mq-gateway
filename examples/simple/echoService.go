package main

import (
	"context"
	"strings"
)

// EchoService ...
type EchoService struct {
}

// NewEchoService ...
func NewEchoService() *EchoService {
	return &EchoService{}
}

// DescribeEvent ...
func (s *EchoService) Echo(ctx context.Context, req *EchoRequest) (res *EchoResponse, err error) {

	return &EchoResponse{
		Hello: req.GetHello() + " " + strings.Join(req.GetNames(), ","),
	}, nil
}
