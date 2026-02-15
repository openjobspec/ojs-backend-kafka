// Package grpc provides a gRPC transport layer for the OJS Kafka backend.
//
// This package implements the OJSService defined in ojs-proto/proto/ojs/v1/service.proto
// by delegating to the core.Backend interface. It serves as the gRPC counterpart
// to the HTTP handlers in the api/ package.
//
// # Prerequisites
//
// Before using this package, generate the proto code:
//
//	cd ../ojs-proto && buf generate
//
// This produces the Go types and gRPC service interface in:
//
//	ojs-proto/generated/go/ojs/v1/
//
// # Usage
//
//	import (
//	    ojsgrpc "github.com/openjobspec/ojs-backend-kafka/internal/grpc"
//	    "google.golang.org/grpc"
//	)
//
//	grpcServer := grpc.NewServer()
//	ojsgrpc.Register(grpcServer, backend)
//	lis, _ := net.Listen("tcp", ":9090")
//	grpcServer.Serve(lis)
package grpc
