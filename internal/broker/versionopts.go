package broker

import "google.golang.org/grpc"

// versionServerOptions returns the interceptors that reject calls on
// connections Hello already refused. They must be chained with
// grpc.ChainUnaryInterceptor and grpc.ChainStreamInterceptor: grpc.UnaryInterceptor
// and grpc.StreamInterceptor sit at the outermost layer and would let the
// access interceptor run first.
func versionServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(versionUnaryInterceptor),
		grpc.ChainStreamInterceptor(versionStreamInterceptor),
	}
}
