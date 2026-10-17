module github.com/DmitriiTrifonov/redis-startup-diag/grpcdiag

go 1.24.7

require (
	github.com/DmitriiTrifonov/redis-startup-diag v0.0.0
	github.com/redis/go-redis/v9 v9.17.0
	google.golang.org/grpc v1.70.0
	google.golang.org/protobuf v1.35.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/net v0.32.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241202173237-19429a94021a // indirect
)

// Local development: use the module from this repository.
replace github.com/DmitriiTrifonov/redis-startup-diag => ../
