module fluxer-observer

go 1.24

require (
	github.com/nats-io/nats.go v1.37.0
	github.com/jackc/pgx/v5 v5.5.5
	github.com/robfig/cron/v3 v3.0.1
	// Pinned: go-redis v9.23+ requires Go 1.26, the build image is 1.24.
	github.com/redis/go-redis/v9 v9.7.0
	golang.org/x/net v0.33.0
)
