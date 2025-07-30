package db

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

/*
===========================================================
copy from gogstash: inputredis.go
*/
type Redis7 struct {
	Host     string `json:"Host"`     // host:port, default: "localhost:6379"
	DB       int    `json:"DB"`       // redis db, default: 0
	Password string `json:"Password"` // default: ""
	Key      string `json:"Key"`      // where to get data
	PoolSize int    `json:"PoolSize"` // maximum number of socket connections, default: 10

	client *redis.Client
}

func (r *Redis7) initClient() error {
	client := redis.NewClient(&redis.Options{
		Addr:     r.Host,
		DB:       r.DB,
		Password: r.Password,
		PoolSize: r.PoolSize,
	})
	ctx := context.Background()
	client = client.WithContext(ctx)

	if _, err := client.Ping(ctx).Result(); err != nil {
		return errors.New("ping failed")
	}
	r.client = client
	return nil
}

func (r *Redis7) Client() (*redis.Client, error) {
	if r.client == nil {
		err := r.initClient()
		if err != nil {
			return nil, err
		}
	}
	return r.client, nil
}

func (r *Redis7) Close() {
	if r.client != nil {
		r.client.Close()
	}
}
