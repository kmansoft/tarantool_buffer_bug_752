package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tarantool/go-tarantool"
	"io/ioutil"
	"os"
	"time"
)

const (
	CONFIG_FILE_ENV_VAR = "PUSH_CONFIG_FILE"
	CONFIG_FILE_DEFAULT = "push_config_debug.json"
	CONNECT_RETRY_COUNT = 10
)

// Database config

type PushDbConfig struct {
	Timeout       time.Duration `json:"timeout"`
	Reconnect     time.Duration `json:"reconnect"`
	MaxReconnects uint          `json:"max_reconnects"`
	User          string        `json:"user"`
	Pass          string        `json:"pass"`
}

func (db PushDbConfig) Connect(server PushServerConfig) (*tarantool.Connection, error) {
	addr := server.DbConnect
	fmt.Printf("Database: addr = %q, timeout = %s, reconnect = %s, max = %d\n",
		addr, db.Timeout, db.Reconnect, db.MaxReconnects)

	opts := tarantool.Opts{
		Timeout:       db.Timeout,
		Reconnect:     db.Reconnect,
		MaxReconnects: db.MaxReconnects}

	if len(db.User) > 0 && len(db.Pass) > 0 {
		fmt.Println("Access control:", "user", db.User)
		opts.User = db.User
		opts.Pass = db.Pass
	} else {
		fmt.Println("Access control:", "guest")
	}

	var lastErr error = nil
	lastDelay := 250 * time.Millisecond
	for i := 0; i < CONNECT_RETRY_COUNT; i++ {
		client, err := tarantool.Connect(addr, opts)
		if err == nil {
			fmt.Printf("Connected to %q\n", addr)
			return client, err
		}

		lastErr = err
		terr, ok := err.(tarantool.ClientError)
		if !ok || terr.Code != tarantool.ErrConnectionNotReady {
			return client, err
		}

		fmt.Printf("Waiting %s and will try connecting again...\n", lastDelay)
		time.Sleep(lastDelay)
		lastDelay = lastDelay * 2
	}
	return nil, lastErr
}

// Logging

type PushLogConfig struct {
	Silent  bool `json:"silent"`
	Verbose bool `json:"verbose"`
}

// Common server

type PushServerConfig struct {
	DbConnect string `json:"db_connect"`
}

// EWS server config

type PushEwsConfig struct {
	PushLogConfig
	PushServerConfig
}

func (ews PushEwsConfig) String() string {
	return fmt.Sprintf("silent = %t, verbose = %t",
		ews.Silent, ews.Verbose)
}

// All together now

type PushConfig struct {
	Db   PushDbConfig   `json:"db"`
	Ews  PushEwsConfig  `json:"ews"`
}

func NewPushConfig() (*PushConfig, error) {
	db_addr := "127.0.0.1:60501"

	db := PushDbConfig{
		Timeout:       5000 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3}

	ews := PushEwsConfig{}
	ews.DbConnect = db_addr

	config := &PushConfig{
		Db:   db,
		Ews:  ews}

	// Get file name from env var if there
	file_name := os.Getenv(CONFIG_FILE_ENV_VAR)
	if len(file_name) == 0 {
		file_name = CONFIG_FILE_DEFAULT
	}

	// Read and parse
	file_data, err := ioutil.ReadFile(file_name)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file_data, config)
	if err != nil {
		return nil, err
	}

	// Validate
	if config.Ews.DbConnect == "" {
		return nil, errors.New("Empty db address for EWS")
	}

	return config, nil
}
