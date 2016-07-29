#!/usr/bin/env bash

go run \
	test_db_server.go \
	push_config.go \
	push_db_util.go \
	$@
