#!/bin/bash

go run mrcoordinator.go pg-*.txt &
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so 2>&1 /dev/null &
go run mrworker.go wc.so 2>&1 /dev/null &
go run mrworker.go wc.so 2>&1 /dev/null &