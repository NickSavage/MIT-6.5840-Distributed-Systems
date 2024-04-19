#!/bin/bash

go run mrcoordinator.go pg-*.txt &
go build -buildmode=plugin ../mrapps/wc.go
go build -buildmode=plugin ../mrapps/indexer.go
go run mrworker.go indexer.so &
go run mrworker.go indexer.so &
go run mrworker.go indexer.so &