package main

import (
	"binlog/tools"
	"time"
)

func main() {
	go tools.BinLogListener()
	time.Sleep(2 * time.Minute)
}
