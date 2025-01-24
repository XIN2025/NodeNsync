package main

const (
	CommandSET    = "set"
	CommandGET    = "get"
	CommandHELLO  = "hello"
	CommandClient = "client"
)

type ClientCommand struct {
	value string
}

type HelloCommand struct {
	value string
}
