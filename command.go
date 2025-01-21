package main

type CommandType string

const (
	CmdPing      CommandType = "PING"
	CmdQuit      CommandType = "QUIT"
	CmdAuth      CommandType = "AUTH"
	CmdSet       CommandType = "SET"
	CmdGet       CommandType = "GET"
	CmdDel       CommandType = "DEL"
	CmdIncr      CommandType = "INCR"
	CmdHSet      CommandType = "HSET"
	CmdHGet      CommandType = "HGET"
	CmdPublish   CommandType = "PUBLISH"
	CmdSubscribe CommandType = "SUBSCRIBE"
	CmdMonitor   CommandType = "MONITOR"
	CmdInfo      CommandType = "INFO"
	CmdRole      CommandType = "ROLE"
	CmdHelp      CommandType = "HELP"
	CmdSync      CommandType = "SYNC"
	CmdReplicaOf CommandType = "REPLICAOF"
)

type PingCommand struct{}

type QuitCommand struct{}
type UnsubscribeCommand struct {
	channels []string
}
type AuthCommand struct {
	username string
	password string
}

type SetCommand struct {
	key   []byte
	value []byte
}

type ReplicaOfCommand struct {
	host string
	port string
}

type SyncCommand struct{}
type GetCommand struct {
	key []byte
}

type DelCommand struct {
	key []byte
}

type IncrCommand struct {
	key []byte
}

type HSetCommand struct {
	key   []byte
	field []byte
	value []byte
}
type CommandCommand struct{}
type HGetCommand struct {
	key   []byte
	field []byte
}

type Command interface{}

type PublishCommand struct {
	channel string
	message []byte
}

type SubscribeCommand struct {
	channel string
}

type MonitorCommand struct{}

type InfoCommand struct{}

type RoleCommand struct{}

type HelpCommand struct {
	command string
}
