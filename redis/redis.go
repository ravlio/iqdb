package redis

import "errors"

type MessageType string

var ErrWrongArgNum = errors.New("Wrong arguments number")
var ErrWrongTTL = errors.New("Wrong TTL")
var ErrUnknownParseError = errors.New("Unknown parse error")

const (
	TypeString  = "+"
	TypeError   = "-"
	TypeInteger = ":"
	TypeArray   = "*"
	TypeBulk    = "$"
)

// Redis protocol format
type Message struct {
	Type   string
	String string
	Int    int64
	Arr    []*Message
	Bulk   []byte
	Err    error
}
