package redis

import "errors"

type MessageType string

var ErrWrongArgNum = errors.New("wrong arguments number")
var ErrWrongTTL = errors.New("wrong TTL")
var ErrUnknownParseError = errors.New("unknown parse error")

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
