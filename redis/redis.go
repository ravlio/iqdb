package redis

type MessageType string

const (
	TypeString  = "+"
	TypeError   = "-"
	TypeInteger = ":"
	TypeArray   = "*"
	TypeBulk    = "$"
)

type Message struct {
	Type   string
	String string
	Int    int64
	Arr    []*Message
	Bulk   []byte
	Err    error
}
