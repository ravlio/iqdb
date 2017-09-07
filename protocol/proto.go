package protocol

import "io"
import "encoding/binary"

const (
	OpSet      = 1
	OpRemove   = 2
	OpTTL      = 3
	OpListPush = 4
	OpListPop  = 5
	OpHashDel  = 6
	OpHashSet  = 7
	OpError    = 8
	OpGet      = 9
	OpOk       = 10
)

func readString(rdr io.Reader) (string, error) {
	b, err := readBytes(rdr)

	if err != nil {
		return "", err
	}

	return string(b), nil
}

func readBytes(rdr io.Reader) ([]byte, error) {
	var l = make([]byte, 8)

	_, err := io.ReadFull(rdr, l)
	//_, err := rdr.Read(l)
	if err != nil {
		return nil, err
	}

	//print("len ", binary.LittleEndian.Uint64(l), " ")
	b := make([]byte, binary.LittleEndian.Uint64(l))

	_, err = io.ReadFull(rdr, b)
	//println("v", string(b))
	if err != nil {
		return nil, err
	}

	return b, nil
}

func readUint64(rdr io.Reader) (uint64, error) {
	i := make([]byte, 8)
	_, err := io.ReadFull(rdr, i)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(i), nil
}
