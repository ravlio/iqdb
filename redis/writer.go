package redis

import (
	"io"
	"reflect"
	"strconv"
	"errors"
	"fmt"
)

type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
	}
}

func (w *Writer) Write(args ...interface{}) error {
	argsNum := len(args)
	buf := make([]byte, 0, 10*argsNum)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(argsNum), 10)
	buf = appendTail(buf)

	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			buf = appendBytes(buf, []byte{})

		case bool:
			if v {
				buf = appendBytes(buf, []byte{'1'})
			} else {
				buf = appendBytes(buf, []byte{'0'})
			}

		case []byte:
			buf = appendBytes(buf, v)

		case string:
			buf = appendBytes(buf, []byte(v))

		case int:
			buf = appendInt64(buf, int64(v))
		case int8:
			buf = appendInt64(buf, int64(v))
		case int16:
			buf = appendInt64(buf, int64(v))
		case int32:
			buf = appendInt64(buf, int64(v))
		case int64:
			buf = appendInt64(buf, v)
		case uint:
			buf = appendInt64(buf, int64(v))
		case uint8:
			buf = appendInt64(buf, int64(v))
		case uint16:
			buf = appendInt64(buf, int64(v))
		case uint32:
			buf = appendInt64(buf, int64(v))
		case uint64:
			buf = appendInt64(buf, int64(v))

		case float32:
			buf = appendFloat(buf, float64(v))
		case float64:
			buf = appendFloat(buf, v)
		case error:
			buf = appendError(buf, arg.(error))
		default:
			return errors.New(fmt.Sprintf("Invalid argument type : {%s} while writing.", reflect.TypeOf(arg)))
		}
	}

	_, err := w.w.Write(buf)

	return err
}

func integerLen(number int64) int64 {
	var count int64 = 1
	if number < 0 {
		number = -number
		count = 2
	}
	for number > 9 {
		number /= 10
		count++
	}

	return count
}

func appendTail(buf []byte) []byte {
	return append(buf, '\r', '\n')
}

func appendInt64(buf []byte, n int64) []byte {
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, integerLen(n), 10)
	buf = appendTail(buf)
	buf = strconv.AppendInt(buf, n, 10)

	return appendTail(buf)
}

func appendBytes(buf []byte, b []byte) []byte {
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(b)), 10)
	buf = appendTail(buf)
	buf = append(buf, b...)

	return appendTail(buf)
}

func appendError(buf []byte, e error) []byte {
	buf = append(buf, '-')
	buf = append(buf, []byte(e.Error())...)

	return appendTail(buf)
}

func appendFloat(buf []byte, f float64) []byte {
	return appendBytes(buf, []byte(strconv.FormatFloat(f, 'f', -1, 64)))
}
