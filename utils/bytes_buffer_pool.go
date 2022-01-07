package utils

import (
	"bytes"
	"sync"
)

const (
	TooBigBlockSize = 1024 * 1024 * 4
)

var (
	bytesBufferPool = sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
	bytesBufferChan = make(chan *bytes.Buffer, 10)
)

func BytesBufferGet() (data *bytes.Buffer) {
	select {
	case data = <-bytesBufferChan:
	default:
		data = bytesBufferPool.Get().(*bytes.Buffer)
	}

	data.Reset()

	return data
}

func BytesBufferPut(data *bytes.Buffer) {
	if data == nil || len(data.Bytes()) > TooBigBlockSize {
		return
	}

	select {
	case bytesBufferChan <- data:
	default:
		bytesBufferPool.Put(data)
	}
}
