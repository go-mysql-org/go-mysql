package compress

import (
	"bytes"
	"compress/zlib"
	"io"
	"sync"
)

const DefaultCompressionLevel = 6

var (
	zlibReaderPool *sync.Pool
	zlibWriterPool sync.Pool
)

func init() {
	zlibReaderPool = &sync.Pool{
		New: func() interface{} {
			return nil
		},
	}

	zlibWriterPool = sync.Pool{
		New: func() interface{} {
			w, err := zlib.NewWriterLevel(new(bytes.Buffer), DefaultCompressionLevel)
			if err != nil {
				panic(err)
			}
			return w
		},
	}
}

func BorrowWriter(target io.Writer) io.WriteCloser {
	w := zlibWriterPool.Get().(*zlib.Writer)
	w.Reset(target)
	return w
}

func BorrowReader(src io.Reader) (io.Reader, error) {
	var (
		rc  io.ReadCloser
		err error
	)

	if r := zlibReaderPool.Get(); r != nil {
		rc = r.(io.ReadCloser)
		if rc.(zlib.Resetter).Reset(src, nil) != nil {
			return nil, err
		}
	} else {
		if rc, err = zlib.NewReader(src); err != nil {
			return nil, err
		}
	}

	return rc, nil
}

func ReturnWriter(w io.WriteCloser) {
	if _, ok := w.(*zlib.Writer); !ok {
		zlibWriterPool.Put(w)
	}
}

func ReturnReader(r io.Reader) {
	if _, ok := r.(zlib.Resetter); ok {
		zlibReaderPool.Put(r)
	}
}
