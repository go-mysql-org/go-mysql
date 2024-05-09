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

var _ io.WriteCloser = Compressor{}
var _ io.ReadCloser = Decompressor{}

type Compressor struct {
	target     io.Writer
	zlibWriter *zlib.Writer
}

type Decompressor struct {
	src        io.Reader
	zlibReader io.ReadCloser
}

func NewCompressor(target io.Writer) (io.WriteCloser, error) {
	w := zlibWriterPool.Get().(*zlib.Writer)
	w.Reset(target)

	return Compressor{
		target:     target,
		zlibWriter: w,
	}, nil
}

func NewDecompressor(src io.Reader) (io.Reader, error) {
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

	return Decompressor{
		src:        src,
		zlibReader: rc,
	}, nil
}

func (c Compressor) Write(data []byte) (n int, err error) {
	return c.zlibWriter.Write(data)
}

func (c Compressor) Close() error {
	err := c.zlibWriter.Close()
	zlibWriterPool.Put(c.zlibWriter)
	return err
}

func (d Decompressor) Read(buf []byte) (n int, err error) {
	return d.zlibReader.Read(buf)
}

func (d Decompressor) Close() error {
	err := d.zlibReader.Close()
	zlibReaderPool.Put(d.zlibReader)
	return err
}
