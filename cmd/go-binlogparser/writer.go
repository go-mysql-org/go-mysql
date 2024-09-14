package main

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

type BinlogWriter interface {
	SetHeader(s []byte)
	SetFooter(s []byte)
	Write(p []byte) (n int, err error)
	Close() error
}

type NormalWriter struct {
	writer io.WriteCloser
}

func (w *NormalWriter) SetHeader(s []byte) {
}
func (w *NormalWriter) SetFooter(s []byte) {
}
func (w *NormalWriter) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}
func (w *NormalWriter) Close() error {
	return w.writer.Close()
}

func NewNormalWriter(writer io.WriteCloser) *NormalWriter {
	return &NormalWriter{writer: writer}
}

type FlashbackWriter struct {
	filePrefix      string
	maxCacheSize    int
	cacheHeader     []byte
	cacheFooter     []byte
	cache           [][]byte
	currentIndex    int
	currentWritten  int
	currentFileName string

	currentIOWriter io.WriteCloser
	filePartId      int
}

func (c *FlashbackWriter) Write(p []byte) (n int, err error) {
	if c.currentIOWriter == nil {
		return 0, errors.New("unknown writer")
	}
	pc := make([]byte, len(p))
	copy(pc, p)
	c.cache = append(c.cache, pc)
	//c.cache[c.currentIndex] = p
	c.currentIndex += 1
	c.currentWritten += len(p)
	if c.currentWritten >= c.maxCacheSize {
		c.currentIOWriter.Write([]byte("\nDELIMITER ;\n"))
		//time.Sleep(60 * time.Second)
		c.currentWritten = 0
		c.currentIndex = 0
		for i := len(c.cache); i > 0; i-- {
			if i == len(c.cache) {
				c.currentIOWriter.Write(c.cacheHeader)
			}
			_, err = c.currentIOWriter.Write(c.cache[i-1])
			if err != nil {
				return 0, err
			}
		}
		c.next()
		//return len(p), errors.New("cache full")
	}
	return len(p), nil
}

// NewBytesCache filePrefix may contain db_table name
func (c *FlashbackWriter) next() {
	c.currentIOWriter.Close()
	c.cache = nil
	c.filePartId += 1
	c.currentFileName = fmt.Sprintf("%s.%03d.sql", c.filePrefix, c.filePartId)
	//c.currentIOWriter, _ = os.OpenFile(c.currentFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	c.currentIOWriter, _ = os.Create(c.currentFileName)
}

func (c *FlashbackWriter) SetHeader(s []byte) {
	c.cacheHeader = s
}
func (c *FlashbackWriter) SetFooter(s []byte) {
	c.cacheFooter = s
}

func (c *FlashbackWriter) Close() (err error) {
	for i := len(c.cache); i > 0; i-- {
		if i == len(c.cache) {
			c.currentIOWriter.Write(c.cacheHeader)
		}
		_, err = c.currentIOWriter.Write(c.cache[i-1])
		if err != nil {
			return err
		}
	}
	c.currentIOWriter.Write(c.cacheFooter)
	c.cache = nil
	return c.currentIOWriter.Close()
}

func NewFlashbackWriter(filePrefix string, cacheSize int) *FlashbackWriter {
	fmt.Println("xxx", filePrefix)
	bc := &FlashbackWriter{
		filePrefix:   filePrefix,
		maxCacheSize: cacheSize,
	}
	bc.currentFileName = fmt.Sprintf("%s.%03d.sql", filePrefix, bc.filePartId)
	//bc.currentIOWriter, _ = os.OpenFile(bc.currentFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	bc.currentIOWriter, _ = os.Create(bc.currentFileName)
	return bc
}
