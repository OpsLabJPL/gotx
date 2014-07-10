package std

import (
	"bufio"
	"bytes"
	"errors"
	progressPkg "github.com/opslabjpl/gotx.git/progress"
	"github.com/opslabjpl/gotx.git/tx"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type chunk struct {
	src    *source
	err    error
	num    int
	offset int64
	isLast bool
	size   int
	data   []byte
}

func (this *chunk) Fetch(progress chan<- int64) error {
	// Do nothing, we pre-fetched the data
	return nil
}

func (this *chunk) Reader() (io.ReadSeeker, error) {
	return bytes.NewReader(this.data), nil
}

func (this *chunk) Num() int {
	return this.num
}

func (this *chunk) Size() int {
	return this.size
}

func (this *chunk) Offset() int64 {
	return this.offset
}

func (this *chunk) IsLast() bool {
	return this.isLast
}

func (this *chunk) Source() tx.Source {
	return this.src
}

func (this *chunk) Error() error {
	return this.err
}

type source struct {
	file      *os.File
	buf       *bufio.Reader
	chunkSize int
	stream    bool
	size      int64
	num       int
	offset    int64
	eof       bool
}

func NewSource(file *os.File, chunkSize int) (tx.Source, error) {
	src := &source{}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	src.file = file
	src.buf = bufio.NewReader(file)
	src.chunkSize = chunkSize
	src.stream = !info.Mode().IsRegular()
	if src.stream {
		src.size = -1
	} else {
		src.size = info.Size()
	}
	src.num = 1
	src.offset = 0
	return src, nil
}

func (this *source) ChunkSize() int {
	return this.chunkSize
}

func (this *source) NextChunk(progress chan<- int64) (tx.Chunk, error) {
	if this.eof {
		return nil, nil
	}
	c := &chunk{}
	c.src = this
	c.err = nil
	reader := progressPkg.NewReader(io.LimitReader(this.buf, int64(this.chunkSize)), progress)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		this.eof = true
		return nil, err
	}
	if len(data) < this.chunkSize {
		this.eof = true
		c.isLast = true
	} else if (len(data) == this.chunkSize) && (this.buf.Buffered() == 0) {
		// If the amount of data read is equal to the chunk size, do some buffer hax to determine if
		// this is the last chunk.
		_, err = this.buf.ReadByte()
		if err != nil {
			this.eof = true
			if err == io.EOF {
				c.isLast = true
			} else {
				return nil, err
			}
		}
		err = this.buf.UnreadByte()
		if err != nil {
			this.eof = true
			return nil, err
		}
	}
	c.num = this.num
	this.num++
	c.offset = this.offset
	this.offset += int64(len(data))
	c.data = data
	c.size = len(data)
	return c, nil
}

func (this *source) IsPreFetch() bool {
	return true
}

func (this *source) Size() int64 {
	return this.size
}

type destination struct {
	file   *os.File
	stream bool
	mutex  sync.Mutex
}

func NewDestination(file *os.File) (tx.Destination, error) {
	dst := &destination{}
	dst.file = file
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	dst.stream = !info.Mode().IsRegular()
	dst.mutex = sync.Mutex{}
	return dst, nil
}

func (this *destination) PutChunk(chunk tx.Chunk, progress chan<- int64) error {
	reader, err := chunk.Reader()
	if err != nil {
		return err
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	// Only attempt to seek if this is a regular file!
	if !this.stream {
		o, err := this.file.Seek(chunk.Offset(), 0)
		if o != chunk.Offset() {
			return errors.New("couldn't seek to offset " + string(o))
		}
		if err != nil {
			return err
		}
	}
	writer := progressPkg.NewWriter(this.file, progress)
	n, err := io.Copy(writer, reader)
	if err != nil {
		return err
	}
	if n != int64(chunk.Size()) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (this *destination) Finish() (err error) {
	if !this.stream {
		err = this.file.Close()
	}
	return
}

func (this *destination) Abort() {
	if !this.stream {
		this.file.Close()
	}
}
