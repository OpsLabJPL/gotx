package s3

import (
	"bytes"
	"fmt"
	"github.com/opslabjpl/goamz.git/s3"
	progressPkg "github.com/opslabjpl/gotx.git/progress"
	"github.com/opslabjpl/gotx.git/tx"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

const (
	// The smallest chunk size for doing multipart uploads to S3. Only the last chunk may be smaller.
	MinMultiChunk = 5 * 1024 * 1024
)

type chunk struct {
	src       *source
	err       error
	num       int
	offset    int64
	isLast    bool
	size      int
	data      []byte
	dataReady chan struct{}
}

func (this *chunk) Fetch(progress chan<- int64) (err error) {
	defer func() {
		this.err = err
		close(this.dataReady)
	}()
	start := this.offset
	// 'end' is inclusive in HTTP range GET requests
	end := start + int64(this.size-1)
	byteRange := fmt.Sprintf("bytes=%d-%d", start, end)
	headers := make(http.Header)
	headers.Add("Range", byteRange)
	resp, err := this.src.bucket.GetResponseWithHeaders(this.src.key, headers)
	if err != nil {
		return
	}
	reader := progressPkg.NewReader(resp.Body, progress)
	this.data, err = ioutil.ReadAll(reader)
	resp.Body.Close()
	if len(this.data) != this.size {
		err = fmt.Errorf("unexpected number of bytes received. expected %d bytes, got %d bytes.", this.size, len(this.data))
	}
	return
}

func (this *chunk) Reader() (io.ReadSeeker, error) {
	<-this.dataReady
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
	bucket    *s3.Bucket
	key       string
	chunkSize int
	num       int
	size      int64
	offset    int64
}

func NewSource(bucket *s3.Bucket, key string, chunkSize int) (tx.Source, error) {
	resp, err := bucket.Head(key, nil)
	if err != nil {
		return nil, err
	}
	obj := &source{}
	obj.bucket = bucket
	obj.key = key
	obj.chunkSize = chunkSize
	// Start at 1 for S3 part numbers
	obj.num = 1
	obj.size = resp.ContentLength
	obj.offset = 0
	return obj, nil
}

func (this *source) ChunkSize() int {
	return this.chunkSize
}

func (this *source) NextChunk(progress chan<- int64) (tx.Chunk, error) {
	if this.offset >= this.size {
		return nil, nil
	}
	c := &chunk{}
	c.src = this
	c.err = nil
	c.num = this.num
	this.num++
	c.offset = this.offset
	chunkSize := this.chunkSize
	if (c.offset + int64(chunkSize)) >= this.size {
		chunkSize = int(this.size - c.offset)
		c.isLast = true
	} else {
		c.isLast = false
	}
	c.size = chunkSize
	c.data = nil
	c.dataReady = make(chan struct{})
	this.offset += int64(chunkSize)
	return c, nil
}

func (this *source) IsPreFetch() bool {
	return false
}

func (this *source) Size() int64 {
	return this.size
}

type destination struct {
	bucket *s3.Bucket
	key    string
	multi  *s3.Multi
	parts  []s3.Part
	mutex  sync.Mutex
}

func NewDestination(bucket *s3.Bucket, key string) tx.Destination {
	obj := &destination{}
	obj.bucket = bucket
	obj.key = key
	obj.multi = nil
	obj.parts = make([]s3.Part, 0, 16)
	obj.mutex = sync.Mutex{}
	return obj
}

func (this *destination) PutChunk(chunk tx.Chunk, progress chan<- int64) error {
	reader, err := chunk.Reader()
	if err != nil {
		return err
	}
	reader = progressPkg.NewReadSeeker(reader, progress)
	// Don't do a multipart upload if this chunk is both the first and the last.
	if (chunk.Offset() == 0) && chunk.IsLast() {
		err = this.bucket.PutReader(this.key, reader, int64(chunk.Size()), "application/octet-stream", s3.Private, s3.Options{})
		return err
	}
	if !chunk.IsLast() && chunk.Size() < MinMultiChunk {
		return fmt.Errorf("non-terminal chunk with size %d bytes does not meet 5MB requirement for multipart uploading", chunk.Size())
	}
	// Initialize the multipart upload if it isn't initialized already.
	if this.multi == nil {
		err = this.initMulti()
		if err != nil {
			return err
		}
	}
	part, err := this.multi.PutPart(chunk.Num(), reader)
	if err != nil {
		return err
	}
	this.addPart(part)
	return nil
}

func (this *destination) addPart(part s3.Part) {
	this.mutex.Lock()
	defer func() {
		this.mutex.Unlock()
	}()
	this.parts = append(this.parts, part)
}

func (this *destination) initMulti() error {
	this.mutex.Lock()
	defer func() {
		this.mutex.Unlock()
	}()
	if this.multi == nil {
		multi, err := this.bucket.InitMulti(this.key, "application/octet-stream", s3.Private)
		if err != nil {
			return err
		}
		this.multi = multi
	}
	return nil
}

func (this *destination) Finish() (err error) {
	if this.multi != nil {
		err = this.multi.Complete(this.parts)
	}
	return
}

func (this *destination) Abort() {
	if this.multi != nil {
		this.multi.Abort()
	}
}
