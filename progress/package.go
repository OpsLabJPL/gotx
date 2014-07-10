// This package implements various io.Reader and io.Writer interfaces for reporting read/write progress
// to channels as the reads/writes happen.  All of the interfaces here are used to wrap existing
// Readers and Writers.
//
// The Read(), Write(), and Seek() methods will behave the same as their counterparts from the
// io.Reader, io.Writer, and io.Seeker interfaces, respectively.
package progress

import (
	"io"
)

type ReadSeeker interface {
	io.ReadSeeker
	// Returns the total number of bytes read so far.
	Progress() int64
}

type readSeeker struct {
	txChan chan<- int64
	reader io.ReadSeeker
	offset int64
	// This field is used by the ReadSeeker interface to make sure we don't double count bytes (when seeking backwards)
	largestOffset int64
}

// Returns a new ReadSeeker, wrapping the given io.ReadSeeker, that will publish read progres to the given channel.
func NewReadSeeker(r io.ReadSeeker, txChan chan<- int64) ReadSeeker {
	return &readSeeker{txChan, r, 0, 0}
}

// Returns the total number of bytes read so far.
func (this *readSeeker) Progress() int64 {
	return this.largestOffset
}

func (this *readSeeker) Read(b []byte) (n int, err error) {
	n, err = this.reader.Read(b)
	this.offset += int64(n)
	if this.offset > this.largestOffset {
		this.largestOffset = this.offset
		this.txChan <- int64(n)
	}
	return
}

func (this *readSeeker) Seek(offset int64, whence int) (newOffset int64, err error) {
	newOffset, err = this.reader.Seek(offset, whence)
	this.offset = newOffset
	return
}

type Reader interface {
	io.Reader
	// Returns the total number of bytes read so far.
	Progress() int64
}

type reader struct {
	reader   io.Reader
	txChan   chan<- int64
	progress int64
}

// Returns a new Reader, wrapping the given io.Reader, that will publish read progres to the given channel.
func NewReader(r io.Reader, txChan chan<- int64) Reader {
	return &reader{r, txChan, 0}
}

func (this *reader) Read(b []byte) (n int, err error) {
	n, err = this.reader.Read(b)
	if n > 0 {
		this.progress += int64(n)
		this.txChan <- int64(n)
	}
	return
}

func (this *reader) Progress() int64 {
	return this.progress
}

//
//
//

type WriteSeeker interface {
	io.WriteSeeker
	// Returns the total number of bytes write so far.
	Progress() int64
}

type writeSeeker struct {
	txChan        chan<- int64
	writer        io.WriteSeeker
	offset        int64
	largestOffset int64
}

// Returns a new WriteSeeker, wrapping the given io.WriteSeeker, that will publish write progres to the given channel.
func NewWriteSeeker(w io.WriteSeeker, txChan chan<- int64) WriteSeeker {
	return &writeSeeker{txChan, w, 0, 0}
}

// Returns the total number of bytes write so far.
func (this *writeSeeker) Progress() int64 {
	return this.largestOffset
}

func (this *writeSeeker) Write(b []byte) (n int, err error) {
	n, err = this.writer.Write(b)
	this.offset += int64(n)
	if this.offset > this.largestOffset {
		this.largestOffset = this.offset
		this.txChan <- int64(n)
	}
	return
}

func (this *writeSeeker) Seek(offset int64, whence int) (newOffset int64, err error) {
	newOffset, err = this.writer.Seek(offset, whence)
	this.offset = newOffset
	return
}

type Writer interface {
	io.Writer
	// Returns the total number of bytes write so far.
	Progress() int64
}

type writer struct {
	writer   io.Writer
	txChan   chan<- int64
	progress int64
}

// Returns a new Writer, wrapping the given io.Writer, that will publish write progres to the given channel.
func NewWriter(w io.Writer, txChan chan<- int64) Writer {
	return &writer{w, txChan, 0}
}

func (this *writer) Write(b []byte) (n int, err error) {
	n, err = this.writer.Write(b)
	if n > 0 {
		this.progress += int64(n)
		this.txChan <- int64(n)
	}
	return
}

// Returns the total number of bytes write so far.
func (this *writer) Progress() int64 {
	return this.progress
}
