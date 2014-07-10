// This is a general package for doing parallel transfers.  It is designed to be source/destination agnostic.
package tx

import (
	"errors"
	"io"
	"sync"
)

// A Chunk is an abstraction representing fixed-length data that must be read from a source and written to a destination.
type Chunk interface {
	// This method should load the data from the source into memory (from disk or from network, etc.).
	// The progress argument is a channel that you should use to report transfer progress for chunks at a more granular level. Do not close this channel.
	Fetch(progress chan<- int64) error
	// Return a ReaderSeeker that can be used to read the loaded chunk. This should block until Fetch() is called.
	// This should return immediately if the data is available or there was an error preventing the data from being available.
	Reader() (io.ReadSeeker, error)
	// Returns the ordered chunk number (starting at 1) for this chunk.
	Num() int
	// Return the actual size of this chunk. This can be -1 if yet unknown, but should be >= 0 once Fetch() is called.
	Size() int
	// Return the offset of this chunk.  Should always be >= 0.
	Offset() int64
	// This should return true if this is the last chunk from the source (in terms of sequential offset order).
	IsLast() bool
	// Return the source from which this chunk originated.
	Source() Source
	// Return an error that happened when reading or writing this chunk
	Error() error
}

// A Source is an abstraction of a data source.  Sources are used for retrieving Chunks to be written to a Destination.  Sources
// can be a file, an object in a block store (like S3), or really anything else that is readable.
type Source interface {
	// Return the size of each chunk that will be returned from this source. The last chunk can be smaller.
	ChunkSize() int
	// Produce the next chunk in the sequence of chunks comprosing this data source. Actual chunk data may be read in this method if it makes
	// sense to do so (like reading from a stream of unknown size). If chunk data is read in this method, IsPreFetch should return true.
	// This method should return nil if there are no more chunks in the sequence.
	NextChunk(progress chan<- int64) (Chunk, error)
	// Returns true if the NextChunk method will actually read data from some data source (buffering it in memory). If this returns true,
	// Fetch() will never be called for chunks. However, if this return true, Chunk.Fetch() should return immediately without error.
	IsPreFetch() bool
	// Return the size of this data source.  Return -1 to indicate 'unknown'.
	Size() int64
}

// A Destination is an abstraction of where to write Chunks to.  It can be a file, a block store (like S3), or anything else, really.
type Destination interface {
	// This should write the data of the given chunk to the destination. This method should be blocking (i.e. not return until the write finishes).
	// If Chunk.Fetch() resulted in an error, this method should return immediately with an error.
	// The progress argument is a channel that you should use to report transfer progress for chunks at a more granular level. Do not close this channel.
	PutChunk(chunk Chunk, progress chan<- int64) error
	// This will be called when all the chunks have been written to peform any cleanup necessary. This will only be called once.
	Finish() error
	// This will be called if an error occurs and the transfer needs to be aborted. It gives the implementer a chance to clean up resources.
	// This will only be called once.
	Abort()
}

// Tx is an interface representing a transfer being processed by a TxMgr.
type Tx interface {
	// Return the source of this transfer.
	Source() Source
	// Return the destination of the transfer.
	Destination() Destination
	// Return the total number of bytes read from the source.
	Rx() int64
	// Return the total number of bytes written to the destination.
	Wx() int64
	// Aborts this transfer.
	Abort()
	// Wait for this transfer to finish.
	Wait() error
	// If an error occurred during this transfer, this method will return it.
	Error() error
}

// TxMgr is a transfer manager interface for handling multiple parallel transfers.
type TxMgr interface {
	// Starts the transfer.
	Start()
	// Adds a transfer to the transfer manager.  This must be invoked before calling Start().  It will return a Tx object
	// that can be used to monitor the transfer (including waiting for it to compelete or monitoring progress).
	Add(src Source, dst Destination) Tx
	// Returns the total number of bytes read from the Sources involved in this transfer so far.
	Rx() int64
	// Returns the total number of bytes written to the Destinations involved in this transfer so far.
	Wx() int64
	// Returns the known size of all the sources being transfered.
	KnownSize() int64
	// Returns the actual size of all sources being transfered.  This might be unknown (-1) if a source is of unknown size (like stdin).
	ActualSize() int64
	// Creates and returns a channel with capacity "size" that will contain transfers as they finish (on success or failure).
	// This will create a new channel with each invocation.  Each channel created will receive transfers as they finish.  It is
	// very important that all created channels are being drained by clients or else deadlock can occur.
	NewTxChan(size int) <-chan Tx
	// Abort all the outstanding transfers. Some may have already completed (or failed).
	Abort()
	// Closes this TxMgr and waits for all the transfers for finish.
	Wait()
}

type chunkTask struct {
	*tx
	// This channel is a gatekeeper. It will be closed when the read is complete (even if due to error).
	readDone chan struct{}
	chunk    Chunk
}

type tx struct {
	mgr     *txMgr
	src     Source
	dst     Destination
	wx      int64
	cWx     chan int64
	rx      int64
	cRx     chan int64
	err     error
	after   sync.Once
	aborted bool
	wg      sync.WaitGroup
	done    chan struct{}
}

type txMgr struct {
	wg           sync.WaitGroup
	aborted      bool
	after        sync.Once
	rx           int64
	cRx          chan int64
	wx           int64
	cWx          chan int64
	readWorkers  int
	writeWorkers int
	memPermits   chan bool
	readTasks    chan *chunkTask
	writeTasks   chan *chunkTask
	txQ          []*tx
	started      bool
	knownSize    int64
	actualSize   int64
	txChannels   []chan Tx
	mutex        sync.Mutex
}

// Creates a new TxMgr with the given number of worker threads.  The two arguments, serialRead and serialWrite, determine
// if reads and writes occur serially, respectively.  If serialRead is true, there is only one read thread.  If serialWrite
// is true, there is only one write thread.  The number of worker threads also bounds the memory used.  At any time during
// the transfers, there will be at most numThreads Chunks in memory.
func NewTxMgr(numThreads int, serialRead bool, serialWrite bool) TxMgr {
	obj := &txMgr{}
	obj.wg = sync.WaitGroup{}
	obj.aborted = false
	obj.after = sync.Once{}
	obj.rx = 0
	obj.cRx = make(chan int64, 16)
	obj.wx = 0
	obj.cWx = make(chan int64, 16)
	if numThreads < 1 {
		numThreads = 1
	}
	if serialRead {
		obj.readWorkers = 1
	} else {
		obj.readWorkers = numThreads
	}
	if serialWrite {
		obj.writeWorkers = 1
	} else {
		obj.writeWorkers = numThreads
	}
	// Create and pre-load the permit channel
	obj.memPermits = make(chan bool, numThreads)
	for i := 0; i < numThreads; i++ {
		obj.memPermits <- true
	}
	obj.readTasks = make(chan *chunkTask, numThreads)
	obj.writeTasks = make(chan *chunkTask, numThreads)
	obj.txQ = nil
	obj.mutex = sync.Mutex{}
	obj.started = false
	obj.knownSize = 0
	obj.actualSize = 0
	obj.txChannels = nil
	return obj
}

func (this *txMgr) Start() {
	if this.started {
		return
	}
	this.started = true
	// Launch the read progress handler for TxMgr
	go func() {
		for delta := range this.cRx {
			this.rx += delta
		}
	}()
	// Launch the write progress handle for TxMgr
	go func() {
		for delta := range this.cWx {
			this.wx += delta
		}
	}()
	go func() {
		for _, _tx := range this.txQ {
			if this.aborted {
				break
			}
			// Launch the read progress monitor goroutine for Tx.
			// Pass in transfer struct to avoid lexical scoping issues.
			go func(_tx *tx) {
				for delta := range _tx.cRx {
					_tx.rx += delta
					_tx.mgr.cRx <- delta
				}
			}(_tx)
			// Launch the read progress monitor goroutine for Tx.
			// Pass in transfer struct to avoid lexical scoping issues.
			go func(_tx *tx) {
				for delta := range _tx.cWx {
					_tx.wx += delta
					_tx.mgr.cWx <- delta
				}
			}(_tx)
			for !_tx.aborted && !this.aborted {
				_tx.wg.Add(1)
				// Get a memory permit if this is a pre-fetching source.
				if _tx.src.IsPreFetch() {
					<-this.memPermits
				}
				chunk, err := _tx.src.NextChunk(_tx.cRx)
				if err != nil {
					_tx.wg.Done()
					_tx.abort(err)
					// Return the memory permit if this a pre-fetching source, because we didn't actually get a chunk.
					if _tx.src.IsPreFetch() {
						this.memPermits <- true
					}
					break
				}
				if chunk == nil {
					_tx.wg.Done()
					// Return the memory permit if this a pre-fetching source, because we didn't actually get a chunk.
					if _tx.src.IsPreFetch() {
						this.memPermits <- true
					}
					break
				}
				task := &chunkTask{_tx, make(chan struct{}), chunk}
				this.readTasks <- task
				this.writeTasks <- task
			}
		}
		close(this.readTasks)
		close(this.writeTasks)
	}()
	readWorker := func(id int) {
		for task := range this.readTasks {
			// Only fetch data if the task wasn't aborted and the data wasn't prefetched.
			if !task.tx.src.IsPreFetch() {
				<-this.memPermits
				if !task.aborted {
					err := task.chunk.Fetch(task.tx.cRx)
					if err != nil {
						// Stop the presses
						task.tx.abort(err)
					}
				}
			}
			close(task.readDone)
		}
	}
	writeWorker := func(id int) {
		for task := range this.writeTasks {
			// Wait for the read task to finish, to prevent race conditions when closing channels. If we signal
			// the waitgroup to be done here while a read is still happening, it could cause a panic due to writing
			// to a closed channel.
			<-task.readDone
			if !task.aborted {
				err := task.tx.dst.PutChunk(task.chunk, task.tx.cWx)
				if err != nil {
					// Stop the presses
					task.tx.abort(err)
				} else if task.chunk.IsLast() {
					task.tx.finish()
				}
			}
			task.tx.wg.Done()
			this.memPermits <- true
		}
	}
	for i := 0; i < this.readWorkers; i++ {
		go readWorker(i)
	}
	for i := 0; i < this.writeWorkers; i++ {
		go writeWorker(i)
	}
	// This function will wait for all the transfers to finish and then close the txChannels channel.
	// It also closes the progress channels.
	go func() {
		this.wg.Wait()
		for _, c := range this.txChannels {
			close(c)
		}
		close(this.cRx)
		close(this.cWx)
	}()
}

func (this *txMgr) Add(src Source, dst Destination) Tx {
	if this.started {
		return nil
	}
	_tx := &tx{}
	_tx.mgr = this
	_tx.src = src
	_tx.dst = dst
	_tx.wg = sync.WaitGroup{}
	_tx.rx = 0
	_tx.cRx = make(chan int64, 16)
	_tx.wx = 0
	_tx.cWx = make(chan int64, 16)
	_tx.done = make(chan struct{})
	// Add 1 to the initial Tx waitgroup to eliminate race conditions
	// (possible call to Abort before we even start adding tasks could result in panic by sending to closed channel)
	_tx.wg.Add(1)
	this.mutex.Lock()
	defer this.mutex.Unlock()
	size := src.Size()
	// Update knownSize and actualSize accordingly
	if size >= 0 {
		this.knownSize += size
		if this.actualSize >= 0 {
			this.actualSize += size
		}
	} else {
		this.actualSize = -1
	}
	this.wg.Add(1)
	this.txQ = append(this.txQ, _tx)
	return _tx
}

func (this *txMgr) Rx() int64 {
	return this.rx
}

func (this *txMgr) Wx() int64 {
	return this.wx
}

func (this *txMgr) KnownSize() int64 {
	return this.knownSize
}

func (this *txMgr) ActualSize() int64 {
	return this.actualSize
}

func (this *txMgr) NewTxChan(size int) <-chan Tx {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	c := make(chan Tx, size)
	this.txChannels = append(this.txChannels, c)
	return c
}

func (this *txMgr) Abort() {
	this.aborted = true
	for _, _tx := range this.txQ {
		_tx.abort(errors.New("aborted"))
	}
}

func (this *txMgr) Wait() {
	this.wg.Wait()
}

func (this *tx) Source() Source {
	return this.src
}

func (this *tx) Destination() Destination {
	return this.dst
}

func (this *tx) Rx() int64 {
	return this.rx
}

func (this *tx) Wx() int64 {
	return this.wx
}

func (this *tx) Wait() error {
	<-this.done
	return this.err
}

func (this *tx) Abort() {
	this.abort(errors.New("user aborted"))
}

func (this *tx) Error() error {
	return this.err
}

// Sets the aborted flag and uses sync.Once to launch a goroutine to handle cleanly aborting this transfer.
// This is not a blocking function.
func (this *tx) abort(err error) {
	this.aborted = true
	this.after.Do(func() {
		this.err = err
		this.wg.Done()
		go func() {
			// Wait for read/write tasks to finish
			this.wg.Wait()
			// Then abort the destination
			this.dst.Abort()
			close(this.cRx)
			close(this.cWx)
			// Finally, report that the task is done
			close(this.done)
			this.mgr.wg.Done()
			this.mgr.broadcastDone(this)
		}()
	})
}

// Uses sync.Once to launch a goroutine to cleanly finish this transfer.  This is not a blocking function,
func (this *tx) finish() {
	this.after.Do(func() {
		this.wg.Done()
		go func() {
			// Wait for read/write tasks to finish
			this.wg.Wait()
			// Then finish the destination
			this.err = this.dst.Finish()
			close(this.cRx)
			close(this.cWx)
			// Finally, report that the task is done
			close(this.done)
			this.mgr.wg.Done()
			this.mgr.broadcastDone(this)
		}()
	})
}

// Send the finished transfer to each channel that has been registered.
func (this *txMgr) broadcastDone(doneTx *tx) {
	for _, c := range this.txChannels {
		c <- doneTx
	}
}
