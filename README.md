gotx
====

This is a generalized package for performing parallel transfers.  Right now it supports reading from and writing to local files and Amazon S3 objects.

### Project Structure

* *progress* - A package for wrapping io.Reader and io.Writer interfaces that also provides read/write progress reporting to a channel
* *tx* - Main implementation for the transfer package.  This package contains the high-level interfaces for managing transfers.
* *tx/file* - Actual implementation of Source and Destination for local files.
* *tx/s3* - Actual implemention of Source and Destination for Amazon S3 objects.

### Usage

Here is a very simplified high-level example of how to use this package:

```
import (
	"fmt"
	"github.com/opslabjpl/gotx.git/tx"
	txFile "github.com/opslabjpl/gotx.git/tx/file"
	txS3 "github.com/opslabjpl/gotx.git/tx/s3"
)
// Create a new transfer manager with 32 worker threads, but only one reader thread (serial reads).
txMgr := tx.TxMgr(32, true, false)
// ... code to open some file.
// Create a new file source that will generate chunk sizes based on the minimum chunk size for S3 MultiPart uploads.
src := txFile.NewSource(file, txS3.MinMultiChunk)
// ... code to get an s3.Bucket object from the goamz package
dst := txS3.NewDestination(bucket, key)
tx := txMgr.Add(src, dst)
// Create a channel to listen for transfers as they finish (make channel with buffer size of 16)
doneChan := txMgr.NewTxChan(16)
// Start the transfer(s)!
txMgr.Start()
// Here we iterate over all the transfers as they finish.  If you don't care about individual transfers,
// you can just use txMgr.Wait().
for finishedTx := range doneChan {
	if finishedTx.Error() != nil {
		fmt.Printf("error: %s", finishedTx.Error())
	}
}
fmt.Printf("all done!")
```

Example usage can be found in the following projects:

* https://github.com/opslabjpl/earthkit-cli/blob/master/workspace/remote/remote.go
