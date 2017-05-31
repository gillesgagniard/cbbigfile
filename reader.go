package cbbigfile

import (
	"errors"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	gocb "gopkg.in/couchbase/gocb.v1"
)

type Reader struct {
	bucket *gocb.Bucket
	path   string
	item   *Item
	buffer []byte
}

func MakeReader(bucket *gocb.Bucket, path string) Reader {
	return Reader{bucket: bucket, path: path}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.item == nil {
		c := makeCatalog()
		_, err := r.bucket.Get("cbfs-catalog", &c)
		if err != nil {
			log.Error("unable to retrieve catalog", err)
			return 0, err
		}
		c.rebuildCatalog()
		i, err := c.findItem(r.path)
		if err != nil {
			return 0, err
		}
		r.item = i
	}
	for r.buffer == nil || len(p) > len(r.buffer) { // not enough stuff -> load next chunk
		chunk := r.item.nextChunk()
		if chunk == nil { // no more chunks
			log.Debug("reached EOF remaining buffer=", len(r.buffer))
			bc := copy(p, r.buffer) // copy what we have left
			return bc, io.EOF       // signal we are done
		}
		log.Debug("next chunk ", chunk)
		var chunkdata []byte
		_, err := r.bucket.Get("cbfs-chunk-"+chunk.Checksum, &chunkdata)
		if err != nil {
			log.Error("unable to read chunk", err)
			return 0, err
		}
		r.buffer = append(r.buffer, chunkdata...)
		r.item.checksumChunk(chunkdata)
	}

	bc := copy(p, r.buffer)
	r.buffer = r.buffer[bc:] // remove what we returned from buffer
	return bc, nil
}

func (r *Reader) Close() error {
	actualChecksum := fmt.Sprintf("%x", r.item.hash.Sum(nil))
	if r.item.Checksum != actualChecksum {
		log.Error("invalid item checksum desired=", r.item.Checksum, " got=", actualChecksum)
		return errors.New("invalid item checksum")
	}
	log.Info("verified item checksum=", r.item.Checksum)
	return nil
}
