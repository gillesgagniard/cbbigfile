package cbbigfile

import (
	"io"

	log "github.com/sirupsen/logrus"
	gocb "gopkg.in/couchbase/gocb.v1"
)

type Reader struct {
	bucket            *gocb.Bucket
	path              string
	item              *Item
	currentChunkIndex uint
	buffer            []byte
}

func MakeReader(bucket *gocb.Bucket, path string) Reader {
	return Reader{bucket: bucket, path: path}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	log.Debug("read length=", len(p))
	if r.item == nil {
		var c Catalog
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
		if r.currentChunkIndex >= uint(len(r.item.Chunks)) { // no more chunk
			log.Debug("reached EOF buffer=", len(r.buffer))
			bc := copy(p, r.buffer) // copy what we have left
			log.Debug("copied bytes=", bc)
			return bc, io.EOF // signal we are done
		}
		chunk := r.item.Chunks[r.currentChunkIndex]
		log.Debug("retrieve chunk ", chunk)
		var chunkdata []byte
		_, err := r.bucket.Get("cbfs-chunk-"+chunk.Checksum, &chunkdata)
		if err != nil {
			log.Error("unable to read chunk", err)
			return 0, err
		}
		r.currentChunkIndex++
		r.buffer = append(r.buffer, chunkdata...)
	}

	bc := copy(p, r.buffer)
	r.buffer = r.buffer[bc:] // remove what we returned from buffer
	return bc, nil
}

func (w *Reader) Close() error {
	return nil
}
