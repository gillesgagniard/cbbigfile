package cbbigfile

import (
	log "github.com/sirupsen/logrus"
	gocb "gopkg.in/couchbase/gocb.v1"
)

type Writer struct {
	bucket  *gocb.Bucket
	item    Item
	buffer  []byte
	catalog *Catalog
}

func MakeWriter(bucket *gocb.Bucket, path string) Writer {
	c := makeCatalog()
	return Writer{bucket: bucket, item: makeItem(&c, path), catalog: &c}
}

func (w *Writer) Write(p []byte) (n int, err error) {
	if w.buffer == nil {
		w.buffer = make([]byte, 0, chunkDefaultSize)
	}
	bufferRemaining := cap(w.buffer) - len(w.buffer)
	if bufferRemaining > len(p) {
		w.buffer = append(w.buffer, p...)
	} else {
		w.buffer = append(w.buffer, p[:bufferRemaining]...) // write only what fits in the buffer capacity
		err := w.writeChunk()
		if err != nil {
			return 0, err
		}
		w.buffer = make([]byte, 0, chunkDefaultSize) // reinit buffer
		w.Write(p[bufferRemaining:])                 // recursively write remaining data
	}
	return len(p), nil
}

func (w *Writer) Close() error {
	err := w.writeChunk()
	if err != nil {
		return err
	}
	w.item.finalize()
	err = w.updateCatalog()
	if err != nil {
		return err
	}
	h := makeHouseKeeper(w.bucket, w.catalog)
	err = h.do()
	if err != nil {
		return err
	}
	log.Debug("writer closed")
	return nil
}

func (w *Writer) updateCatalog() error {
	cas, err := w.bucket.GetAndLock("cbfs-catalog", 10, w.catalog)
	if err == gocb.ErrKeyNotFound {
		w.bucket.Insert("cbfs-catalog", nil, 0) // create an empty catalog
		cas, err = w.bucket.GetAndLock("cbfs-catalog", 10, w.catalog)
	}
	if err != nil {
		log.Error("cannot retrieve catalog ", err)
		return err
	}
	w.catalog.rebuildCatalog() // we need to first rebuild to initialize internal data structures
	w.catalog.addNewItem(&w.item)
	w.catalog.rebuildCatalog() // and then once again to purge old versions
	_, err = w.bucket.Replace("cbfs-catalog", w.catalog, cas, 0)
	if err != nil {
		log.Error("cannot replace catalog ", err)
		return err
	}
	log.Debug("updated catalog ", w.catalog)
	return nil
}

func (w *Writer) writeChunk() error {
	if len(w.buffer) == 0 {
		return nil // nothing to do
	}

	c := w.item.addChunk(w.buffer)
	log.Debug("write chunk size=", len(w.buffer), " checksum=", c.Checksum)
	_, err := w.bucket.Upsert("cbfs-chunk-"+c.Checksum, w.buffer, 0)
	if err != nil {
		log.Error("chunk write error=", err)
		return err
	}
	return nil
}
