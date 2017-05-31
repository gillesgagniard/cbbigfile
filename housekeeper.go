package cbbigfile

import gocb "gopkg.in/couchbase/gocb.v1"

type houseKeeper struct {
	bucket  *gocb.Bucket
	catalog *Catalog
}

func makeHouseKeeper(bucket *gocb.Bucket, catalog *Catalog) houseKeeper {
	return houseKeeper{bucket: bucket, catalog: catalog}
}

func (h *houseKeeper) do() error {
	for _, c := range h.catalog.allChunks {
		if !c.used {
			_, err := h.bucket.Remove("cbfs-chunk-"+c.Checksum, 0)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
