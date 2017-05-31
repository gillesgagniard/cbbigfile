package cbbigfile

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"time"

	log "github.com/sirupsen/logrus"
)

//const chunkDefaultSize uint = 262144 // 256k
const chunkDefaultSize uint = 500000
const itemHistorySize uint = 3

type chunkCompression uint8

const (
	chunkCompressionNone chunkCompression = iota
	chunkCompressionZlib
)

type Chunk struct {
	Size     uint
	Checksum string

	used bool
}

type Item struct {
	Path         string
	Version      uint
	CreationTime time.Time
	Compression  chunkCompression
	TotalSize    uint
	Checksum     string
	Chunks       []*Chunk

	hash       hash.Hash
	chunkQueue []*Chunk
	catalog    *Catalog
}

func makeItem(catalog *Catalog, path string) Item {
	return Item{Path: path, CreationTime: time.Now(), catalog: catalog}
}

func (i *Item) addChunk(data []byte) *Chunk {
	sum := fmt.Sprintf("%x", sha256.Sum256(data))
	c := &Chunk{Checksum: sum, Size: uint(len(data))}
	i.Chunks = append(i.Chunks, c)
	i.catalog.allChunks[sum] = c
	i.TotalSize += c.Size

	if i.hash == nil {
		i.hash = sha256.New()
	}
	i.hash.Write(data)

	return c
}

func (i *Item) checksumChunk(data []byte) {
	if i.hash == nil {
		i.hash = sha256.New()
	}
	i.hash.Write(data)
}

func (i *Item) nextChunk() *Chunk {
	if i.chunkQueue == nil {
		i.chunkQueue = i.Chunks
	}
	if len(i.chunkQueue) == 0 {
		return nil // no more chunks
	}
	c := i.chunkQueue[0]
	i.chunkQueue = i.chunkQueue[1:]
	return c
}

func (i *Item) finalize() {
	i.Checksum = fmt.Sprintf("%x", i.hash.Sum(nil))
}

func (i *Item) markChunkUsed() {
	for _, c := range i.Chunks {
		c.used = true
	}
}

type Catalog struct {
	AllItems []*Item

	itemsByPath map[string][]*Item // map path -> item, hidden in json
	allChunks   map[string]*Chunk  // map checksum -> chunk, hidden in json, needed for garbage collection
}

func makeCatalog() Catalog {
	return Catalog{allChunks: make(map[string]*Chunk), itemsByPath: make(map[string][]*Item)}
}

func (c *Catalog) rebuildCatalog() {
	// we skim through AllItems that we got from json, build internal data structures
	// we also do some housekeeping there
	c.itemsByPath = make(map[string][]*Item)
	for _, i := range c.AllItems {
		for _, ch := range i.Chunks { // collect all known chunks
			c.allChunks[ch.Checksum] = ch
		}
		if uint(len(c.itemsByPath[i.Path])) >= itemHistorySize { // we need to trash old version
			log.Debug("rebuild : forget old version ", c.itemsByPath[i.Path][0])
			c.itemsByPath[i.Path] = c.itemsByPath[i.Path][1:] // remove oldest
		}
		c.itemsByPath[i.Path] = append(c.itemsByPath[i.Path], i)
	}

	// now we reconstruct the "new" AllItems and mark all relevant chunks as used
	c.AllItems = []*Item{}
	for _, li := range c.itemsByPath {
		for _, i := range li {
			c.AllItems = append(c.AllItems, i)
			i.markChunkUsed()
		}
	}
	log.Debug("end rebuild catalog ", c)
}

func (c *Catalog) addNewItem(i *Item) {
	ip, ok := c.itemsByPath[i.Path]
	var version uint
	if ok {
		version = ip[len(ip)-1].Version + 1
	} else {
		version = 0
	}
	i.Version = version
	c.AllItems = append(c.AllItems, i)
	c.itemsByPath[i.Path] = append(c.itemsByPath[i.Path], i)
}

func (c *Catalog) findItem(path string) (*Item, error) {
	ip, ok := c.itemsByPath[path]
	if ok {
		log.Debug("found item in catalog ", ip[len(ip)-1])
		return ip[len(ip)-1], nil
	}
	log.Error("cannot find path in catalog")
	return nil, errors.New("cannot find path in catalog")
}
