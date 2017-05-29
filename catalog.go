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

type chunkCompression uint8

const (
	chunkCompressionNone chunkCompression = iota
	chunkCompressionZlib
)

type Chunk struct {
	Size     uint
	Checksum string
}

type Item struct {
	Path         string
	Version      uint
	CreationTime time.Time
	Compression  chunkCompression
	TotalSize    uint
	Checksum     string
	Chunks       []*Chunk

	hash hash.Hash
}

func makeItem(path string) Item {
	return Item{Path: path, CreationTime: time.Now(), hash: sha256.New()}
}

func (i *Item) addChunk(data []byte) *Chunk {
	sum := sha256.Sum256(data)
	c := &Chunk{Checksum: fmt.Sprintf("%x", sum), Size: uint(len(data))}
	i.Chunks = append(i.Chunks, c)
	i.TotalSize += c.Size
	i.hash.Write(data)
	return c
}

func (i *Item) finalize() {
	i.Checksum = fmt.Sprintf("%x", i.hash.Sum(nil))
}

type Catalog struct {
	AllItems []*Item

	itemsByPath map[string][]*Item // map path -> item, hidden in json
}

func (c *Catalog) rebuildCatalog() {
	c.itemsByPath = make(map[string][]*Item)
	for _, i := range c.AllItems {
		c.itemsByPath[i.Path] = append(c.itemsByPath[i.Path], i)
	}
	log.Debug("rebuild catalog ", c)
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
