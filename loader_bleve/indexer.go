package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/lang/en"
	"github.com/blevesearch/bleve/analysis/token/keyword"
	mm "github.com/blevesearch/bleve/mapping"
)

type Indexer struct {
	path    string
	batchSz int
	shards  []bleve.Index
	alias   bleve.IndexAlias
}

// New returns a new indexer.
func New(path string, nShards, batchSz int) *Indexer {
	return &Indexer{
		path:    path,
		batchSz: batchSz,
		shards:  make([]bleve.Index, 0, nShards),
		alias:   bleve.NewIndexAlias(),
	}
}

func (i *Indexer) Open() error {
	err := os.MkdirAll(i.path, 0755)
	if err != nil {
		return fmt.Errorf("unable to create the index directory %s", i.path)
	}

	for shardNumber := 0; shardNumber < cap(i.shards); shardNumber++ {
		path := filepath.Join(i.path, strconv.Itoa(shardNumber))
		fmt.Printf("adding shard %d to path %s\n", shardNumber, path)
		shard, err := bleve.New(path, bleve.NewIndexMapping())
		if err != nil {
			return fmt.Errorf("couldn't create shard %d for index %s", shardNumber, i.path)
		}

		i.shards = append(i.shards, shard)
		i.alias.Add(shard)
	}

	return nil
}

func (i *Indexer) Index(hotels []Hotel) error {
	hotelsPerShard := len(hotels) / len(i.shards)
	leftovers := len(hotels) % len(i.shards)
	from := 0
	var wg sync.WaitGroup
	wg.Add(len(i.shards))
	for shardNumber, shard := range i.shards {
		to := from + hotelsPerShard
		if shardNumber == len(i.shards)-1 {
			to = from + hotelsPerShard + leftovers
		}
		go i.IndexShard(&wg, shard, hotels[from:to])
		from = to
	}
	wg.Wait()
	return nil
}

func (i *Indexer) IndexShard(wg *sync.WaitGroup, shard bleve.Index, hotels []Hotel) error {
	defer wg.Done()
	batch := shard.NewBatch()

	for index, hotel := range hotels {
		err := batch.Index(strconv.Itoa(index), hotel)
		if err != nil {
			return fmt.Errorf("could not index hotel %s", hotel.Name)
		}
		if index != 0 && index%i.batchSz == 0 {
			err := shard.Batch(batch)
			if err != nil {
				return fmt.Errorf("could not batch hotels")
			}
			batch = shard.NewBatch()
		}
	}

	err := shard.Batch(batch)
	if err != nil {
		return fmt.Errorf("could not batch hotels")
	}
	return nil
}

func (i *Indexer) Search(search *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return i.alias.Search(search)
}

func mapping() *mm.IndexMappingImpl {
	// a generic reusable mapping for english text
	englishTextFieldMapping := bleve.NewTextFieldMapping()
	englishTextFieldMapping.Analyzer = en.AnalyzerName

	articleMapping := bleve.NewDocumentMapping()

	zipCodeFieldMapping := bleve.NewTextFieldMapping()
	zipCodeFieldMapping.Analyzer = keyword.Name

	// body
	articleMapping.AddFieldMappingsAt("Name", englishTextFieldMapping)
	articleMapping.AddFieldMappingsAt("Zip", zipCodeFieldMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = articleMapping
	indexMapping.DefaultAnalyzer = "en"
	return indexMapping
}
