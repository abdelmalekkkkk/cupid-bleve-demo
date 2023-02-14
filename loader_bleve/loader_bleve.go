package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/blevesearch/bleve"
)

type Hotel struct {
	Name     string
	City     string
	Zip      string
	Country  string
	Address  string
	Location string
}

func load(index *Indexer) {
	f, err := os.Open("i.csv")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	fmt.Println("reading csv")
	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	var hotels []Hotel
	start := time.Now()
	for i, line := range data {
		if i == 0 {
			continue
		}
		hotel := Hotel{
			Name:     line[1],
			City:     line[2],
			Country:  line[3],
			Address:  line[4],
			Location: line[6] + "," + line[7],
		}

		hotels = append(hotels, hotel)
	}
	elapsed := time.Since(start)
	fmt.Printf("loading csv took %s", elapsed)
	fmt.Println("indexsing csv")
	start = time.Now()
	err = index.Index(hotels)
	if err != nil {
		panic(err)
	}
	elapsed = time.Since(start)

	fmt.Printf("loading took %s\n", elapsed)
}

func main() {
	fmt.Println("creating indexes")
	fmt.Println("loading index")
	index := New("indexes", 20, 2000)
	if err := index.Open(); err != nil {
		fmt.Println("failed to open indexer:", err)
		os.Exit(1)
	}

	load(index)

	query := bleve.NewQueryStringQuery("Sofitel")
	search := bleve.NewSearchRequest(query)
	result, _ := index.Search(search)
	fmt.Println(result.Hits)
}
