package main

import (
	"container/list"
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

func notALetter(symbol rune) bool {
	return !unicode.IsLetter(symbol)
}

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	mappedList := list.New()                       // create a temporary list to store mapped key/value pairs
	words := strings.FieldsFunc(value, notALetter) // weed out symbols and numbers
	for i := 0; i < len(words); i++ {
		mappedList.PushBack(mapreduce.KeyValue{words[i], "1"}) // initialize each word(with repetition) to a count of 1
	}

	return mappedList
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	totalCount := 0
	for e := values.Front(); e != nil; e = e.Next() {
		// NOTE: must assert "string" type when accessing a struct value
		count, err := strconv.Atoi(e.Value.(string)) // get total count of a word
		// check if there's a error, if not continue
		if err == nil {
			totalCount += count
		}
	}
	return strconv.Itoa(totalCount) // convert totalCount value to string
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
