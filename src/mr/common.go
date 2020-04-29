package mr

import (
	"io/ioutil"
	"log"
	"os"
)

//
// KeyValue: Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return "", err
	}
	file.Close()
	return string(content), nil
}
