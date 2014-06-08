package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "strconv"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	wordList := list.New()
	length := len(value)
	for i := 0; i < length; {
		for i < length && !isChar(value[i]) {
			i++
		}

		if i >= length {
			break
		}

		tmp := ""
		for i < length && isChar(value[i]) {
			tmp = tmp + string(value[i])
			i++
		}

		//fmt.Printf("tmp: %s\n", tmp)

		wordList.PushBack(mapreduce.KeyValue{ tmp, "1" })
	}

	return wordList
}

func isChar(ch byte) bool {
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
		return true
	}
	return false
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {

	sum := 0

	for e := values.Front(); e != nil; e = e.Next() {

		num, err := strconv.Atoi(e.Value.(string))

		if err != nil {
			continue
		}
		
		sum += num
	}

	return strconv.Itoa(sum)
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
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
