package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var maps = make(map[string][]string)

	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		log.Println("===fileName===", fileName)
		// data, err := ioutil.ReadFile(fileName)
		// if err != nil {
		// 	log.Fatalf("<doReduce> ioutil.ReadFile(%s) failed:%s", fileName, err)
		// }
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("<doReduce> os.Open(%s) failed: %s", fileName, err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)

		for {
			var v KeyValue
			err := dec.Decode(&v)
			if err != nil {
				break
			}

			_, ok := maps[v.Key]
			if !ok {
				maps[v.Key] = make([]string, 0)
			}
			maps[v.Key] = append(maps[v.Key], v.Value)
		}
	}

	var keys []string
	for key := range maps {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeFileName)
	defer mergeFile.Close()
	if err != nil {
		log.Fatalf("<doReduce> os.Create(%s) failed:%s", mergeFileName, err)
	}
	enc := json.NewEncoder(mergeFile)
	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, maps[key])})
	}

	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
