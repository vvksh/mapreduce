# Mapreduce

This repo contains mapreduce implementation in Go.

This is first lab in MIT's course on distributed systems 6.824. Please see the lab instruction [here](https://github.com/vksah32/mapreduce/blob/master/lab_instruction/lab-mr.md). Note that I didn't submit the lab, since I wasn't enrolled. 

## Overview

We have a **master** and multiple **workers**. 
1. User starts a master by specifying number of reducers (nR) and a list of files to run mapReduce on. User also starts multiple workers with custom mapper/reducer funtions
2. Master stores the list of files the users provided and other state in a master Data structure.
3. When workers are started, they ping master for task every 5 sec (its synchronous, so worker finishes the task at hand before pinging the master again )
4. During map phase:
    - master gives worker one file to run map 
    - worker runs mapper on each file. for each map task (i-th file), the worker produces bunch of key, values. Each key,value is stored as a json entry in a file called `mr-{{i}}-{{r}}.txt` where `i` is   the file index  and `r = hash(key) mod nR`. So each map tasks produces at most `rn` files. So overall after all map tasks are done there will be `numFiles * nR` files.
5. During Reduce phase:
    - there will be nR reduce tasks. For each r-th task, master gives worker a reduce task with ID = `r` and a list of all files which have key, values such that `r = hash(key) mod nR`  namely the files 
      `mr-*-{{r}}.txt`.
    - the output of each reduce task will be in a file name `mr-out-r`


### Handling worker crash
The master will start a goroutine to monitor state of tasks. It checks if any tasks are not marked DONE within 10 secs. If so, they are marked FAILED and eligible for reassignment.
One of the map reduce apps is a `crash.go` which crashes or runs slow with certain probabilities. The test file `src/main/test-mr.sh` produces scenario to run the crash scenario.

## How to run

- Start master with a list of files as argument

```bash
cd src/main
go run mrmaster.go pg-being_ernest.txt pg-grimm.txt
```

- Start some workers with a mapreduce plugin (eg wordcounter)
```
cd src/main
# compile plugin
go build -buildmode=plugin ../mrapps/wc.go

# run worker
go run mrworker.go wc.so
```
## Test

```
cd src/main
sh test-mr.sh
```


## TODO

This project is not ready for distributed envronment. All the mappers produce the intermediate files in the same directory which reducers can access. 
In distributed environment, the reducers will either have to connect to mapper via RPC to access the files or the mappers will have to store files in 
a distributed file system (eg HDFS)
