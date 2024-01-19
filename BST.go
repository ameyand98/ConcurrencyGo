package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var waitGroup = sync.WaitGroup{}

var trees []*BST
var numHashed int

var equal [][]bool

var hashIDMap *ConcurrentMap

var lock sync.Mutex

var workBuffer *ConcBuffer

func main() {
	hashWorkersPtr := flag.Int("hash-workers", 0, "num_hash_workers")
	dataWorkersPtr := flag.Int("data-workers", 0, "num_data_workers")
	compWorkersPtr := flag.Int("comp-workers", 0, "num_comp_workers")
	inputPtr := flag.String("input", "", "input_file")
	flag.Parse()

	generateTreesFromFile(*inputPtr)
	compareBST(*hashWorkersPtr, *dataWorkersPtr, *compWorkersPtr)
}

func generateTreesFromFile(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := scanner.Text()
		bst := generateTree(line)
		trees = append(trees, bst)
	}
}

func generateTree(vals string) *BST {
	ints := strings.Fields(vals)
	rootVal, _ := strconv.Atoi(ints[0])
	root := &Node{rootVal, nil, nil}

	// rwMutex := &sync.RWMutex{}
	bst := &BST{root, 0}
	length := len(ints)
	for i := 0; i < length; i = i + 1 {
		val, _ := strconv.Atoi(ints[i])
		bst.Insert(val)
	}

	return bst

}

func compareBST(hashWorkers int, dataWorkers int, compWorkers int) {
	// hash computation
	channel := make(chan [2]int)
	if hashWorkers == 1 && dataWorkers == 0 && compWorkers == 0 {
		start := time.Now()
		sequentialHashCompute(false)
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("hashTime:", duration)
		return
	} else if hashWorkers > 1 && dataWorkers == 0 && compWorkers == 0 {
		start := time.Now()
		parallelHashCompute(hashWorkers, nil, false, false)
		waitGroup.Wait()
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("hashTime:", duration)
		return
	}

	// hash group computation
	if dataWorkers == 1 && hashWorkers > 1 {
		start := time.Now()
		parallelHashCompute(hashWorkers, channel, true, true)
		waitGroup.Add(1)
		go fillMapWithChannel(channel)
		waitGroup.Wait()
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("hashTime:", duration)
		fmt.Println("hashGroupTime:", duration)
		printGroups(true)
	} else if dataWorkers == hashWorkers && hashWorkers > 1 {
		// individually update map
		start := time.Now()
		parallelHashCompute(hashWorkers, channel, true, false)
		waitGroup.Wait()
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("hashTime:", duration)
		fmt.Println("hashGroupTime:", duration)
		printGroups(true)
	} else if hashWorkers > 1 && dataWorkers > 1 && hashWorkers != dataWorkers {
		// optional impl
	} else {
		start := time.Now()
		sequentialHashCompute(true)
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("hashTime:", duration)
		fmt.Println("hashGroupTime:", duration)
		printGroups(true)
	}

	if compWorkers == 0 {
		return
	}
	if compWorkers == 1 {
		start := time.Now()
		sequentialTreeComp()
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("compareTreeTime:", duration)
		createEqualGroups()
		printGroups(false)
	} else {
		start := time.Now()
		compareTreesByBuffer(compWorkers)
		// compareTrees(len(trees))
		waitGroup.Wait()
		duration := time.Since(start)
		// outFile.WriteString(fmt.Sprintf("%f\n", duration*1000))
		fmt.Println("compareTreeTime:", duration)
		createEqualGroups()
		// createGroups()
		printGroups(false)
		// for _, val := range equalGroups.items {
		// 	fmt.Println(val.items)
		// }
	}

}

func parallelHashCompute(hashWorkers int, channel chan [2]int, mapUpdate bool, updateMapByChan bool) {
	numTrees := len(trees)
	var numThreads int
	if hashWorkers == 0 {
		numThreads = numTrees
	} else {
		numThreads = hashWorkers
	}
	treesWorkPerThread := numTrees / numThreads
	if numTrees%numThreads != 0 {
		treesWorkPerThread = treesWorkPerThread + 1
	}

	if mapUpdate {
		mutex := &sync.RWMutex{}
		actualMap := make(map[int]*ConcurrentSlice)
		hashIDMap = &ConcurrentMap{!updateMapByChan, mutex, actualMap}
	}

	var startOffset int
	for i := 0; i < numThreads; i = i + 1 {
		waitGroup.Add(1)
		go generateHashFromOneThread(startOffset, treesWorkPerThread, mapUpdate, updateMapByChan, channel)
		startOffset = startOffset + treesWorkPerThread
	}
}

func generateHashFromOneThread(startOffset int, numTreesToHash int, mapUpdate bool, updateMapByChan bool, channel chan [2]int) {
	defer waitGroup.Done()

	lastToHash := startOffset + numTreesToHash
	totalTrees := len(trees)
	for id := startOffset; id < lastToHash; id = id + 1 {
		if id < totalTrees {
			hash := generateSingleTreeHash(trees[id])
			if mapUpdate {
				generateHashGoups(updateMapByChan, channel, id, hash)
			}
		}
	}
}

func generateSingleTreeHash(bst *BST) int {
	var inOrderVals []int
	bst.InOrderTraverse(func(val int) {
		inOrderVals = append(inOrderVals, val)
	})

	numVals := len(inOrderVals)
	hash := 1
	for i := 0; i < numVals; i = i + 1 {
		newValue := inOrderVals[i] + 2
		hash = (hash*newValue + newValue) % 4222234741
	}
	bst.hash = hash

	return hash
}

func generateHashGoups(updateMapByChan bool, channel chan [2]int, id int, hash int) {
	if updateMapByChan {
		item := [2]int{id, hash}
		channel <- item

		lock.Lock()
		numHashed++
		if numHashed == len(trees) {
			close(channel)
		}
		lock.Unlock()
	} else {
		hashIDMap.Set(hash, id)
	}
}

func fillMapWithChannel(channel chan [2]int) {
	defer waitGroup.Done()

	for elem := range channel {
		id := elem[0]
		hash := elem[1]
		hashIDMap.Set(hash, id)
	}
}

func printGroups(hashGroups bool) {
	grpNum := 0
	for key, val := range hashIDMap.items {
		ids := val.items
		if len(ids) > 1 {
			if hashGroups {
				fmt.Printf("%d: ", key)
			} else {
				fmt.Print("group ", grpNum, ": ")
				grpNum++
			}

			for _, elem := range ids {
				fmt.Print(elem, " ")
			}
			fmt.Println()
		}
	}
}

func compareTreesByBuffer(compWorkers int) {
	workBuffer = NewConcBuffer(uint(compWorkers))
	initializeEqualityArray(len(trees))

	for i := 0; i < compWorkers; i = i + 1 {
		waitGroup.Add(1)
		go bufferTreeEquality()
	}

	for _, val := range hashIDMap.items {
		if len(val.items) <= 1 {
			id := val.items[0]
			equal[id][id] = true
		} else {
			numTrees := len(val.items)
			for i := 0; i < numTrees-1; i = i + 1 {
				for j := i + 1; j < numTrees; j = j + 1 {
					workIds := [2]int{val.items[i], val.items[j]}
					// assert(workIds[0] != workIds[1], "sending the same tree to compare!")
					_ = workBuffer.AddWork(workIds)
					// assert(err == nil, "add work returned an error, check conditions and locks")
				}
			}
		}
	}
	workBuffer.CloseBuffer()
}

func initializeEqualityArray(numTrees int) {
	array := make([][]bool, numTrees)
	for i := range array {
		array[i] = make([]bool, numTrees)
	}

	equal = array
}

func bufferTreeEquality() {
	defer waitGroup.Done()

	for !workBuffer.IsClosed() {
		workIDs, err := workBuffer.RemoveWork()
		if err != nil {
			break
		}
		assert(workIDs[0] != workIDs[1], "comparing the same tree!")

		bst1 := trees[workIDs[0]]
		bst2 := trees[workIDs[1]]

		areTreesEqual(bst1, bst2, workIDs[0], workIDs[1], true)
	}

	for !workBuffer.isEmpty() {
		workIDs, err := workBuffer.RemoveWork()
		if err != nil {
			break
		}
		assert(workIDs[0] != workIDs[1], "comparing the same tree!")

		bst1 := trees[workIDs[0]]
		bst2 := trees[workIDs[1]]

		areTreesEqual(bst1, bst2, workIDs[0], workIDs[1], true)
	}
}

func areTreesEqual(bst1 *BST, bst2 *BST, id1 int, id2 int, useBuffer bool) int {
	if !useBuffer {
		defer waitGroup.Done()
	}
	var inOrderOne []int
	bst1.InOrderTraverse(func(val int) {
		inOrderOne = append(inOrderOne, val)
	})

	isEqual := 0
	index := 0

	bst2.InOrderTraverse(func(val int) {
		if inOrderOne[index] != val {
			isEqual++
			return
		}
		index++
	})

	if isEqual == 0 {
		equal[id1][id2] = true
		equal[id2][id1] = true
	} else {
		equal[id1][id2] = false
		equal[id2][id1] = false
	}
	return isEqual
}

func sequentialHashCompute(mapUpdate bool) {
	if mapUpdate {
		mutex := &sync.RWMutex{}
		actualMap := make(map[int]*ConcurrentSlice)
		hashIDMap = &ConcurrentMap{false, mutex, actualMap}
	}

	for i := 0; i < len(trees); i = i + 1 {
		hash := generateSingleTreeHash(trees[i])
		if mapUpdate {
			hashIDMap.Set(hash, i)
		}
	}
}

func sequentialTreeComp() {
	initializeEqualityArray(len(trees))
	for _, val := range hashIDMap.items {
		if len(val.items) <= 1 {
			id := val.items[0]
			equal[id][id] = true
		} else {
			numIDs := len(val.items)
			for i := 0; i < numIDs-1; i = i + 1 {
				for j := i + 1; j < numIDs; j = j + 1 {
					id1 := val.items[i]
					id2 := val.items[j]
					bst1 := trees[id1]
					bst2 := trees[id2]
					areTreesEqual(bst1, bst2, id1, id2, true)
				}
			}
		}
	}
}

func createEqualGroups() {
	for _, val := range hashIDMap.items {
		if len(val.items) != 1 {
			numIDs := len(val.items)
			for i := 0; i < numIDs; i = i + 1 {
				atLeastOneEqual := false
				numUnequal := 0
				for j := 0; j < numIDs; j = j + 1 {
					if i != j {
						if equal[val.items[i]][val.items[j]] {
							atLeastOneEqual = true
						} else {
							numUnequal++
						}
					}
				}
				if !atLeastOneEqual {
					val.items = removeElem(val.items, i)
				}
			}
		}
	}
}

func removeElem(slice []int, s int) []int {
	return append(slice[:s], slice[s+1:]...)
}

func compareTrees(numTrees int) {
	initializeEqualityArray(numTrees)
	for _, val := range hashIDMap.items {
		if len(val.items) == 1 {
			id := val.items[0]
			equal[id][id] = true
		} else {
			numTrees := len(val.items)
			for i := 0; i < numTrees-1; i = i + 1 {
				for j := i + 1; j < numTrees; j = j + 1 {
					id1 := val.items[i]
					id2 := val.items[j]
					bst1 := trees[id1]
					bst2 := trees[id2]
					waitGroup.Add(1)
					go areTreesEqual(bst1, bst2, id1, id2, false)
				}
			}
		}
	}
}
