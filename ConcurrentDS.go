package main

import (
	"errors"
	"sync"
)

func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

// ConcurrentSlice conc slice
type ConcurrentSlice struct {
	synchronize bool
	*sync.RWMutex
	// items []interface{}
	items []int
}

// ConcSliceItem item
type ConcSliceItem struct {
	index int
	// value interface{}
	value int
}

// Append ..
func (c_slice *ConcurrentSlice) Append(item int) {
	if c_slice.synchronize {
		c_slice.Lock()
		defer c_slice.Unlock()
	}

	c_slice.items = append(c_slice.items, item)
}

// Iterate ..
func (c_slice *ConcurrentSlice) Iterate() <-chan ConcSliceItem {
	channel := make(chan ConcSliceItem)

	iterate := func() {
		if c_slice.synchronize {
			c_slice.Lock()
			defer c_slice.Unlock()
		}
		for index, value := range c_slice.items {
			channel <- ConcSliceItem{index, value}
		}
		close(channel)
	}
	// can just call it here
	go iterate()

	return channel
}

// ConcurrentMap ..
type ConcurrentMap struct {
	synchronize bool
	*sync.RWMutex
	items map[int]*ConcurrentSlice
}

// ConcMapItem ..
type ConcMapItem struct {
	key   int
	value *ConcurrentSlice
}

// Set ..
func (c_map *ConcurrentMap) Set(key int, value int) {
	if c_map.synchronize {
		c_map.Lock()
		defer c_map.Unlock()
	}

	if c_map.items[key] == nil {
		c_map.items[key] = &ConcurrentSlice{}
	}

	c_map.items[key].Append(value)

}

// Get ..
func (c_map *ConcurrentMap) Get(key int) (*ConcurrentSlice, bool) {
	if c_map.synchronize {
		c_map.RLock()
		defer c_map.RUnlock()
	}

	value, ok := c_map.items[key]

	return value, ok
}

// Iter ..
func (c_map *ConcurrentMap) Iter() <-chan ConcMapItem {
	channel := make(chan ConcMapItem)

	iterate := func() {
		if c_map.synchronize {
			c_map.Lock()
			defer c_map.Unlock()
		}

		for k, v := range c_map.items {
			channel <- ConcMapItem{k, v}
		}
		close(channel)
	}
	go iterate()

	return channel
}

// Node ..
type Node struct {
	value int
	left  *Node
	right *Node
}

// BST ..
type BST struct {
	root *Node
	// *sync.RWMutex
	hash int
}

// Insert insert method on a tree
func (bst *BST) Insert(value int) {
	// bst.Lock()
	// defer bst.Unlock()
	newNode := &Node{value, nil, nil}

	if bst.root == nil {
		bst.root = newNode
	} else {
		insertNode(bst.root, newNode)
	}
}

// Remove ..
func (bst *BST) Remove(value int) {
	// bst.Lock()
	// defer bst.Unlock()
	remove(bst.root, value)
}

// InOrderTraverse ..
func (bst *BST) InOrderTraverse(fn func(int)) {
	// bst.RLock()
	// defer bst.RUnlock()
	inOrderTraverse(bst.root, fn)
}

// the insert utility function
func insertNode(node, newNode *Node) {
	if newNode.value < node.value {
		if node.left == nil {
			node.left = newNode
		} else {
			insertNode(node.left, newNode)
		}
	} else if newNode.value > node.value {
		if node.right == nil {
			node.right = newNode
		} else {
			insertNode(node.right, newNode)
		}
	}
}

func inOrderTraverse(node *Node, fn func(int)) {
	if node != nil {
		inOrderTraverse(node.left, fn)
		fn(node.value)
		inOrderTraverse(node.right, fn)
	}
}

func remove(node *Node, value int) *Node {
	if node == nil {
		return nil
	}
	if value < node.value {
		node.left = remove(node.left, value)
		return node
	}
	if value > node.value {
		node.right = remove(node.right, value)
		return node
	}

	// this node needs to be removed
	if node.left == nil && node.right == nil {
		node = nil
		return nil
	}
	if node.right == nil {
		node = node.right
		return node
	}
	if node.left == nil {
		node = node.left
		return node
	}

	smallestRightSide := node.right
	for {
		if smallestRightSide != nil && smallestRightSide.left != nil {
			smallestRightSide = smallestRightSide.left
		} else {
			break
		}
	}
	node.value = smallestRightSide.value
	node.right = remove(node.right, node.value)
	return node
}

// ConcBuffer ..
type ConcBuffer struct {
	closed   bool
	bufLock  *sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	bufferQ  *BufferQ
}

// AddWork ..
func (concBuf *ConcBuffer) AddWork(data [2]int) error {
	concBuf.bufLock.Lock()
	defer concBuf.bufLock.Unlock()

	// assert(!concBuf.closed, "adding work in closed buffer")

	for concBuf.bufferQ.IsFull() {
		// assert(concBuf.bufferQ.size == concBuf.bufferQ.maxSize, "buffer not full but sender blocked")
		concBuf.notFull.Wait()
	}

	err := concBuf.bufferQ.AddWork(data)
	// assert(concBuf.bufferQ.size != 0, "buffer size 0 after adding work")
	// assert(err == nil, "add in full buffer")

	concBuf.notEmpty.Signal()

	return err
}

// RemoveWork ..
func (concBuf *ConcBuffer) RemoveWork() ([2]int, error) {
	concBuf.bufLock.Lock()
	defer concBuf.bufLock.Unlock()

	for concBuf.bufferQ.IsEmpty() {
		if concBuf.closed {
			return [2]int{}, errors.New("all work done")
		}
		// assert(!concBuf.closed, "buffer closed and empty")
		// assert(concBuf.bufferQ.size == 0, "buffer not empty but receiver blocked")
		concBuf.notEmpty.Wait()
	}

	data, err := concBuf.bufferQ.RemoveWork()
	// assert(err == nil, "remove on empty buffer")
	// assert(currSize > concBuf.bufferQ.size, "size not updated after remove work")
	concBuf.notFull.Signal()

	return data, err
}

// Size ..
func (concBuf *ConcBuffer) Size() uint {
	concBuf.bufLock.Lock()
	defer concBuf.bufLock.Unlock()

	return concBuf.bufferQ.size
}

// CloseBuffer ..
func (concBuf *ConcBuffer) CloseBuffer() {
	concBuf.bufLock.Lock()
	defer concBuf.bufLock.Unlock()

	concBuf.closed = true
	concBuf.notEmpty.Broadcast()
}

// IsClosed ..
func (concBuf *ConcBuffer) IsClosed() bool {
	concBuf.bufLock.Lock()
	defer concBuf.bufLock.Unlock()

	return concBuf.closed
}

func (concBuf *ConcBuffer) isEmpty() bool {
	concBuf.bufLock.Lock()
	defer concBuf.bufLock.Unlock()

	return concBuf.bufferQ.size == 0
}

// NewConcBuffer ..
func NewConcBuffer(maxSize uint) *ConcBuffer {
	concBuf := ConcBuffer{}

	concBuf.bufLock = &sync.Mutex{}
	concBuf.notEmpty = sync.NewCond(concBuf.bufLock)
	concBuf.notFull = sync.NewCond(concBuf.bufLock)

	concBuf.bufferQ = &BufferQ{}
	concBuf.bufferQ.size = 0
	concBuf.bufferQ.head = nil
	concBuf.bufferQ.tail = nil

	concBuf.bufferQ.maxSize = maxSize
	concBuf.closed = false

	return &concBuf
}

// QNode ..
type QNode struct {
	data [2]int
	prev *QNode
	next *QNode
}

// BufferQ ..
type BufferQ struct {
	head    *QNode
	tail    *QNode
	size    uint
	maxSize uint
}

// CreateWork ..
func (bufferQ *BufferQ) CreateWork(data [2]int) *QNode {
	newWork := &QNode{}
	newWork.data = data
	newWork.next = nil
	newWork.prev = nil

	return newWork
}

// AddWork ..
func (bufferQ *BufferQ) AddWork(data [2]int) error {
	if bufferQ.size >= bufferQ.maxSize {
		err := errors.New("Queue full")
		return err
	}

	if bufferQ.size == 0 {
		work := bufferQ.CreateWork(data)
		bufferQ.head = work
		bufferQ.tail = work
		bufferQ.size++
		return nil
	}

	currentTail := bufferQ.tail
	newTail := bufferQ.CreateWork(data)
	newTail.prev = currentTail
	currentTail.next = newTail
	bufferQ.tail = newTail

	bufferQ.size++

	return nil

}

// RemoveWork ..
func (bufferQ *BufferQ) RemoveWork() ([2]int, error) {
	if bufferQ.size == 0 {
		err := errors.New("remove on empty queue")
		return [2]int{}, err
	}

	currentHead := bufferQ.head
	newHead := currentHead.next
	if newHead != nil {
		newHead.prev = nil
	}

	bufferQ.size--
	if bufferQ.size == 0 {
		bufferQ.head = nil
		bufferQ.tail = nil
	}
	bufferQ.head = newHead

	return currentHead.data, nil
}

// IsEmpty ..
func (bufferQ *BufferQ) IsEmpty() bool {
	return bufferQ.size == 0
}

// IsFull ..
func (bufferQ *BufferQ) IsFull() bool {
	return bufferQ.size >= bufferQ.maxSize
}
