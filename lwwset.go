package systemprocessing

import (
	"errors"
	"log"
	"sync"
)

// CRDT represent Conflict Free Replicated Data Types
type CRDT interface {
	Add(interface{}, int64)
	Lookup(interface{}) bool
	Remove(interface{}, int64)
	Merge(CRDT) error
}

// LWWSet implements Last Write Wins CRDT
type LWWSet struct {
	lock      sync.RWMutex
	addSet    map[interface{}]int64
	removeSet map[interface{}]int64
}

// NewLWWSet create a new LWW-Element-Set
func NewLWWSet() CRDT {
	return &LWWSet{
		addSet:    map[interface{}]int64{},
		removeSet: map[interface{}]int64{},
	}
}

// Add an element with its unix timestamp.
func (s *LWWSet) Add(element interface{}, ts int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.addSet[element] = ts
}

// Lookup an element.
func (s *LWWSet) Lookup(element interface{}) bool {
	s.lock.RLock()
	defer s.lock.Unlock()
	tsAdd, ok := s.addSet[element]
	tsRemove := s.removeSet[element]

	if ok && tsAdd >= tsRemove {
		return true
	}

	return false
}

// Remove an element with its unix timestamp
func (s *LWWSet) Remove(element interface{}, ts int64) {
	if s.Lookup(element) {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.removeSet[element] = ts
	}
}

// Merge current set with another CRDT set
func (s *LWWSet) Merge(crdt CRDT) error {
	lww, ok := (crdt).(*LWWSet)
	if !ok {
		log.Fatal("CRDT is not of same type")
		return errors.New("invalid CRDT type")
	}

	lww.lock.RLock()
	defer lww.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()

	for element, timestamp := range lww.addSet {
		ts, ok := s.addSet[element]

		if !ok || (ok && ts < timestamp) {
			s.addSet[element] = timestamp
		}
	}

	// merging the removeSet of lww to s with keeping the latest timestamp
	for element, timestamp := range lww.removeSet {

		// get the element and its timestamp in removeSet of s
		ts, ok := s.removeSet[element]

		if !ok || (ok && ts < timestamp) {
			s.removeSet[element] = timestamp
		}
	}
	return nil
}
