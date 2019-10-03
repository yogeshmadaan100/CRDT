package systemprocessing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLWWSetLogic(t *testing.T) {
	set1 := NewLWWSet()

	element1 := 1
	timestamp1_1 := int64(1)
	timestamp1_2 := int64(112)
	timestamp1_3 := int64(130)

	element2 := 2
	timetamp2_1 := int64(2)
	timetamp2_2 := int64(22)

	set1.Add(element1, timestamp1_1)
	assert.Equal(t, true, set1.Lookup(element1))
	assert.Equal(t, false, set1.Lookup(element2))

	// the bias case, should the element should still exist
	set1.Remove(element1, timestamp1_1)
	assert.Equal(t, true, set1.Lookup(element1))

	// element1 should not exist anymore
	set1.Remove(element1, timestamp1_2)
	assert.Equal(t, false, set1.Lookup(element1))

	set2 := NewLWWSet()

	set2.Add(element2, timetamp2_1)
	assert.Equal(t, true, set2.Lookup(element2))

	// set1 merge set2
	err := set1.Merge(set2)
	assert.NoError(t, err)
	assert.Equal(t, false, set1.Lookup(element1))
	assert.Equal(t, true, set1.Lookup(element2))

	// set2 add the latest element1, after merging, element1 should be back again to set1
	set2.Add(element1, timestamp1_3)
	err = set1.Merge(set2)
	assert.NoError(t, err)
	assert.Equal(t, true, set1.Lookup(element1))

	// remove the element from set2, after merging, element2 should not exist anymore
	set2.Remove(element2, timetamp2_2)
	err = set1.Merge(set2)
	assert.NoError(t, err)
	assert.Equal(t, false, set1.Lookup(element2))
}
