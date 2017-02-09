package concurrent_map

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync"
	"testing"
)

type TestData struct {
	value string
}

func TestCMap_Count(t *testing.T) {
	cmap := NewCMap(10)
	t1 := TestData{"t1"}
	t2 := TestData{"t2"}
	cmap.Put("t1", t1)
	cmap.Put("t2", t2)
	assert.Equal(t, 2, cmap.Count())
}

func TestCMap_GetPut(t *testing.T) {
	cmap := NewCMap(10)
	t1 := TestData{"t1"}
	t2 := TestData{"t2"}
	cmap.Put("t1", t1)
	cmap.Put("t2", t2)
	rt1, ok1 := cmap.Get("t1")
	assert.Equal(t, t1, rt1)
	assert.Equal(t, true, ok1)
	rt2, ok2 := cmap.Get("t2")
	assert.Equal(t, t2, rt2)
	assert.Equal(t, true, ok2)
	rt3, ok3 := cmap.Get("t3")
	assert.Equal(t, nil, rt3)
	assert.Equal(t, false, ok3)
}

func TestCMap_Remove(t *testing.T) {
	cmap := NewCMap(10)
	cmap.Put("test", 123)
	assert.Equal(t, true, cmap.IsExist("test"))
	assert.Nil(t, cmap.Remove("test"))
	assert.Error(t, cmap.Remove("test"))
	assert.Error(t, cmap.Remove("test1"))
}

func TestConcurrent1(t *testing.T) {
	tasks := make(chan int, 100)
	cmap := NewCMap(10)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		go func(tasks chan int, m CMapInterface, wg *sync.WaitGroup) {
			defer wg.Done()
			mult := <-tasks
			for i := 0; i < 1000; i++ {
				m.Put(strconv.Itoa(mult*1000+i), mult*1000+i)
			}
		}(tasks, cmap, &wg)
		wg.Add(1)
	}
	for i := 0; i < 100; i++ {
		tasks <- i
	}
	wg.Wait()
	assert.Equal(t, cmap.Count(), 100*1000)
	for i := 0; i < 100*1000; i++ {
		v, ok := cmap.Get(strconv.Itoa(i))
		require.Equal(t, true, ok)
		require.Equal(t, i, v)
	}
}

func TestConcurrent2(t *testing.T) {
	cmap := NewCMap(10)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		go func(m CMapInterface, wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				m.Put(strconv.Itoa(i), i)
			}
		}(cmap, &wg)
		wg.Add(1)
	}
	wg.Wait()
	assert.Equal(t, cmap.Count(), 1000)
	for i := 0; i < 1000; i++ {
		v, ok := cmap.Get(strconv.Itoa(i))
		require.Equal(t, true, ok)
		require.Equal(t, i, v)
	}
}
