package buffer

import (
	"fmt"
	"starix-crawler/errors"
	"sync"
	"sync/atomic"
)

// Pool 代表数据缓冲池的接口类型。
type Pool interface {
	// BufferCap 用于获取池中缓冲器的统一容量。
	BufferCap() uint32
	// MaxBufferNumber 用于获取池中缓冲器的最大数量。
	MaxBufferNumber() uint32
	// BufferNumber 用于获取池中缓冲器的数量。
	BufferNumber() uint32
	// Total 用于获取缓冲池中数据的总数。
	Total() uint64
	// Put 用于向缓冲池放入数据。
	// 注意！本方法应该是阻塞的。
	// 若缓冲池已关闭则会直接返回非nil的错误值。
	Put(datum interface{}) error
	// Get 用于从缓冲池获取数据。
	// 注意！本方法应该是阻塞的。
	// 若缓冲池已关闭则会直接返回非nil的错误值。
	Get() (datum interface{}, err error)
	// Close 用于关闭缓冲池。
	// 若缓冲池之前已关闭则返回false，否则返回true。
	Close() bool
	// Closed 用于判断缓冲池是否已关闭。
	Closed() bool
}

// myPool 代表数据缓冲池接口的实现类型。
type myPool struct {
	// bufferCap 代表缓冲器的统一容量。
	bufferCap uint32
	// maxBufferNumber 代表缓冲器的最大数量。
	maxBufferNumber uint32
	// bufferNumber 代表缓冲器的实际数量。
	bufferNumber uint32
	// total 代表池中数据的总数。
	total uint64
	// bufCh 代表存放缓冲器的通道。
	bufCh chan Buffer
	// closed 代表缓冲池的关闭状态：0-未关闭；1-已关闭。
	closed uint32
	// lock 代表保护内部共享资源的读写锁。
	rwlock sync.RWMutex
}

// NewPool 用于创建一个数据缓冲池。
// 参数bufferCap代表池内缓冲器的统一容量。
// 参数maxBufferNumber代表池中最多包含的缓冲器的数量。
func NewPool(bufferCap uint32, maxBufferNumber uint32) (Pool, error) {
	if bufferCap == 0 {
		errMsg := fmt.Sprintf("illegal buffer cap for buffer pool: %d", bufferCap)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	if maxBufferNumber == 0 {
		errMsg := fmt.Sprintf("illegal max buffer number for buffer pool: %d", maxBufferNumber)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	bufCh := make(chan Buffer, maxBufferNumber)
	buf, _ := NewBuffer(bufferCap)
	bufCh <- buf
	return &myPool{
		bufferCap:       bufferCap,
		maxBufferNumber: maxBufferNumber,
		bufferNumber:    1,
		bufCh:           bufCh,
	}, nil
}

func (pool *myPool) BufferCap() uint32 {
	return pool.bufferCap
}

func (pool *myPool) MaxBufferNumber() uint32 {
	return pool.maxBufferNumber
}

func (pool *myPool) BufferNumber() uint32 {
	return atomic.LoadUint32(&pool.bufferNumber)
}

func (pool *myPool) Total() uint64 {
	return atomic.LoadUint64(&pool.total)
}

func (pool *myPool) Put(datum interface{}) (err error) {
	if pool.Closed() {
		return ErrClosedBufferPool
	}
	// Put失败次数（已满）
	var count uint32
	// 最大失败次数
	maxCount := pool.BufferNumber() * 5
	var ok bool
	for buf := range pool.bufCh {
		ok, err = pool.putData(buf, datum, &count, maxCount)
		if ok || err != nil {
			break
		}
	}
	return
}

func (pool *myPool) putData(buf Buffer, datum interface{}, count *uint32, maxCount uint32) (ok bool, err error) {
	if pool.Closed() {
		return false, ErrClosedBufferPool
	}
	// 方法执行结束时，归还缓冲器
	defer func() {
		// 使用读锁，避免并发情况下有可能向已关闭的buffCh发送值
		// 如主动调用Close()方法与归还缓冲器这两个操作并发的场景
		// 此处使用读锁的前提是Close方法内使用了写锁
		pool.rwlock.RLock()
		if pool.Closed() {
			// 缓冲池已关闭，不归还缓冲器，并且缓冲器数量-1
			atomic.AddUint32(&pool.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		} else {
			pool.bufCh <- buf
		}
		pool.rwlock.RUnlock()
	}()

	ok, err = buf.Put(datum)
	if err != nil {
		return
	}
	if ok {
		atomic.AddUint64(&pool.total, 1)
		return
	}
	// 若因缓冲器已满而未放入数据，就递增已满计数
	*count++
	// 如果缓冲器放入数据失败次数（已满计数）达到阈值
	// 并且缓冲器数量未达到最大值
	// 则尝试新增一个缓冲器加入缓冲池
	if *count >= maxCount && pool.BufferNumber() < pool.MaxBufferNumber() {
		pool.rwlock.Lock()
		// 双检锁
		if *count >= maxCount && pool.BufferNumber() < pool.MaxBufferNumber() {
			if pool.Closed() {
				pool.rwlock.Unlock()
				return
			}
			newBuf, _ := NewBuffer(pool.bufferCap)
			newBuf.Put(datum)
			pool.bufCh <- newBuf
			atomic.AddUint32(&pool.bufferNumber, 1)
			atomic.AddUint64(&pool.total, 1)
			ok = true
		}
		pool.rwlock.Unlock()
		// 不管新增成功与否，都清零失败计数，减少不必要的新增缓冲器操作
		*count = 0
	}
	return
}

func (pool *myPool) Get() (datum interface{}, err error) {
	if pool.Closed() {
		return nil, ErrClosedBufferPool
	}
	// Get失败次数（已空）
	var count uint32
	// 最大失败次数
	maxCount := pool.BufferNumber() * 10
	for buf := range pool.bufCh {
		datum, err = pool.getData(buf, &count, maxCount)
		if datum != nil || err != nil {
			break
		}
	}
	return
}

func (pool *myPool) getData(buf Buffer, count *uint32, maxCount uint32) (datum interface{}, err error) {
	if pool.Closed() {
		return nil, ErrClosedBufferPool
	}
	defer func() {
		// 当获取数据失败次数达到阈值，同时当前缓冲器已空，且缓冲器数量大于1，
		// 则减少缓冲器数量（直接关闭当前缓冲器，并且不归还）
		if *count >= maxCount && buf.Len() == 0 && pool.bufferNumber > 1 {
			pool.rwlock.Lock()
			// 双检锁
			if *count >= maxCount && buf.Len() == 0 && pool.bufferNumber > 1 {
				buf.Close()
				atomic.AddUint32(&pool.bufferNumber, ^uint32(0))
			}
			pool.rwlock.Unlock()
			*count = 0
			return
		}

		// 使用读锁，避免并发情况下有可能向已关闭的buffCh发送值
		// 如主动调用Close()方法与归还缓冲器这两个操作并发的场景
		// 此处使用读锁的前提是Close方法内使用了写锁
		pool.rwlock.RLock()
		if pool.Closed() {
			// 缓冲池已关闭，不归还缓冲器，并且缓冲器数量-1
			atomic.AddUint32(&pool.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		} else {
			pool.bufCh <- buf
		}
		pool.rwlock.RUnlock()
	}()

	datum, err = buf.Get()
	if err != nil {
		return
	}
	if datum != nil {
		atomic.AddUint64(&pool.total, ^uint64(0))
		return
	}

	// err和datum同时为空，说明缓冲器已空，更新计数
	*count++
	return
}

func (pool *myPool) Close() bool {
	if !atomic.CompareAndSwapUint32(&pool.closed, 0, 1) {
		return false
	}
	pool.rwlock.Lock()
	defer pool.rwlock.Unlock()
	close(pool.bufCh)
	for buf := range pool.bufCh {
		buf.Close()
	}
	return true
}

func (pool *myPool) Closed() bool {
	return atomic.LoadUint32(&pool.closed) == 1
}
