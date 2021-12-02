package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
)

var (
	// ErrClosed performs any operation on the closed client will return this error.
	ErrClosed = errors.New("redis: client is closed")

	// ErrPoolTimeout timed out waiting to get a connection from the connection pool.
	ErrPoolTimeout = errors.New("redis: connection pool timeout")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats 连接池状态信息
// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // number of total connections in the pool
	IdleConns  uint32 // number of idle connections in the pool
	StaleConns uint32 // number of stale connections removed from the pool
}

// Pooler 连接池接口
type Pooler interface {
	NewConn(context.Context) (*Conn, error)
	CloseConn(*Conn) error

	Get(context.Context) (*Conn, error)
	Put(context.Context, *Conn)
	Remove(context.Context, *Conn, error)

	Len() int
	IdleLen() int
	Stats() *Stats

	Close() error
}

// Options 配置
type Options struct {
	Dialer  func(context.Context) (net.Conn, error) // 拨号器
	OnClose func(*Conn) error                       // 关闭事件处理

	PoolFIFO           bool // 是否是先进先出
	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type lastDialErrorWrap struct {
	err error
}

type ConnPool struct {
	opt *Options

	dialErrorsNum uint32 // atomic

	lastDialError atomic.Value

	queue chan struct{} // 并发控制

	connsMu      sync.Mutex
	conns        []*Conn // 所有连接的连接对象 包含不是池连接
	idleConns    []*Conn // 空闲连接
	poolSize     int     // 分配出去的池连接个数
	idleConnsLen int

	stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
}

var _ Pooler = (*ConnPool)(nil)

func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize), // 最多允许opt.PoolSize个协程处理
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
	}

	// 初始化一定的空闲连接
	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

// 保持一定的空闲连接
// 调用者 持有锁
func (p *ConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		p.poolSize++
		p.idleConnsLen++

		go func() {
			err := p.addIdleConn()
			if err != nil && err != ErrClosed {
				p.connsMu.Lock()
				p.poolSize--
				p.idleConnsLen--
				p.connsMu.Unlock()
			}
		}()
	}
}

// 新建一个空闲连接
func (p *ConnPool) addIdleConn() error {
	cn, err := p.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		_ = cn.Close()
		return ErrClosed
	}

	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	return nil
}

// NewConn 新建一个不归还池的连接
func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.newConn(ctx, false)
}

// 新建连接
func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		_ = cn.Close()
		return nil, ErrClosed
	}

	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}

	return cn, nil
}

// 拨号建立新连接
func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			// 拨号错误数等于池容量 开始链路探测
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn)
	cn.pooled = pooled
	return cn, nil
}

// 检测网络是否可达
func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

// 设置最后一次拨号错误
func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

// 获取最后一次拨号错误
func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get 从池中获取一个连接
// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	// 尝试获取执行权限
	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}

	for {
		p.connsMu.Lock()
		cn, err := p.popIdle()
		p.connsMu.Unlock()

		if err != nil {
			return nil, err
		}

		if cn == nil {
			break
		}

		// 判断是否是活跃连接
		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	// 新建连接
	newcn, err := p.newConn(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

// 获取执行权限
func (p *ConnPool) getTurn() {
	p.queue <- struct{}{}
}

// 等待获取执行权限
func (p *ConnPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	// 不能立即获取执行权限
	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	select {
	case <-ctx.Done():
		// 取消上下文
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		// 可以获得执行权限
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		// 等待超时
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

// 释放执行权限
func (p *ConnPool) freeTurn() {
	<-p.queue
}

// 弹出空闲连接
func (p *ConnPool) popIdle() (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	n := len(p.idleConns)
	if n == 0 {
		return nil, nil
	}

	var cn *Conn
	if p.opt.PoolFIFO {
		cn = p.idleConns[0]
		// 防止重复申请内存
		copy(p.idleConns, p.idleConns[1:])
		p.idleConns = p.idleConns[:n-1]
	} else {
		idx := n - 1
		cn = p.idleConns[idx]
		p.idleConns = p.idleConns[:idx]
	}
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn, nil
}

// Put 向池归还连接 释放执行权限
func (p *ConnPool) Put(ctx context.Context, cn *Conn) {
	if cn.rd.Buffered() > 0 {
		// 还有未写入的数据 脏数据移除连接
		internal.Logger.Printf(ctx, "Conn has unread data")
		p.Remove(ctx, cn, BadConnError{})
		return
	}

	if !cn.pooled {
		// 不需要归还池 移除连接
		p.Remove(ctx, cn, nil)
		return
	}

	// 归还池
	p.connsMu.Lock()
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	// 释放执行权限
	p.freeTurn()
}

// Remove 移除连接 释放执行权限 关闭连接
func (p *ConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

// CloseConn 移除连接 关闭连接
func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

// 连接切片中移除连接
func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

// 连接切片中移除cn
// 调用者持有锁
func (p *ConnPool) removeConn(cn *Conn) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				// 是池连接 就保持最小空闲连接
				p.poolSize--
				p.checkMinIdleConns()
			}
			return
		}
	}
}

// 关闭连接
func (p *ConnPool) closeConn(cn *Conn) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

func (p *ConnPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

// Filter 对所有连接执行判断 fn 为 true 则关闭连接
func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	var firstErr error
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Close 关闭连接池
func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	close(p.closedCh)

	var firstErr error
	p.connsMu.Lock()
	// 关闭池中所有连接
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

// 定时清理不活跃的连接
func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleConns()
			if err != nil {
				internal.Logger.Printf(context.Background(), "ReapStaleConns failed: %s", err)
				continue
			}
		case <-p.closedCh:
			return
		}
	}
}

// ReapStaleConns 清理队首所有不活跃的连接
func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()

		p.freeTurn()

		if cn != nil {
			_ = p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

// 尝试移除不活跃的队首连接
func (p *ConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	// 清理队首连接
	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		// 活跃就不处理
		return nil
	}

	// 不活跃就移除
	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn)

	return cn
}

// 判断连接是否需要清理
func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		// 不活跃
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		// 连接太旧
		return true
	}

	return false
}
