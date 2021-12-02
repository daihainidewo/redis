package pool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

const (
	stateDefault = 0
	stateInited  = 1
	stateClosed  = 2
)

type BadConnError struct {
	wrapped error
}

var _ error = (*BadConnError)(nil)

func (e BadConnError) Error() string {
	s := "redis: Conn is in a bad state"
	if e.wrapped != nil {
		s += ": " + e.wrapped.Error()
	}
	return s
}

func (e BadConnError) Unwrap() error {
	return e.wrapped
}

//------------------------------------------------------------------------------

// StickyConnPool 用于有监视的事务执行
// 当key改变主动断开连接
type StickyConnPool struct {
	pool   Pooler
	// 引用计数
	shared int32 // atomic

	state uint32 // atomic
	// 单个连接的通道数据
	ch    chan *Conn

	_badConnError atomic.Value
}

var _ Pooler = (*StickyConnPool)(nil)

func NewStickyConnPool(pool Pooler) *StickyConnPool {
	p, ok := pool.(*StickyConnPool)
	if !ok {
		p = &StickyConnPool{
			pool: pool,
			ch:   make(chan *Conn, 1),
		}
	}
	atomic.AddInt32(&p.shared, 1)
	return p
}

func (p *StickyConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.pool.NewConn(ctx)
}

func (p *StickyConnPool) CloseConn(cn *Conn) error {
	return p.pool.CloseConn(cn)
}

// Get 获取连接
func (p *StickyConnPool) Get(ctx context.Context) (*Conn, error) {
	// In worst case this races with Close which is not a very common operation.
	for i := 0; i < 1000; i++ {
		switch atomic.LoadUint32(&p.state) {
		case stateDefault:
			cn, err := p.pool.Get(ctx)
			if err != nil {
				return nil, err
			}
			// 乐观装换状态
			if atomic.CompareAndSwapUint32(&p.state, stateDefault, stateInited) {
				return cn, nil
			}
			// 转换状态失败就移除连接
			p.pool.Remove(ctx, cn, ErrClosed)
		case stateInited:
			if err := p.badConnError(); err != nil {
				return nil, err
			}
			cn, ok := <-p.ch
			if !ok {
				return nil, ErrClosed
			}
			return cn, nil
		case stateClosed:
			return nil, ErrClosed
		default:
			panic("not reached")
		}
	}
	return nil, fmt.Errorf("redis: StickyConnPool.Get: infinite loop")
}

func (p *StickyConnPool) Put(ctx context.Context, cn *Conn) {
	defer func() {
		if recover() != nil {
			// p.ch 关闭就释放连接
			p.freeConn(ctx, cn)
		}
	}()
	p.ch <- cn
}

// 归还连接
func (p *StickyConnPool) freeConn(ctx context.Context, cn *Conn) {
	if err := p.badConnError(); err != nil {
		// 有错误就移除
		p.pool.Remove(ctx, cn, err)
	} else {
		// 没有就放回池中
		p.pool.Put(ctx, cn)
	}
}

// Remove 能放回就放回 不能放回就从池中移除
func (p *StickyConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	defer func() {
		if recover() != nil {
			// p.ch 关闭就移除连接
			p.pool.Remove(ctx, cn, ErrClosed)
		}
	}()
	// 封装连接错误
	p._badConnError.Store(BadConnError{wrapped: reason})
	p.ch <- cn
}

// Close 关闭池
func (p *StickyConnPool) Close() error {
	// 确定引用计数
	if shared := atomic.AddInt32(&p.shared, -1); shared > 0 {
		return nil
	}

	for i := 0; i < 1000; i++ {
		state := atomic.LoadUint32(&p.state)
		if state == stateClosed {
			return ErrClosed
		}
		// 乐观切换状态
		if atomic.CompareAndSwapUint32(&p.state, state, stateClosed) {
			close(p.ch)
			cn, ok := <-p.ch
			if ok {
				// 归还连接
				p.freeConn(context.TODO(), cn)
			}
			return nil
		}
	}

	return errors.New("redis: StickyConnPool.Close: infinite loop")
}

// Reset 重置连接
func (p *StickyConnPool) Reset(ctx context.Context) error {
	if p.badConnError() == nil {
		return nil
	}

	select {
	case cn, ok := <-p.ch:
		if !ok {
			return ErrClosed
		}
		// 池中移除连接
		p.pool.Remove(ctx, cn, ErrClosed)
		p._badConnError.Store(BadConnError{wrapped: nil})
	default:
		return errors.New("redis: StickyConnPool does not have a Conn")
	}

	if !atomic.CompareAndSwapUint32(&p.state, stateInited, stateDefault) {
		state := atomic.LoadUint32(&p.state)
		return fmt.Errorf("redis: invalid StickyConnPool state: %d", state)
	}

	return nil
}

// 返回连接error
func (p *StickyConnPool) badConnError() error {
	if v := p._badConnError.Load(); v != nil {
		if err := v.(BadConnError); err.wrapped != nil {
			return err
		}
	}
	return nil
}

func (p *StickyConnPool) Len() int {
	switch atomic.LoadUint32(&p.state) {
	case stateDefault:
		return 0
	case stateInited:
		return 1
	case stateClosed:
		return 0
	default:
		panic("not reached")
	}
}

func (p *StickyConnPool) IdleLen() int {
	return len(p.ch)
}

func (p *StickyConnPool) Stats() *Stats {
	return &Stats{}
}
