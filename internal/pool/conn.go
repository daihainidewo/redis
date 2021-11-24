package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal/proto"
)

var noDeadline = time.Time{}

// Conn 连接
type Conn struct {
	usedAt  int64 // atomic
	netConn net.Conn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.wr = proto.NewWriter(cn.bw)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

// SetNetConn 绑定底层连接
func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

// WithReader 带有读超时的执行 fn
func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	// 设置读截止时间
	if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
		return err
	}
	return fn(cn.rd)
}

// WithWriter 带有写超时的执行 fn
func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	// 设置写截止时间
	if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
		return err
	}

	if cn.bw.Buffered() > 0 {
		cn.bw.Reset(cn.netConn)
	}

	if err := fn(cn.wr); err != nil {
		return err
	}

	return cn.bw.Flush()
}

// Close 关闭连接
func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

// 根据传入的 timeout 和 ctx 的 deadline 取最近的时间值为 deadline
func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
