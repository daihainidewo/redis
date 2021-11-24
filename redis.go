package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/pool"
	"github.com/go-redis/redis/v8/internal/proto"
)

// Nil reply returned by Redis when key does not exist.
const Nil = proto.Nil

func SetLogger(logger internal.Logging) {
	internal.Logger = logger
}

//------------------------------------------------------------------------------

// Hook 执行操作的hook 分为单命令和多命令
type Hook interface {
	BeforeProcess(ctx context.Context, cmd Cmder) (context.Context, error)
	AfterProcess(ctx context.Context, cmd Cmder) error

	BeforeProcessPipeline(ctx context.Context, cmds []Cmder) (context.Context, error)
	AfterProcessPipeline(ctx context.Context, cmds []Cmder) error
}

type hooks struct {
	hooks []Hook
}

func (hs *hooks) lock() {
	hs.hooks = hs.hooks[:len(hs.hooks):len(hs.hooks)]
}

func (hs hooks) clone() hooks {
	clone := hs
	clone.lock()
	return clone
}

func (hs *hooks) AddHook(hook Hook) {
	hs.hooks = append(hs.hooks, hook)
}

// 单命令
func (hs hooks) process(
	ctx context.Context, cmd Cmder, fn func(context.Context, Cmder) error,
) error {
	if len(hs.hooks) == 0 {
		err := fn(ctx, cmd)
		cmd.SetErr(err)
		return err
	}

	var hookIndex int
	var retErr error

	for ; hookIndex < len(hs.hooks) && retErr == nil; hookIndex++ {
		ctx, retErr = hs.hooks[hookIndex].BeforeProcess(ctx, cmd)
		if retErr != nil {
			cmd.SetErr(retErr)
		}
	}

	if retErr == nil {
		retErr = fn(ctx, cmd)
		cmd.SetErr(retErr)
	}

	for hookIndex--; hookIndex >= 0; hookIndex-- {
		if err := hs.hooks[hookIndex].AfterProcess(ctx, cmd); err != nil {
			retErr = err
			cmd.SetErr(retErr)
		}
	}

	return retErr
}

// 多命令
func (hs hooks) processPipeline(
	ctx context.Context, cmds []Cmder, fn func(context.Context, []Cmder) error,
) error {
	if len(hs.hooks) == 0 {
		err := fn(ctx, cmds)
		return err
	}

	var hookIndex int
	var retErr error

	for ; hookIndex < len(hs.hooks) && retErr == nil; hookIndex++ {
		ctx, retErr = hs.hooks[hookIndex].BeforeProcessPipeline(ctx, cmds)
		if retErr != nil {
			setCmdsErr(cmds, retErr)
		}
	}

	if retErr == nil {
		retErr = fn(ctx, cmds)
	}

	for hookIndex--; hookIndex >= 0; hookIndex-- {
		if err := hs.hooks[hookIndex].AfterProcessPipeline(ctx, cmds); err != nil {
			retErr = err
			setCmdsErr(cmds, retErr)
		}
	}

	return retErr
}

// 事务多命令
func (hs hooks) processTxPipeline(
	ctx context.Context, cmds []Cmder, fn func(context.Context, []Cmder) error,
) error {
	cmds = wrapMultiExec(ctx, cmds)
	return hs.processPipeline(ctx, cmds, fn)
}

//------------------------------------------------------------------------------

// 封装对连接对象的操作
type baseClient struct {
	opt      *Options
	connPool pool.Pooler

	onClose func() error // hook called when client is closed
}

func newBaseClient(opt *Options, connPool pool.Pooler) *baseClient {
	return &baseClient{
		opt:      opt,
		connPool: connPool,
	}
}

func (c *baseClient) clone() *baseClient {
	clone := *c
	return &clone
}

func (c *baseClient) withTimeout(timeout time.Duration) *baseClient {
	opt := c.opt.clone()
	opt.ReadTimeout = timeout
	opt.WriteTimeout = timeout

	clone := c.clone()
	clone.opt = opt

	return clone
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Redis<%s db:%d>", c.getAddr(), c.opt.DB)
}

// 新建redis连接
func (c *baseClient) newConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := c.connPool.NewConn(ctx)
	if err != nil {
		return nil, err
	}

	err = c.initConn(ctx, cn)
	if err != nil {
		_ = c.connPool.CloseConn(cn)
		return nil, err
	}

	return cn, nil
}

// 获取可执行命令的连接
func (c *baseClient) getConn(ctx context.Context) (*pool.Conn, error) {
	if c.opt.Limiter != nil {
		// 限制器
		err := c.opt.Limiter.Allow()
		if err != nil {
			return nil, err
		}
	}

	cn, err := c._getConn(ctx)
	if err != nil {
		if c.opt.Limiter != nil {
			// 汇报错误
			c.opt.Limiter.ReportResult(err)
		}
		return nil, err
	}

	return cn, nil
}

// 从连接池中获取连接
func (c *baseClient) _getConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := c.connPool.Get(ctx)
	if err != nil {
		return nil, err
	}

	if cn.Inited {
		return cn, nil
	}

	// 初始化连接
	if err := c.initConn(ctx, cn); err != nil {
		// 初始化失败就将连接从连接池中删除
		c.connPool.Remove(ctx, cn, err)
		if err := errors.Unwrap(err); err != nil {
			return nil, err
		}
		return nil, err
	}

	return cn, nil
}

// 初始化连接
func (c *baseClient) initConn(ctx context.Context, cn *pool.Conn) error {
	if cn.Inited {
		return nil
	}
	cn.Inited = true

	if c.opt.Password == "" &&
		c.opt.DB == 0 &&
		!c.opt.readOnly &&
		c.opt.OnConnect == nil {
		return nil
	}

	// 建立一个只返回同一个连接的对象池
	connPool := pool.NewSingleConnPool(c.connPool, cn)
	conn := newConn(ctx, c.opt, connPool)

	_, err := conn.Pipelined(ctx, func(pipe Pipeliner) error {
		if c.opt.Password != "" {
			if c.opt.Username != "" {
				pipe.AuthACL(ctx, c.opt.Username, c.opt.Password)
			} else {
				pipe.Auth(ctx, c.opt.Password)
			}
		}

		if c.opt.DB > 0 {
			pipe.Select(ctx, c.opt.DB)
		}

		if c.opt.readOnly {
			pipe.ReadOnly(ctx)
		}

		return nil
	})
	if err != nil {
		return err
	}

	if c.opt.OnConnect != nil {
		return c.opt.OnConnect(ctx, conn)
	}
	return nil
}

// 向连接池归还连接
func (c *baseClient) releaseConn(ctx context.Context, cn *pool.Conn, err error) {
	if c.opt.Limiter != nil {
		c.opt.Limiter.ReportResult(err)
	}

	if isBadConn(err, false, c.opt.Addr) {
		c.connPool.Remove(ctx, cn, err)
	} else {
		c.connPool.Put(ctx, cn)
	}
}

// 找连接 执行fn
func (c *baseClient) withConn(
	ctx context.Context, fn func(context.Context, *pool.Conn) error,
) error {
	cn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	defer func() {
		c.releaseConn(ctx, cn, err)
	}()

	done := ctx.Done() //nolint:ifshort

	if done == nil {
		// 如果没有关闭通知就直接调用
		err = fn(ctx, cn)
		return err
	}

	errc := make(chan error, 1)
	go func() { errc <- fn(ctx, cn) }()

	select {
	case <-done:
		_ = cn.Close()
		// Wait for the goroutine to finish and send something.
		<-errc

		err = ctx.Err()
		return err
	case err = <-errc:
		return err
	}
}

// 带有重试机制的请求
func (c *baseClient) process(ctx context.Context, cmd Cmder) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRetries; attempt++ {
		attempt := attempt

		retry, err := c._process(ctx, cmd, attempt)
		if err == nil || !retry {
			return err
		}

		lastErr = err
	}
	return lastErr
}

// 发送请求接收响应
func (c *baseClient) _process(ctx context.Context, cmd Cmder, attempt int) (bool, error) {
	if attempt > 0 {
		if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
			return false, err
		}
	}

	retryTimeout := uint32(1)
	err := c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
			return writeCmd(wr, cmd)
		})
		if err != nil {
			return err
		}

		err = cn.WithReader(ctx, c.cmdTimeout(cmd), cmd.readReply)
		if err != nil {
			if cmd.readTimeout() == nil {
				atomic.StoreUint32(&retryTimeout, 1)
			}
			return err
		}

		return nil
	})
	if err == nil {
		return false, nil
	}

	retry := shouldRetry(err, atomic.LoadUint32(&retryTimeout) == 1)
	return retry, err
}

// 根据尝试次数计算休眠时间
func (c *baseClient) retryBackoff(attempt int) time.Duration {
	return internal.RetryBackoff(attempt, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
}

// 返回读超时
func (c *baseClient) cmdTimeout(cmd Cmder) time.Duration {
	if timeout := cmd.readTimeout(); timeout != nil {
		t := *timeout
		if t == 0 {
			return 0
		}
		return t + 10*time.Second
	}
	return c.opt.ReadTimeout
}

// Close 关闭客户端 释放所有的连接
// Close closes the client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *baseClient) Close() error {
	var firstErr error
	if c.onClose != nil {
		// 执行事件操作
		if err := c.onClose(); err != nil {
			firstErr = err
		}
	}
	if err := c.connPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (c *baseClient) getAddr() string {
	return c.opt.Addr
}

func (c *baseClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.generalProcessPipeline(ctx, cmds, c.pipelineProcessCmds)
}

func (c *baseClient) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	return c.generalProcessPipeline(ctx, cmds, c.txPipelineProcessCmds)
}

// 批量处理器
type pipelineProcessor func(context.Context, *pool.Conn, []Cmder) (bool, error)

func (c *baseClient) generalProcessPipeline(
	ctx context.Context, cmds []Cmder, p pipelineProcessor,
) error {
	err := c._generalProcessPipeline(ctx, cmds, p)
	if err != nil {
		setCmdsErr(cmds, err)
		return err
	}
	return cmdsFirstErr(cmds)
}

// 实际执行操作函数
func (c *baseClient) _generalProcessPipeline(
	ctx context.Context, cmds []Cmder, p pipelineProcessor,
) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			// 重试等待
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		// 找 一条连接 执行 p
		var canRetry bool
		lastErr = c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			var err error
			canRetry, err = p(ctx, cn, cmds)
			return err
		})
		if lastErr == nil || !canRetry || !shouldRetry(lastErr, true) {
			return lastErr
		}
	}
	return lastErr
}

// 批量处理多个命令
func (c *baseClient) pipelineProcessCmds(
	ctx context.Context, cn *pool.Conn, cmds []Cmder,
) (bool, error) {
	// 批量写入命令
	err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmds(wr, cmds)
	})
	if err != nil {
		return true, err
	}

	// 批量读取响应
	err = cn.WithReader(ctx, c.opt.ReadTimeout, func(rd *proto.Reader) error {
		return pipelineReadCmds(rd, cmds)
	})
	return true, err
}

// 批量处理响应
func pipelineReadCmds(rd *proto.Reader, cmds []Cmder) error {
	for _, cmd := range cmds {
		err := cmd.readReply(rd)
		cmd.SetErr(err)
		if err != nil && !isRedisError(err) {
			return err
		}
	}
	return nil
}

// 事务执行多个命令 返回时剔除事务请求的响应
func (c *baseClient) txPipelineProcessCmds(
	ctx context.Context, cn *pool.Conn, cmds []Cmder,
) (bool, error) {
	err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmds(wr, cmds)
	})
	if err != nil {
		return true, err
	}

	err = cn.WithReader(ctx, c.opt.ReadTimeout, func(rd *proto.Reader) error {
		statusCmd := cmds[0].(*StatusCmd)
		// Trim multi and exec.
		cmds = cmds[1 : len(cmds)-1]

		// 预处理事务响应头部返回数据
		err := txPipelineReadQueued(rd, statusCmd, cmds)
		if err != nil {
			return err
		}

		return pipelineReadCmds(rd, cmds)
	})
	return false, err
}

// 封装事务执行命令
func wrapMultiExec(ctx context.Context, cmds []Cmder) []Cmder {
	if len(cmds) == 0 {
		panic("not reached")
	}
	cmdCopy := make([]Cmder, len(cmds)+2)
	cmdCopy[0] = NewStatusCmd(ctx, "multi")
	copy(cmdCopy[1:], cmds)
	cmdCopy[len(cmdCopy)-1] = NewSliceCmd(ctx, "exec")
	return cmdCopy
}

// 处理事务读取管道
func txPipelineReadQueued(rd *proto.Reader, statusCmd *StatusCmd, cmds []Cmder) error {
	// Parse queued replies.
	if err := statusCmd.readReply(rd); err != nil {
		return err
	}

	// 查看每个命令执行的返回结果
	for range cmds {
		if err := statusCmd.readReply(rd); err != nil && !isRedisError(err) {
			return err
		}
	}

	// Parse number of replies.
	line, err := rd.ReadLine()
	if err != nil {
		if err == Nil {
			err = TxFailedErr
		}
		return err
	}

	switch line[0] {
	case proto.ErrorReply:
		return proto.ParseErrorReply(line)
	case proto.ArrayReply:
		// ok
	default:
		err := fmt.Errorf("redis: expected '*', but got line %q", line)
		return err
	}

	return nil
}

//------------------------------------------------------------------------------

// Client 并发安全的 带有连接池的 客户端
// Client is a Redis client representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type Client struct {
	*baseClient
	cmdable
	hooks
	ctx context.Context
}

// NewClient returns a client to the Redis Server specified by Options.
func NewClient(opt *Options) *Client {
	// 检测参数 设置默认值
	opt.init()

	c := Client{
		baseClient: newBaseClient(opt, newConnPool(opt)),
		ctx:        context.Background(),
	}
	c.cmdable = c.Process

	return &c
}

func (c *Client) clone() *Client {
	clone := *c
	clone.cmdable = clone.Process
	clone.hooks.lock()
	return &clone
}

func (c *Client) WithTimeout(timeout time.Duration) *Client {
	clone := c.clone()
	clone.baseClient = c.baseClient.withTimeout(timeout)
	return clone
}

func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) WithContext(ctx context.Context) *Client {
	if ctx == nil {
		panic("nil context")
	}
	clone := c.clone()
	clone.ctx = ctx
	return clone
}

func (c *Client) Conn(ctx context.Context) *Conn {
	return newConn(ctx, c.opt, pool.NewStickyConnPool(c.connPool))
}

// Do creates a Cmd from the args and processes the cmd.
func (c *Client) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *Client) Process(ctx context.Context, cmd Cmder) error {
	return c.hooks.process(ctx, cmd, c.baseClient.process)
}

func (c *Client) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processPipeline(ctx, cmds, c.baseClient.processPipeline)
}

func (c *Client) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processTxPipeline(ctx, cmds, c.baseClient.processTxPipeline)
}

// Options returns read-only Options that were used to create the client.
func (c *Client) Options() *Options {
	return c.opt
}

type PoolStats pool.Stats

// PoolStats returns connection pool stats.
func (c *Client) PoolStats() *PoolStats {
	stats := c.connPool.Stats()
	return (*PoolStats)(stats)
}

func (c *Client) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Client) Pipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processPipeline,
	}
	pipe.init()
	return &pipe
}

func (c *Client) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *Client) TxPipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processTxPipeline,
	}
	pipe.init()
	return &pipe
}

// 发布订阅
func (c *Client) pubSub() *PubSub {
	pubsub := &PubSub{
		opt: c.opt,

		newConn: func(ctx context.Context, channels []string) (*pool.Conn, error) {
			return c.newConn(ctx)
		},
		closeConn: c.connPool.CloseConn,
	}
	pubsub.init()
	return pubsub
}

// Subscribe subscribes the client to the specified channels.
// Channels can be omitted to create empty subscription.
// Note that this method does not wait on a response from Redis, so the
// subscription may not be active immediately. To force the connection to wait,
// you may call the Receive() method on the returned *PubSub like so:
//
//    sub := client.Subscribe(queryResp)
//    iface, err := sub.Receive()
//    if err != nil {
//        // handle error
//    }
//
//    // Should be *Subscription, but others are possible if other actions have been
//    // taken on sub since it was created.
//    switch iface.(type) {
//    case *Subscription:
//        // subscribe succeeded
//    case *Message:
//        // received first message
//    case *Pong:
//        // pong received
//    default:
//        // handle error
//    }
//
//    ch := sub.Channel()
func (c *Client) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (c *Client) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.PSubscribe(ctx, channels...)
	}
	return pubsub
}

//------------------------------------------------------------------------------

// 连接
type conn struct {
	baseClient
	cmdable
	statefulCmdable
	hooks // TODO: inherit hooks
}

// Conn 表示单一连接
// Conn represents a single Redis connection rather than a pool of connections.
// Prefer running commands from Client unless there is a specific need
// for a continuous single Redis connection.
type Conn struct {
	*conn
	ctx context.Context
}

// 新建连接对象
func newConn(ctx context.Context, opt *Options, connPool pool.Pooler) *Conn {
	c := Conn{
		conn: &conn{
			baseClient: baseClient{
				opt:      opt,
				connPool: connPool,
			},
		},
		ctx: ctx,
	}
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
	return &c
}

// Process 处理单条命令
func (c *Conn) Process(ctx context.Context, cmd Cmder) error {
	return c.hooks.process(ctx, cmd, c.baseClient.process)
}

// 处理多条命令
func (c *Conn) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processPipeline(ctx, cmds, c.baseClient.processPipeline)
}

// 事务处理多条命令
func (c *Conn) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processTxPipeline(ctx, cmds, c.baseClient.processTxPipeline)
}

func (c *Conn) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Conn) Pipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processPipeline,
	}
	pipe.init()
	return &pipe
}

func (c *Conn) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *Conn) TxPipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processTxPipeline,
	}
	pipe.init()
	return &pipe
}
