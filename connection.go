// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tchannel

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/uber/tchannel-go/tos"

	"github.com/uber-go/atomic"
	"golang.org/x/net/context"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

const (
	// CurrentProtocolVersion is the current version of the TChannel protocol
	// supported by this stack
	CurrentProtocolVersion = 0x02

	// DefaultConnectTimeout is the default timeout used by net.Dial, if no timeout
	// is specified in the context.
	DefaultConnectTimeout = 5 * time.Second

	// defaultConnectionBufferSize is the default size for the connection's
	// read and write channels.
	defaultConnectionBufferSize = 512
)

// 版本协议: 语言，语言版本和tchannel版本
type PeerVersion struct {
	Language        string `json:"language"`
	LanguageVersion string `json:"languageVersion"`
	TChannelVersion string `json:"tchannelVersion"`
}

// rpc service或者rpc client的相关信息: host:port, 进程名称和版本协议
type PeerInfo struct {
	HostPort string `json:"hostPort"`

	ProcessName string `json:"processName"`

	// IsEphemeral returns whether the remote host:port is ephemeral (e.g. not listening).
	IsEphemeral bool `json:"isEphemeral"`

	Version PeerVersion `json:"version"`
}

// 实现String interface，提供PeerInfo的输出
func (p PeerInfo) String() string {
	return fmt.Sprintf("%s(%s)", p.HostPort, p.ProcessName)
}

// IsEphemeralHostPort returns whether the connection is from an ephemeral host:port.
func (p PeerInfo) IsEphemeralHostPort() bool {
	return p.IsEphemeral
}

// 本地PeerInfo，channel存储的localPeerInfo
type LocalPeerInfo struct {
	PeerInfo

	// 服务名
	ServiceName string `json:"serviceName"`
}

func (p LocalPeerInfo) String() string {
	return fmt.Sprintf("%v: %v", p.ServiceName, p.PeerInfo)
}

var (
	// ErrConnectionClosed is returned when a caller performs an method
	// on a closed connection
	ErrConnectionClosed = errors.New("connection is closed")

	// ErrSendBufferFull is returned when a message cannot be sent to the
	// peer because the frame sending buffer has become full.  Typically
	// this indicates that the connection is stuck and writes have become
	// backed up
	ErrSendBufferFull = errors.New("connection send buffer is full, cannot send frame")

	// ErrConnectionNotReady is no longer used.
	ErrConnectionNotReady = errors.New("connection is not yet ready")
)

// errConnectionInvalidState is returned when the connection is in an unknown state.
type errConnectionUnknownState struct {
	site  string
	state connectionState
}

func (e errConnectionUnknownState) Error() string {
	return fmt.Sprintf("connection is in unknown state: %v at %v", e.state, e.site)
}

// ConnectionOptions are options that control the behavior of a Connection
// ConnectionOptions用于控制connection的行为
type ConnectionOptions struct {
	// 发送消息最终都是封装在协议帧中，帧的大量使用使用临时对象池是非常好，tchannel提供了sync.Pool, tchannel队列和其他，默认使用raw heap
	FramePool FramePool

	// The size of send channel buffers. Defaults to 512.
	SendBufferSize int

	// checksum消息校验规则
	ChecksumType ChecksumType

	// ToS class name marked on outbound packets.
	TosPriority tos.ToS

	// connection的健康检查，默认不开启
	HealthChecks HealthCheckOptions
}

// connectionEvents是由connection的状态发生变化时触发
// 比如在channel中经常提到的newConnection方法，当一个流入或者流出的连接创建时，会触发OnActive操作，它会使得connection存储在channel.mutable.conns中
type connectionEvents struct {
	// OnActive is called when a connection becomes active.
	OnActive func(c *Connection)

	// OnCloseStateChange is called when a connection that is closing changes state.
	OnCloseStateChange func(c *Connection)

	// OnExchangeUpdated is called when a message exchange added or removed.
	OnExchangeUpdated func(c *Connection)
}

// Connection represents a connection to a remote peer.
type Connection struct {
	channelConnectionCommon

	connID         uint32            // conn id，自增长
	opts           ConnectionOptions // 控制连接的行为
	conn           net.Conn          // 真正的conn，上面有数据流动
	localPeerInfo  LocalPeerInfo     // 连接发起方的相关服务信息
	remotePeerInfo PeerInfo          // 连接另一端的相关信息
	// 连接上发送协议帧到channel队列上，然后connection的goroutine接收数据并发送到net.conn上
	sendCh   chan *Frame
	stopCh   chan struct{}       //  停止发送数据到net.conn上
	state    connectionState     // connection的状态
	stateMut sync.RWMutex        // 状态读写互斥
	inbound  *messageExchangeSet // ::TODO
	outbound *messageExchangeSet // ::TODO
	// 当每个连接接收到协议帧并获取业务逻辑数据后，
	// 通过继承channel的handler通过反射，往上层放到业务逻辑处理业务
	handler         Handler
	nextMessageID   atomic.Uint32    // connection下一个msg id自增长
	events          connectionEvents // connection state变化时触发的事件
	commonStatsTags map[string]string
	relay           *Relayer // ::TODO

	// 当这个连接为流入时，也就是服务注册的服务channel accept一个连接时，outboundHP为空；
	// 当服务channel主动发起与某个host:port建立一个connection， 则outboundHP为服务channel的host:port
	// outboundHP = outbound host port
	outboundHP string

	// closeNetworkCalled is used to avoid errors from being logged
	// when this side closes a connection.
	closeNetworkCalled atomic.Int32
	// stoppedExchanges is atomically set when exchanges are stopped due to error.
	stoppedExchanges atomic.Uint32
	// pendingMethods is the number of methods running that may block closing of sendCh.
	pendingMethods atomic.Int64
	// remotePeerAddress is used as a cache for remote peer address parsed into individual
	// components that can be used to set peer tags on OpenTracing Span.
	remotePeerAddress peerAddressComponents

	// healthCheckCtx/Quit are used to stop health checks.
	healthCheckCtx     context.Context
	healthCheckQuit    context.CancelFunc
	healthCheckDone    chan struct{}
	healthCheckHistory *healthHistory

	// 该connection上一次有数据流动的时间, goroutine idle sweep通过它检测连接的空闲超时
	// (unix time, nano)
	lastActivity atomic.Int64
}

// 该变量可以通过PeerInfo获取解析
type peerAddressComponents struct {
	port     uint16
	ipv4     uint32
	ipv6     string
	hostname string
}

// 全局conn id
var _nextConnID atomic.Uint32

type connectionState int

const (
	// Connection is fully active
	connectionActive connectionState = iota + 1

	// Connection is starting to close; new incoming requests are rejected, outbound
	// requests are allowed to proceed
	connectionStartClose

	// Connection has finished processing all active inbound, and is
	// waiting for outbound requests to complete or timeout
	connectionInboundClosed

	// Connection is fully closed
	connectionClosed
)

//go:generate stringer -type=connectionState

// 获取context的载体剩余生存时间
func getTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return DefaultConnectTimeout
	}

	return deadline.Sub(time.Now())
}

// connection的默认控制行为，checksum：crc32， raw pool， 512， 默认的健康检查
func (co ConnectionOptions) withDefaults() ConnectionOptions {
	if co.ChecksumType == ChecksumTypeNone {
		co.ChecksumType = ChecksumTypeCrc32
	}
	if co.FramePool == nil {
		co.FramePool = DefaultFramePool
	}
	if co.SendBufferSize <= 0 {
		co.SendBufferSize = defaultConnectionBufferSize
	}
	co.HealthChecks = co.HealthChecks.withDefaults()
	return co
}

// ::TODO, 比较底层
func (ch *Channel) setConnectionTosPriority(tosPriority tos.ToS, c net.Conn) error {
	tcpAddr, isTCP := c.RemoteAddr().(*net.TCPAddr)
	if !isTCP {
		return nil
	}

	// Handle dual stack listeners and set Traffic Class.
	var err error
	switch ip := tcpAddr.IP; {
	case ip.To16() != nil && ip.To4() == nil:
		err = ipv6.NewConn(c).SetTrafficClass(int(tosPriority))
	case ip.To4() != nil:
		err = ipv4.NewConn(c).SetTOS(int(tosPriority))
	}
	return err
}

// 当rpc service通过Accept获取到一个网络连接后，在通过上面的协议帧解析和封装，然后再通过该newConnection创建channel的连接管理
//
// 注意一个有关events的重要细节：当创建active connection完后，会通过event绑定的channel.outboundConnectionActive创建channel.mutable.conns映射的host:port connection, 且流入和流出的连接也是有方向的
// 也就是说，A与B server，两个方向同时发起connection，结果是两个不同的连接，且在server端的len(channel.mutable.conns)=2
func (ch *Channel) newConnection(conn net.Conn, initialID uint32, outboundHP string, remotePeer PeerInfo, remotePeerAddress peerAddressComponents, events connectionEvents) *Connection {
	// 获取服务的连接控制行为
	opts := ch.connectionOptions.withDefaults()

	// 获取connection id, 自增长
	connID := _nextConnID.Inc()
	log := ch.log.WithFields(LogFields{
		{"connID", connID},
		{"localAddr", conn.LocalAddr().String()},
		{"remoteAddr", conn.RemoteAddr().String()},
		{"remoteHostPort", remotePeer.HostPort},
		{"remoteIsEphemeral", remotePeer.IsEphemeral},
		{"remoteProcess", remotePeer.ProcessName},
	}...)
	if outboundHP != "" {
		log = log.WithFields(LogFields{
			{"outboundHP", outboundHP},
			{"connectionDirection", outbound},
		}...)
	} else {
		log = log.WithFields(LogField{"connectionDirection", inbound})
	}
	// 获取服务注册的服务相关信息, 不再赘述
	peerInfo := ch.PeerInfo()

	// 创建一个active connection
	c := &Connection{
		channelConnectionCommon: ch.channelConnectionCommon,

		connID: connID,
		conn:   conn, // accept conn
		opts:   opts,
		state:  connectionActive,
		sendCh: make(chan *Frame, opts.SendBufferSize),
		stopCh: make(chan struct{}),
		// rpc service的服务信息
		localPeerInfo: peerInfo,
		//  rpc client的相关信息, 字段都是peerInfo
		remotePeerInfo: remotePeer,
		// host:port相关，可以通过peerInfo解析获取
		remotePeerAddress: remotePeerAddress,
		outboundHP:        outboundHP,                                             // 流出
		inbound:           newMessageExchangeSet(log, messageExchangeSetInbound),  // ::TODO
		outbound:          newMessageExchangeSet(log, messageExchangeSetOutbound), // ::TODO
		// 所有的rpc client的请求处理都是通过, 即通过channelHandler类型的Handler方法处理,
		// 该channelHandler通过rpc client请求的serviceName，获取subchannel
		// 并通过subchannel的Handle方法处理
		/*
			type Handler interface{
				Handle(ctx context.Context, call *InboundCall)
			}
		*/
		handler:            ch.handler,
		events:             events,
		commonStatsTags:    ch.mutable.commonStatsTags,
		healthCheckHistory: newHealthHistory(),
		// 每个连接最后活跃的时间，这个用于idle sweep goroutine回收超时空闲连接
		lastActivity: *atomic.NewInt64(ch.timeNow().UnixNano()),
	}

	if tosPriority := opts.TosPriority; tosPriority > 0 {
		if err := ch.setConnectionTosPriority(tosPriority, conn); err != nil {
			log.WithFields(ErrField(err)).Error("Failed to set ToS priority.")
		}
	}

	// 设置连接开始时的msg id为0
	c.nextMessageID.Store(initialID)
	c.log = log
	c.inbound.onRemoved = c.checkExchanges
	c.outbound.onRemoved = c.checkExchanges
	c.inbound.onAdded = c.onExchangeAdded
	c.outbound.onAdded = c.onExchangeAdded

	// ::TODO
	if ch.RelayHost() != nil {
		c.relay = NewRelayer(ch, c)
	}

	// 该初始化的active connection, 并起一个goroutine进行健康检查
	// 同时触发channel.mutable.conns存储conn
	//
	// 我觉得goroutine太多了，是不是一个通过channel队列方式进行沟通协作?
	c.callOnActive()

	// 起一个goroutine， 从connection读取协议帧消息
	go c.readFrames(connID)
	// 起一个goroutine，监听从channel队列上发送过来的frame，写入到connection中
	go c.writeFrames(connID)
	return c
}

// ::TODO
func (c *Connection) onExchangeAdded() {
	c.callOnExchangeChange()
}

// 获取connection的当前状态是否健康
func (c *Connection) IsActive() bool {
	return c.readState() == connectionActive
}

// 当新建的连接为active状态时，会触发健康检查和conn的存储
func (c *Connection) callOnActive() {
	log := c.log
	if remoteVersion := c.remotePeerInfo.Version; remoteVersion != (PeerVersion{}) {
		log = log.WithFields(LogFields{
			{"remotePeerLanguage", remoteVersion.Language},
			{"remotePeerLanguageVersion", remoteVersion.LanguageVersion},
			{"remotePeerTChannelVersion", remoteVersion.TChannelVersion},
		}...)
	}
	log.Info("Created new active connection.")

	// 在channel.mutable.conns存储connection
	if f := c.events.OnActive; f != nil {
		f(c)
	}

	// 健康检查
	if c.opts.HealthChecks.enabled() {
		c.healthCheckCtx, c.healthCheckQuit = context.WithCancel(context.Background())
		c.healthCheckDone = make(chan struct{})
		go c.healthCheck(c.connID)
	}
}

// 因connection state为关闭状态，触发events的关闭事件
func (c *Connection) callOnCloseStateChange() {
	if f := c.events.OnCloseStateChange; f != nil {
		f(c)
	}
}

// ::TODO
func (c *Connection) callOnExchangeChange() {
	if f := c.events.OnExchangeUpdated; f != nil {
		f(c)
	}
}

// 当connection的健康检查开启时，会定时的进行协议帧类型为call ping空包检查连接的连通性
func (c *Connection) ping(ctx context.Context) error {
	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return ErrInvalidConnectionState
	}
	defer c.pendingExchangeMethodDone()

	// 拼装ping的协议帧包 ::TODO
	req := &pingReq{id: c.NextMessageID()}
	mex, err := c.outbound.newExchange(ctx, c.opts.FramePool, req.messageType(), req.ID(), 1)
	if err != nil {
		return c.connectionError("create ping exchange", err)
	}
	defer c.outbound.removeExchange(req.ID())

	// 发送帧包到connection发送的goroutine中，通过channel队列实现
	// connection有两个goroutine，一个为发送，一个为接收
	if err := c.sendMessage(req); err != nil {
		return c.connectionError("send ping", err)
	}

	// ::TODO
	return c.recvMessage(ctx, &pingRes{}, mex)
}

// connection针对inbound数据，也即服务注册的服务channel accept连接后，后面针对该connection流入的数据协议帧解析
// 下面的type占1字节, 类型包括：init req, init res, call req, call res, call req continue, call res continue, cancel, claim, ping req, ping res, error
/*
Position	Contents
0-7			size:2 type:1 reserved:1 id:4
8-15		reserved:8
16+			payload - based on type
*/

// handlePingRes，是针对ping res包解析数据
// connection接收对方发过来的ping响应包
func (c *Connection) handlePingRes(frame *Frame) bool {
	// ::TODO
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		c.log.WithFields(LogField{"response", frame.Header}).Warn("Unexpected ping response.")
		return true
	}
	// ping req is waiting for this frame, and will release it.
	return false
}

// handlePingReq, 针对对connection的另一方发出ping req包, 进行解析
func (c *Connection) handlePingReq(frame *Frame) {
	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return
	}
	defer c.pendingExchangeMethodDone()

	if state := c.readState(); state != connectionActive {
		c.protocolError(frame.Header.ID, errConnNotActive{"ping on incoming", state})
		return
	}

	// ping req包拿到后，直接发出ping res包, 发送到connection的goroutine发送数据中，通过channel
	pingRes := &pingRes{id: frame.Header.ID}
	if err := c.sendMessage(pingRes); err != nil {
		c.connectionError("send pong", err)
	}
}

// sendMessage, 发送消息到channel队列上，connection的一个goroutine监听队列数据, 并发送到net.Conn上
func (c *Connection) sendMessage(msg message) error {
	// message封装成帧
	frame := c.opts.FramePool.Get()
	if err := frame.write(msg); err != nil {
		c.opts.FramePool.Release(frame)
		return err
	}

	// 发送到channel队列上
	select {
	case c.sendCh <- frame:
		return nil
	default:
		return ErrSendBufferFull
	}
}

// recvMessage阻塞读，等待frame的到来, 用于connection的ping包响应读取
func (c *Connection) recvMessage(ctx context.Context, msg message, mex *messageExchange) error {
	frame, err := mex.recvPeerFrameOfType(msg.messageType())
	if err != nil {
		if err, ok := err.(errorMessage); ok {
			return err.AsSystemError()
		}
		return err
	}

	err = frame.read(msg)
	c.opts.FramePool.Release(frame)
	return err
}

// 获取connection另一端的peerInfo信息
func (c *Connection) RemotePeerInfo() PeerInfo {
	return c.remotePeerInfo
}

// connection上的msg id自增长
func (c *Connection) NextMessageID() uint32 {
	return c.nextMessageID.Inc()
}

// SendSystemError 发送一个系统错误帧
func (c *Connection) SendSystemError(id uint32, span Span, err error) error {
	frame := c.opts.FramePool.Get()

	if err := frame.write(&errorMessage{
		id:      id,
		errCode: GetSystemErrorCode(err),
		tracing: span,
		message: GetSystemErrorMessage(err),
	}); err != nil {

		// This shouldn't happen - it means writing the errorMessage is broken.
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			LogField{"id", id},
			ErrField(err),
		).Warn("Couldn't create outbound frame.")
		return fmt.Errorf("failed to create outbound error frame")
	}

	// When sending errors, we hold the state rlock to ensure that sendCh is not closed
	// as we are sending the frame.
	return c.withStateRLock(func() error {
		// Errors cannot be sent if the connection has been closed.
		if c.state == connectionClosed {
			c.log.WithFields(
				LogField{"remotePeer", c.remotePeerInfo},
				LogField{"id", id},
			).Info("Could not send error frame on closed connection.")
			return fmt.Errorf("failed to send error frame, connection state %v", c.state)
		}

		select {
		case c.sendCh <- frame: // Good to go
			return nil
		default: // If the send buffer is full, log and return an error.
		}
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			LogField{"id", id},
			ErrField(err),
		).Warn("Couldn't send outbound frame.")
		return fmt.Errorf("failed to send error frame, buffer full")
	})
}

func (c *Connection) logConnectionError(site string, err error) error {
	errCode := ErrCodeNetwork
	if err == io.EOF {
		c.log.Debugf("Connection got EOF")
	} else {
		logger := c.log.WithFields(
			LogField{"site", site},
			ErrField(err),
		)
		if se, ok := err.(SystemError); ok && se.Code() != ErrCodeNetwork {
			errCode = se.Code()
			logger.Error("Connection error.")
		} else {
			logger.Info("Connection error.")
		}
	}
	return NewWrappedSystemError(errCode, err)
}

// connectionError handles a connection level error
func (c *Connection) connectionError(site string, err error) error {
	var closeLogFields LogFields
	if err == io.EOF {
		closeLogFields = LogFields{{"reason", "network connection EOF"}}
	} else {
		closeLogFields = LogFields{
			{"reason", "connection error"},
			ErrField(err),
		}
	}

	c.stopHealthCheck()
	err = c.logConnectionError(site, err)
	c.close(closeLogFields...)

	// On any connection error, notify the exchanges of this error.
	if c.stoppedExchanges.CAS(0, 1) {
		c.outbound.stopExchanges(err)
		c.inbound.stopExchanges(err)
	}
	return err
}

func (c *Connection) protocolError(id uint32, err error) error {
	c.log.WithFields(ErrField(err)).Warn("Protocol error.")
	sysErr := NewWrappedSystemError(ErrCodeProtocol, err)
	c.SendSystemError(id, Span{}, sysErr)
	// Don't close the connection until the error has been sent.
	c.close(
		LogField{"reason", "protocol error"},
		ErrField(err),
	)

	// On any connection error, notify the exchanges of this error.
	if c.stoppedExchanges.CAS(0, 1) {
		c.outbound.stopExchanges(sysErr)
		c.inbound.stopExchanges(sysErr)
	}
	return sysErr
}

// withStateLock的作用：是的外部的闭包函数值的执行是在互斥期间
func (c *Connection) withStateLock(f func() error) error {
	c.stateMut.Lock()
	err := f()
	c.stateMut.Unlock()

	return err
}

// withStateRLock与上面类似，只是锁的粒度变大了
func (c *Connection) withStateRLock(f func() error) error {
	c.stateMut.RLock()
	err := f()
	c.stateMut.RUnlock()

	return err
}

// 获取连接的当前状态
func (c *Connection) readState() connectionState {
	c.stateMut.RLock()
	state := c.state
	c.stateMut.RUnlock()
	return state
}

// connection的一个goroutine用于接收流入的协议帧,
// 并通过connection的handleFrameNoRelay进行具体协议类型分发处理
func (c *Connection) readFrames(_ uint32) {
	headerBuf := make([]byte, FrameHeaderSize)

	handleErr := func(err error) {
		if c.closeNetworkCalled.Load() == 0 {
			c.connectionError("read frames", err)
		} else {
			c.log.Debugf("Ignoring error after connection was closed: %v", err)
		}
	}

	for {
		// 阻塞读取数据流， 因为peer-to-peer， 所以不存在并发问题
		// 读取到帧协议到头部后，在根据frame header首2字节获取大小，再减去16字节头部，则可以截取到frame一个完整的帧
		if _, err := io.ReadFull(c.conn, headerBuf); err != nil {
			handleErr(err)
			return
		}

		// 通过frame相关行为，读取payload数据，并把整帧数据存储到frame中
		frame := c.opts.FramePool.Get()
		if err := frame.ReadBody(headerBuf, c.conn); err != nil {
			handleErr(err)
			c.opts.FramePool.Release(frame)
			return
		}

		// 更新这个connection的最新active时间
		c.updateLastActivity(frame)

		// 协议帧分发到相关帧类型connection处理，比如：handlePingReq、handlePingRes等
		var releaseFrame bool
		if c.relay == nil {
			releaseFrame = c.handleFrameNoRelay(frame)
		} else {
			releaseFrame = c.handleFrameRelay(frame)
		}
		if releaseFrame {
			c.opts.FramePool.Release(frame)
		}
		// 最后用完frame后，记得释放临时对象池
	}
}

// relay ::TODO
func (c *Connection) handleFrameRelay(frame *Frame) bool {
	switch frame.Header.messageType {
	case messageTypeCallReq, messageTypeCallReqContinue, messageTypeCallRes, messageTypeCallResContinue, messageTypeError:
		if err := c.relay.Relay(frame); err != nil {
			c.log.WithFields(
				ErrField(err),
				LogField{"header", frame.Header},
				LogField{"remotePeer", c.remotePeerInfo},
			).Error("Failed to relay frame.")
		}
		return false
	default:
		return c.handleFrameNoRelay(frame)
	}
}

// connection的一个接收另一端发送过来的数据，
// 通过readFrames读取，并通过该方法进行协议类型分发处理
func (c *Connection) handleFrameNoRelay(frame *Frame) bool {
	releaseFrame := true

	// 一共7种类型帧, 分发到具体的协议处理
	// 另外的init req, init res, cancel, claim四种协议类型，目前没看到实现
	switch frame.Header.messageType {
	case messageTypeCallReq:
		releaseFrame = c.handleCallReq(frame)
	case messageTypeCallReqContinue:
		releaseFrame = c.handleCallReqContinue(frame)
	case messageTypeCallRes:
		releaseFrame = c.handleCallRes(frame)
	case messageTypeCallResContinue:
		releaseFrame = c.handleCallResContinue(frame)
	case messageTypePingReq:
		c.handlePingReq(frame)
	case messageTypePingRes:
		releaseFrame = c.handlePingRes(frame)
	case messageTypeError:
		releaseFrame = c.handleError(frame)
	default:
		// TODO(mmihic): Log and close connection with protocol error
		c.log.WithFields(
			LogField{"header", frame.Header},
			LogField{"remotePeer", c.remotePeerInfo},
		).Error("Received unexpected frame.")
	}

	return releaseFrame
}

// writeFrames是connection用于发送协议帧到net.Conn的goroutine
// 它通过channel队列获取frame，connection.writeMessage
func (c *Connection) writeFrames(_ uint32) {
	for {
		select {
		case f := <-c.sendCh:
			if c.log.Enabled(LogLevelDebug) {
				c.log.Debugf("Writing frame %s", f.Header)
			}

			c.updateLastActivity(f)
			// 直接写入到net.Conn
			err := f.WriteOut(c.conn)
			// 释放协议帧到临时对象池中
			c.opts.FramePool.Release(f)
			if err != nil {
				c.connectionError("write frames", err)
				return
			}
		case <-c.stopCh:
			// If there are frames in sendCh, we want to drain them.
			if len(c.sendCh) > 0 {
				continue
			}
			// 关闭connection的net.Conn不再接收外界的数据帧
			c.closeNetwork()
			return
		}
	}
}

// updateLastActivity更新connection的最后活跃时间
// 注意：校验connection是否活跃，只关注协议帧类型为：call req, call req continue, call res, call res continue和error
// ping用于connection的健康检查, 不是活跃数据
func (c *Connection) updateLastActivity(frame *Frame) {
	// Pings are ignored for last activity.
	switch frame.Header.messageType {
	case messageTypeCallReq, messageTypeCallReqContinue, messageTypeCallRes, messageTypeCallResContinue, messageTypeError:
		c.lastActivity.Store(c.timeNow().UnixNano())
	}
}

// ::TODO
func (c *Connection) pendingExchangeMethodAdd() bool {
	return c.pendingMethods.Inc() > 0
}

// ::TODO
func (c *Connection) pendingExchangeMethodDone() {
	c.pendingMethods.Dec()
}

// 关闭connection, 这里的conn id无需使用, 因为本连接不能关闭其他连接吧
func (c *Connection) closeSendCh(connID uint32) {
	// ::TODO
	for !c.pendingMethods.CAS(0, math.MinInt32) {
		time.Sleep(time.Millisecond)
	}

	close(c.stopCh)
}

// ::TODO
func (c *Connection) checkExchanges() {
	c.callOnExchangeChange()

	moveState := func(fromState, toState connectionState) bool {
		err := c.withStateLock(func() error {
			if c.state != fromState {
				return errors.New("")
			}
			c.state = toState
			return nil
		})
		return err == nil
	}

	var updated connectionState
	if c.readState() == connectionStartClose {
		if !c.relay.canClose() {
			return
		}
		if c.inbound.count() == 0 && moveState(connectionStartClose, connectionInboundClosed) {
			updated = connectionInboundClosed
		}
		// If there was no update to the state, there's no more processing to do.
		if updated == 0 {
			return
		}
	}

	if c.readState() == connectionInboundClosed {
		// Safety check -- this should never happen since we already did the check
		// when transitioning to connectionInboundClosed.
		if !c.relay.canClose() {
			c.relay.logger.Error("Relay can't close even though state is InboundClosed.")
			return
		}

		if c.outbound.count() == 0 && moveState(connectionInboundClosed, connectionClosed) {
			updated = connectionClosed
		}
	}

	if updated != 0 {
		// If the connection is closed, we can safely close the channel.
		if updated == connectionClosed {
			go c.closeSendCh(c.connID)
		}

		c.log.WithFields(
			LogField{"newState", updated},
		).Debug("Connection state updated during shutdown.")
		c.callOnCloseStateChange()
	}
}

// 连接关闭, 并调用events中的OnCloseStateChange，执行channel的state变化，和channel.mutable.conns移除
func (c *Connection) close(fields ...LogField) error {
	c.log.WithFields(fields...).Info("Connection closing.")

	// Update the state which will start blocking incoming calls.
	if err := c.withStateLock(func() error {
		switch c.state {
		case connectionActive:
			c.state = connectionStartClose
		default:
			return fmt.Errorf("connection must be Active to Close")
		}
		return nil
	}); err != nil {
		return err
	}

	c.log.WithFields(
		LogField{"newState", c.readState()},
	).Debug("Connection state updated in Close.")
	c.callOnCloseStateChange()

	// ::TODO
	c.checkExchanges()

	return nil
}

// 关闭connection
func (c *Connection) Close() error {
	return c.close(LogField{"reason", "user initiated"})
}

// 关闭connection中的net.Conn连接, 并停止做connection的健康检查
func (c *Connection) closeNetwork() {
	c.log.Debugf("Closing underlying network connection")
	c.stopHealthCheck()
	c.closeNetworkCalled.Inc()
	if err := c.conn.Close(); err != nil {
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			ErrField(err),
		).Warn("Couldn't close connection to peer.")
	}
}

// 获取connection的最后活跃时间
func (c *Connection) getLastActivityTime() time.Time {
	return time.Unix(0, c.lastActivity.Load())
}
