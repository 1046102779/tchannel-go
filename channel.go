// Copyright (c) 2015 1046102779 Technologies, Inc.

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
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/1046102779/tchannel-go/tnet"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/atomic"
	"golang.org/x/net/context"
)

var (
	errAlreadyListening  = errors.New("channel already listening")
	errInvalidStateForOp = errors.New("channel is in an invalid state for that method")
	errMaxIdleTimeNotSet = errors.New("IdleCheckInterval is set but MaxIdleTime is zero")

	// ErrNoServiceName is returned when no service name is provided when
	// creating a new channel.
	ErrNoServiceName = errors.New("no service name provided")
)

const ephemeralHostPort = "0.0.0.0:0"

// ChannelOptions are used to control parameters on a create a TChannel
type ChannelOptions struct {
	// Default Connection options
	DefaultConnectionOptions ConnectionOptions

	// The name of the process, for logging and reporting to peers
	ProcessName string

	// OnPeerStatusChanged is an optional callback that receives a notification
	// whenever the channel establishes a usable connection to a peer, or loses
	// a connection to a peer.
	OnPeerStatusChanged func(*Peer)

	// The logger to use for this channel
	Logger Logger

	// The host:port selection implementation to use for relaying. This is an
	// unstable API - breaking changes are likely.
	RelayHost RelayHost

	// The list of service names that should be handled locally by this channel.
	// This is an unstable API - breaking changes are likely.
	RelayLocalHandlers []string

	// The maximum allowable timeout for relayed calls (longer timeouts are
	// clamped to this value). Passing zero uses the default of 2m.
	// This is an unstable API - breaking changes are likely.
	RelayMaxTimeout time.Duration

	// RelayTimerVerification will disable pooling of relay timers, and instead
	// verify that timers are not used once they are released.
	// This is an unstable API - breaking changes are likely.
	RelayTimerVerification bool

	// The reporter to use for reporting stats for this channel.
	StatsReporter StatsReporter

	// TimeNow is a variable for overriding time.Now in unit tests.
	// Note: This is not a stable part of the API and may change.
	TimeNow func() time.Time

	// TimeTicker is a variable for overriding time.Ticker in unit tests.
	// Note: This is not a stable part of the API and may change.
	TimeTicker func(d time.Duration) *time.Ticker

	// MaxIdleTime controls how long we allow an idle connection to exist
	// before tearing it down. Must be set to non-zero if IdleCheckInterval
	// is set.
	MaxIdleTime time.Duration

	// IdleCheckInterval controls how often the channel runs a sweep over
	// all active connections to see if they can be dropped. Connections that
	// are idle for longer than MaxIdleTime are disconnected. If this is set to
	// zero (the default), idle checking is disabled.
	IdleCheckInterval time.Duration

	// Tracer is an OpenTracing Tracer used to manage distributed tracing spans.
	// If not set, opentracing.GlobalTracer() is used.
	Tracer opentracing.Tracer

	// Handler is an alternate handler for all inbound requests, overriding the
	// default handler that delegates to a subchannel.
	Handler Handler
}

// ChannelState is the state of a channel.
type ChannelState int

const (
	// ChannelClient is a channel that can be used as a client.
	ChannelClient ChannelState = iota + 1

	// ChannelListening is a channel that is listening for new connnections.
	ChannelListening

	// ChannelStartClose is a channel that has received a Close request.
	// The channel is no longer listening, and all new incoming connections are rejected.
	ChannelStartClose

	// ChannelInboundClosed is a channel that has drained all incoming connections, but may
	// have outgoing connections. All incoming calls and new outgoing calls are rejected.
	ChannelInboundClosed

	// ChannelClosed is a channel that has closed completely.
	ChannelClosed
)

//go:generate stringer -type=ChannelState

// A Channel is a bi-directional connection to the peering and routing network.
// Applications can use a Channel to make service calls to remote peers via
// BeginCall, or to listen for incoming calls from peers.  Applications that
// want to receive requests should call one of Serve or ListenAndServe
// TODO(prashant): Shutdown all subchannels + peers when channel is closed.
type Channel struct {
	channelConnectionCommon

	chID                uint32
	createdStack        string
	connectionOptions   ConnectionOptions
	peers               *PeerList
	relayHost           RelayHost
	relayMaxTimeout     time.Duration
	relayTimerVerify    bool
	handler             Handler
	onPeerStatusChanged func(*Peer)

	// mutable contains all the members of Channel which are mutable.
	mutable struct {
		sync.RWMutex    // protects members of the mutable struct.
		state           ChannelState
		peerInfo        LocalPeerInfo // May be ephemeral if this is a client only channel
		commonStatsTags map[string]string
		// 服务注册的服务监听客户端请求
		l         net.Listener // May be nil if this is a client only channel
		idleSweep *idleSweep
		conns     map[uint32]*Connection
	}
}

// channelConnectionCommon is the list of common objects that both use
// and can be copied directly from the channel to the connection.
type channelConnectionCommon struct {
	log           Logger
	relayLocal    map[string]struct{}
	statsReporter StatsReporter
	tracer        opentracing.Tracer
	subChannels   *subChannelMap
	timeNow       func() time.Time
	timeTicker    func(time.Duration) *time.Ticker
}

// _nextChID is used to allocate unique IDs to every channel for debugging purposes.
var _nextChID atomic.Uint32

// Tracer returns the OpenTracing Tracer for this channel. If no tracer was provided
// in the configuration, returns opentracing.GlobalTracer(). Note that this approach
// allows opentracing.GlobalTracer() to be initialized _after_ the channel is created.
func (ccc channelConnectionCommon) Tracer() opentracing.Tracer {
	if ccc.tracer != nil {
		return ccc.tracer
	}
	return opentracing.GlobalTracer()
}

var defaultChannelFn = func(opts *ChannelOptions) {
	if opts.TimeNow == nil {
		opts.TimeNow = time.Now
	}
	if opts.TimeTicker == nil {
		opts.TimeTicker = time.NewTicker
	}
	if opts.StatsReporter == nil {
		opts.StatsReporter = NullStatsReporter
	}
	if opts.Logger == nil {
		opts.Logger = NullLogger
	}
	if opts.ProcessName == "" {
		opts.ProcessName = fmt.Sprintf("%s[%d]", filepath.Base(os.Args[0]), os.Getpid())
	}
}

// 服务注册，创建一个Channel实例
//
// 这里使用了函数参数设置
func NewChannel(serviceName string, optFns ...func(*ChannelOptions)) (*Channel, error) {
	// 注册服务的服务名不能为空
	if serviceName == "" {
		return nil, ErrNoServiceName
	}
	// 末尾加上一个设置，主要是如果有些channel实例的参数没有设置，需要设置一个默认值
	opts := &ChannelOptions{}
	optFns = append(optFns, defaultChannelFn)

	for _, optFn := range optFns {
		optFn(opts)
	}

	// 获取一个channel id, 自增长
	chID := _nextChID.Inc()
	opts.Logger = opts.Logger.WithFields(
		LogField{"serviceName", serviceName},
		LogField{"process", opts.ProcessName},
		LogField{"chID", chID},
	)

	// 校验空闲连接检测是否符合, idle sweep
	if err := opts.validateIdleCheck(); err != nil {
		return nil, err
	}

	ch := &Channel{
		// 大多数传入的channelOptions都是空值
		channelConnectionCommon: channelConnectionCommon{
			log:           opts.Logger,
			relayLocal:    toStringSet(opts.RelayLocalHandlers),
			statsReporter: opts.StatsReporter,
			subChannels:   &subChannelMap{},
			timeNow:       opts.TimeNow,
			timeTicker:    opts.TimeTicker,
			tracer:        opts.Tracer,
		},
		chID: chID,
		// 相关连接属性
		connectionOptions: opts.DefaultConnectionOptions.withDefaults(),
		relayHost:         opts.RelayHost,
		relayMaxTimeout:   validateRelayMaxTimeout(opts.RelayMaxTimeout, opts.Logger),
		relayTimerVerify:  opts.RelayTimerVerification,
	}
	ch.peers = newRootPeerList(ch, opts.OnPeerStatusChanged).newChild()

	// 该注册的服务端底层tchannel获取到一个rpc client的请求帧时，会通过arg1参数method找到对应的handler，也就是channel.handler函数，并通过reflect进行拼装上层业务逻辑处理函数, 进行业务逻辑处理, 并返回response到rpc client
	if opts.Handler != nil {
		ch.handler = opts.Handler
	} else {
		ch.handler = channelHandler{ch}
	}

	// 该服务注册的相关信息, 包括：服务进程名，go版本号，tchannel协议版本号和注册的服务名
	ch.mutable.peerInfo = LocalPeerInfo{
		PeerInfo: PeerInfo{
			ProcessName: opts.ProcessName,
			HostPort:    ephemeralHostPort,
			IsEphemeral: true,
			Version: PeerVersion{
				Language:        "go",
				LanguageVersion: strings.TrimPrefix(runtime.Version(), "go"),
				TChannelVersion: VersionInfo,
			},
		},
		ServiceName: serviceName,
	}
	// 该服务是为rpc client提供服务的。
	ch.mutable.state = ChannelClient
	// 该服务的所有连接管理, 每个msg id对应一个connection
	ch.mutable.conns = make(map[uint32]*Connection)
	ch.createCommonStats()

	// 这个用于两个内部统计: 1.  TChannel internal state; 2. Golang runtime stats
	if opts.Handler == nil {
		ch.registerInternal()
	}

	// 注册该服务channel到全局channelMap中
	registerNewChannel(ch)

	if opts.RelayHost != nil {
		opts.RelayHost.SetChannel(ch)
	}

	// 起一个goroutine，进行空闲连接检测和回收
	ch.mutable.idleSweep = startIdleSweep(ch, opts)

	return ch, nil
}

// 返回channel的通用连接参数
func (ch *Channel) ConnectionOptions() *ConnectionOptions {
	return &ch.connectionOptions
}

// rpc service有两种服务提供方式，1. Serve方法；2. ListenAndServe方法
func (ch *Channel) Serve(l net.Listener) error {
	mutable := &ch.mutable
	mutable.Lock()
	defer mutable.Unlock()

	if mutable.l != nil {
		return errAlreadyListening
	}
	mutable.l = tnet.Wrap(l)

	// 创建时，服务注册端的服务是为rpc client提供服务的
	if mutable.state != ChannelClient {
		return errInvalidStateForOp
	}
	// 当服务启动时，在改为服务监听rpc client请求
	mutable.state = ChannelListening

	// 服务端的host:port
	mutable.peerInfo.HostPort = l.Addr().String()
	mutable.peerInfo.IsEphemeral = false
	ch.log = ch.log.WithFields(LogField{"hostPort", mutable.peerInfo.HostPort})
	ch.log.Info("Channel is listening.")
	// 起goroutine，用于监听rpc client，并分发处理请求
	go ch.serve()
	return nil
}

// 第二种提供服务监听的方式, 创建listener, 再调用上面的Serve(net.Listener)
func (ch *Channel) ListenAndServe(hostPort string) error {
	mutable := &ch.mutable
	mutable.RLock()

	if mutable.l != nil {
		mutable.RUnlock()
		return errAlreadyListening
	}

	l, err := net.Listen("tcp", hostPort)
	if err != nil {
		mutable.RUnlock()
		return err
	}

	mutable.RUnlock()
	return ch.Serve(l)
}

// 服务提供的所有服务方法注册接口
type Registrar interface {
	// 获取服务注册的服务名
	ServiceName() string

	// 注册服务提供的所有服务方法和对应的处理方法映射
	Register(h Handler, methodName string)

	// 返回服务注册的日志写入实例
	Logger() Logger

	// 统计报告
	StatsReporter() StatsReporter

	// 统计tags
	StatsTags() map[string]string

	// Peers returns the peer list for this Registrar.
	Peers() *PeerList
}

// 服务注册通过NewChannel方法获取的channel实例，实现了Registrar interface
func (ch *Channel) Register(h Handler, methodName string) {
	if _, ok := ch.handler.(channelHandler); !ok {
		panic("can't register handler when channel configured with alternate root handler")
	}
	// 通过服务注册名称，获取subchannel，然后进行服务注册
	//
	// 我们可以看到subchannel也实现了Registrar interface
	ch.GetSubChannel(ch.PeerInfo().ServiceName).Register(h, methodName)
}

// 获取服务注册的服务信息, 包括：服务名称，服务的host:port, 进程名称, 以及语言版本，协议版本等
func (ch *Channel) PeerInfo() LocalPeerInfo {
	ch.mutable.RLock()
	peerInfo := ch.mutable.peerInfo
	ch.mutable.RUnlock()

	return peerInfo
}

// 在服务注册方获取一些通用信息，包括进程名称、服务名称和主机名称, 存储在channel中
func (ch *Channel) createCommonStats() {
	ch.mutable.commonStatsTags = map[string]string{
		"app":     ch.mutable.peerInfo.ProcessName,
		"service": ch.mutable.peerInfo.ServiceName,
	}
	host, err := os.Hostname()
	if err != nil {
		ch.log.WithFields(ErrField(err)).Info("Channel creation failed to get host.")
		return
	}
	ch.mutable.commonStatsTags["host"] = host
}

// 一个通过NewChannel方法进行服务注册的channel，可以拥有map[string]*SubChannel, 这个后面再看
// ::TODO
// 通过服务名获取subchannel, 如果是新建subchannel，则需要为subchannel实例设置一些参数
func (ch *Channel) GetSubChannel(serviceName string, opts ...SubChannelOption) *SubChannel {
	sub, added := ch.subChannels.getOrAdd(serviceName, ch)
	if added {
		for _, opt := range opts {
			opt(sub)
		}
	}
	return sub
}

// 返回该channel与其他建立connection的所有Peers
func (ch *Channel) Peers() *PeerList {
	return ch.peers
}

// 所有的Peers都在channel的RootPeerList中存在，返回root的PeerList
func (ch *Channel) RootPeers() *RootPeerList {
	return ch.peers.parent
}

// 当rpc client进行服务调用时，第一步需要在协议帧的非业务数据上写入服务名、方法名和其他参数设置, 例如：协议帧的headers各个参数
//
// 协议帧的body部分通过, 通过上层具体的the arg scheme format：json，thrift，http，来进行参数封装写入，以及阻塞读取rpc service handler的response返回
// 对于peer相关 封装一个对外调用的OutboundCall，这包含了frame的消息帧arg1
func (ch *Channel) BeginCall(ctx context.Context, hostPort, serviceName, methodName string, callOptions *CallOptions) (*OutboundCall, error) {
	p := ch.RootPeers().GetOrAdd(hostPort)
	return p.BeginCall(ctx, serviceName, methodName, callOptions)
}

// 上面的channel的Serve和ListenAndServe方法会最后通过go serve方式调用
func (ch *Channel) serve() {
	acceptBackoff := 0 * time.Millisecond

	for {
		// 接收来自rpc client的请求
		netConn, err := ch.mutable.l.Accept()
		if err != nil {
			// 如果出错，睡眠的时间逐渐增大到一个最大值后，会接收client请求处理速度变慢
			// 如果出错了，下次正常接收，则accepBackoff为0，影响降为0
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if acceptBackoff == 0 {
					acceptBackoff = 5 * time.Millisecond
				} else {
					acceptBackoff *= 2
				}
				if max := 1 * time.Second; acceptBackoff > max {
					acceptBackoff = max
				}
				ch.log.WithFields(
					ErrField(err),
					LogField{"backoff", acceptBackoff},
				).Warn("Accept error, will wait and retry.")
				time.Sleep(acceptBackoff)
				continue
			} else {
				// Only log an error if this didn't happen due to a Close.
				if ch.State() >= ChannelStartClose {
					return
				}
				ch.log.WithFields(ErrField(err)).Fatal("Unrecoverable accept error, closing server.")
				return
			}
		}

		acceptBackoff = 0

		// 通过Accept获取的connection, 解析协议帧的headers
		go func() {
			// 注册connection的state变化时的触发事件
			events := connectionEvents{
				OnActive:           ch.inboundConnectionActive,
				OnCloseStateChange: ch.connectionCloseStateChange,
				OnExchangeUpdated:  ch.exchangeUpdated,
			}
			// 解析协议帧，并创建channel层面可以管理的connection，并进行健康检查、
			// 监听连接上的流入数据和流出数据
			//
			// 返回的connection，并没有在channel中存储管理起来，而是connection自身管理的
			if _, err := ch.inboundHandshake(context.Background(), netConn, events); err != nil {
				netConn.Close()
			}
		}()
	}
}

// 服务端主动与host:port已经建立的connection，发送一个ping请求，看是否有response
func (ch *Channel) Ping(ctx context.Context, hostPort string) error {
	peer := ch.RootPeers().GetOrAdd(hostPort)
	conn, err := peer.GetConnection(ctx)
	if err != nil {
		return err
	}

	return conn.ping(ctx)
}

// 返回channel的logger日志
func (ch *Channel) Logger() Logger {
	return ch.log
}

// 返回channel的数据统计报告
func (ch *Channel) StatsReporter() StatsReporter {
	return ch.statsReporter
}

// 快照一份tags数据
func (ch *Channel) StatsTags() map[string]string {
	m := make(map[string]string)
	ch.mutable.RLock()
	for k, v := range ch.mutable.commonStatsTags {
		m[k] = v
	}
	ch.mutable.RUnlock()
	return m
}

// 返回注册服务的服务名称
func (ch *Channel) ServiceName() string {
	return ch.PeerInfo().ServiceName
}

// 服务端主动向host:port发起一个connection
//
// 几处地方说过：channel.mutable.conns连接管理是通过events事件的outboundConnectionActive进行维护得
func (ch *Channel) Connect(ctx context.Context, hostPort string) (*Connection, error) {
	switch state := ch.State(); state {
	case ChannelClient, ChannelListening:
		break
	default:
		ch.log.Debugf("Connect rejecting new connection as state is %v", state)
		return nil, errInvalidStateForOp
	}

	// The context timeout applies to the whole call, but users may want a lower
	// connect timeout (e.g. for streams).
	if params := getTChannelParams(ctx); params != nil && params.connectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, params.connectTimeout)
		defer cancel()
	}

	events := connectionEvents{
		OnActive:           ch.outboundConnectionActive,
		OnCloseStateChange: ch.connectionCloseStateChange,
		OnExchangeUpdated:  ch.exchangeUpdated,
	}

	if err := ctx.Err(); err != nil {
		return nil, GetContextError(err)
	}

	timeout := getTimeout(ctx)
	// 服务端主动拨号给host:port, 返回一个tcp连接
	tcpConn, err := dialContext(ctx, hostPort)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			ch.log.WithFields(
				LogField{"remoteHostPort", hostPort},
				LogField{"timeout", timeout},
			).Info("Outbound net.Dial timed out.")
			err = ErrTimeout
		} else if ctx.Err() == context.Canceled {
			ch.log.WithFields(
				LogField{"remoteHostPort", hostPort},
			).Info("Outbound net.Dial was cancelled.")
			err = GetContextError(ErrRequestCancelled)
		} else {
			ch.log.WithFields(
				ErrField(err),
				LogField{"remoteHostPort", hostPort},
			).Info("Outbound net.Dial failed.")
		}
		return nil, err
	}

	// 在channel中起了一个goroutine, 通过serve去accept rpc client的请求，并通过inboundHandshake处理流入的请求connection
	//
	// 这里是服务端主动向host:port发起一个连接请求, 最后还是通过newConnection方法创建connection，并起3个goroutine，健康检查，接收和发送数据
	conn, err := ch.outboundHandshake(ctx, tcpConn, hostPort, events)
	if conn != nil {
		// It's possible that the connection we just created responds with a host:port
		// that is not what we tried to connect to. E.g., we may have connected to
		// 127.0.0.1:1234, but the returned host:port may be 10.0.0.1:1234.
		// In this case, the connection won't be added to 127.0.0.1:1234 peer
		// and so future calls to that peer may end up creating new connections. To
		// avoid this issue, and to avoid clients being aware of any TCP relays, we
		// add the connection to the intended peer.
		if hostPort != conn.remotePeerInfo.HostPort {
			conn.log.Debugf("Outbound connection host:port mismatch, adding to peer %v", conn.remotePeerInfo.HostPort)
			ch.addConnectionToPeer(hostPort, conn, outbound)
		}
	}

	return conn, err
}

// exchangeUpdated updates the peer heap.
func (ch *Channel) exchangeUpdated(c *Connection) {
	if c.remotePeerInfo.HostPort == "" {
		// Hostport is unknown until we get init resp.
		return
	}

	p, ok := ch.RootPeers().Get(c.remotePeerInfo.HostPort)
	if !ok {
		return
	}

	ch.updatePeer(p)
}

// updatePeer方法更新Peer，包括PeerScore相关
func (ch *Channel) updatePeer(p *Peer) {
	ch.peers.onPeerChange(p)
	ch.subChannels.updatePeer(p)
	p.callOnUpdateComplete()
}

// 连接的增加是通过创建connection时绑定在channel的events:onActive进行connection的添加，最终会调用addConnection
func (ch *Channel) addConnection(c *Connection) bool {
	ch.mutable.Lock()
	defer ch.mutable.Unlock()

	// 连接增加到channel.mutable.conns时，会校验连接是否为active状态
	if c.readState() != connectionActive {
		return false
	}

	// 如果服务注册的服务channel，没有准备好对rpc client提供监听服务，则拒绝接收connection
	switch state := ch.mutable.state; state {
	case ChannelClient, ChannelListening:
		break
	default:
		return false
	}

	ch.mutable.conns[c.connID] = c
	return true
}

// 每个建立的连接都是有方向的：流入和流出
//
// 例如：服务注册的服务channel，accept rpc client请求，则是流入;
// 如果服务注册的服务channel，主动发起outboundHandshake或者ping host:port请求，则流出
func (ch *Channel) connectionActive(c *Connection, direction connectionDirection) {
	c.log.Debugf("New active %v connection for peer %v", direction, c.remotePeerInfo.HostPort)

	if added := ch.addConnection(c); !added {
		// The channel isn't in a valid state to accept this connection, close the connection.
		c.close(LogField{"reason", "new active connection on closing channel"})
		return
	}

	// 增加一个新的connection到相应的Peer中direction数组中
	ch.addConnectionToPeer(c.remotePeerInfo.HostPort, c, direction)
}

// 增加服务channel与Peers的connections中
func (ch *Channel) addConnectionToPeer(hostPort string, c *Connection, direction connectionDirection) {
	p := ch.RootPeers().GetOrAdd(hostPort)
	if err := p.addConnection(c, direction); err != nil {
		c.log.WithFields(
			LogField{"remoteHostPort", c.remotePeerInfo.HostPort},
			LogField{"direction", direction},
			ErrField(err),
		).Warn("Failed to add connection to peer.")
	}

	ch.updatePeer(p)
}

// 当建立的connection为流入到服务channel端时，则方向为inbound
//
// 也就是服务注册的服务channel accept了一个新连接
func (ch *Channel) inboundConnectionActive(c *Connection) {
	ch.connectionActive(c, inbound)
}

// 当建立的connection为服务channel主动向host:port发起一个请求，则方向为outbound
//
// 也就是服务注册的服务channel作为rpc client发起一个请求
func (ch *Channel) outboundConnectionActive(c *Connection) {
	ch.connectionActive(c, outbound)
}

// 当连接关闭时，则会触发channel.events的onClosed事件，移除连接
//
// 比如：goroutine idle sweep剔除超时空闲连接
func (ch *Channel) removeClosedConn(c *Connection) {
	if c.readState() != connectionClosed {
		return
	}

	ch.mutable.Lock()
	delete(ch.mutable.conns, c.connID)
	ch.mutable.Unlock()
}

// 获取channel.mutable.conns管理所有连接中的最小状态
//
// 这个主要用于：设置channel.mutable.state状态，它是与channel.mutable.conns所有连接相关联的
// 前者的状态是conns所有状态中的最小状态
func (ch *Channel) getMinConnectionState() connectionState {
	minState := connectionClosed
	for _, c := range ch.mutable.conns {
		if s := c.readState(); s < minState {
			minState = s
		}
	}
	return minState
}

// 当关闭一个connection时，就会触发从channel中移除连接，
// 并且可能还要修改channel.mutable.state的状态值
func (ch *Channel) connectionCloseStateChange(c *Connection) {
	// 从channel.mutable.conns中移除connection
	ch.removeClosedConn(c)
	// 获取channel与host:port建立的remote service peer, 关闭操作
	if peer, ok := ch.RootPeers().Get(c.remotePeerInfo.HostPort); ok {
		peer.connectionCloseStateChange(c)
		ch.updatePeer(peer)
	}
	if c.outboundHP != "" && c.outboundHP != c.remotePeerInfo.HostPort {
		// Outbound connections may be in multiple peers.
		if peer, ok := ch.RootPeers().Get(c.outboundHP); ok {
			peer.connectionCloseStateChange(c)
			ch.updatePeer(peer)
		}
	}

	chState := ch.State()
	if chState != ChannelStartClose && chState != ChannelInboundClosed {
		return
	}

	ch.mutable.RLock()
	minState := ch.getMinConnectionState()
	ch.mutable.RUnlock()

	var updateTo ChannelState
	if minState >= connectionClosed {
		updateTo = ChannelClosed
	} else if minState >= connectionInboundClosed && chState == ChannelStartClose {
		updateTo = ChannelInboundClosed
	}

	var updatedToState ChannelState
	if updateTo > 0 {
		ch.mutable.Lock()
		// Recheck the state as it's possible another goroutine changed the state
		// from what we expected, and so we might make a stale change.
		if ch.mutable.state == chState {
			ch.mutable.state = updateTo
			updatedToState = updateTo
		}
		ch.mutable.Unlock()
		chState = updateTo
	}

	c.log.Debugf("ConnectionCloseStateChange channel state = %v connection minState = %v",
		chState, minState)

	// 如果channel的state状态为ChannelClosed关闭状态，则从全局channelMap中移除该channel
	if updatedToState == ChannelClosed {
		ch.onClosed()
	}
}

// 从channelMap中移除该channel
func (ch *Channel) onClosed() {
	removeClosedChannel(ch)
	ch.log.Infof("Channel closed.")
}

// 校验channel state是否为ChannelClosed
func (ch *Channel) Closed() bool {
	return ch.State() == ChannelClosed
}

// 获取channel的状态, 也就是所有连接的最小状态
func (ch *Channel) State() ChannelState {
	ch.mutable.RLock()
	state := ch.mutable.state
	ch.mutable.RUnlock()

	return state
}

// Close starts a graceful Close for the channel. This does not happen immediately:
// 1. This call closes the Listener and starts closing connections.
// 2. When all incoming connections are drained, the connection blocks new outgoing calls.
// 3. When all connections are drained, the channel's state is updated to Closed.
//
// 1. channel主动关闭，首先会关闭服务端的监听listener，不会再Accept新请求
// 2. idle sweep停止对所有连接进行超时检测, 该goroutine退出
func (ch *Channel) Close() {
	ch.Logger().Info("Channel.Close called.")
	var connections []*Connection
	var channelClosed bool

	ch.mutable.Lock()

	if ch.mutable.l != nil {
		ch.mutable.l.Close()
	}

	// Stop the idle connections timer.
	ch.mutable.idleSweep.Stop()

	// 所有的连接开始关闭, 当所有连接关闭时，则执行从channelMap中移除channel操作
	ch.mutable.state = ChannelStartClose
	if len(ch.mutable.conns) == 0 {
		ch.mutable.state = ChannelClosed
		channelClosed = true
	}
	for _, c := range ch.mutable.conns {
		connections = append(connections, c)
	}
	ch.mutable.Unlock()

	for _, c := range connections {
		c.close(LogField{"reason", "channel closing"})
	}

	if channelClosed {
		ch.onClosed()
	}
}

// RelayHost returns the channel's RelayHost, if any.
// ::TODO
func (ch *Channel) RelayHost() RelayHost {
	return ch.relayHost
}

// 如果channel options参数的空闲连接检查间隔时间大于0， 但是空闲连接时间长度小于0， 则无法启动goroutine idle sweep, 则返回错误
func (o *ChannelOptions) validateIdleCheck() error {
	if o.IdleCheckInterval > 0 && o.MaxIdleTime <= 0 {
		return errMaxIdleTimeNotSet
	}

	return nil
}

// 把slice改为字典hash
func toStringSet(ss []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		set[s] = struct{}{}
	}
	return set
}
