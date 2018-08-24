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
	"fmt"
	"sync"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
)

// SubChannelOption are used to set options for subchannels.
type SubChannelOption func(*SubChannel)

// Isolated is a SubChannelOption that creates an isolated subchannel.
func Isolated(s *SubChannel) {
	s.Lock()
	s.peers = s.topChannel.peers.newSibling()
	s.peers.SetStrategy(newLeastPendingCalculator())
	s.Unlock()
}

// SubChannel存储remote service与对应的method业务逻辑处理方法列表
// SubChannel实现了Registrar interface
type SubChannel struct {
	sync.RWMutex
	serviceName        string
	topChannel         *Channel
	defaultCallOptions *CallOptions
	peers              *PeerList
	handler            Handler
	logger             Logger
	statsReporter      StatsReporter
}

// Map of subchannel and the corresponding service
type subChannelMap struct {
	sync.RWMutex
	subchannels map[string]*SubChannel
}

// newSubChannel方法创建一个SubChannel实例.
//
// 因为一个微服务大有可能会调用多个微服务，来完成一个请求动作， 比如：用户支付完成，就包括：支付单的完成、订单状态的修改、财务流水、日志记录, 和库存的扣减等, 一个动作需要很多微服务协作完成
// 那么这个微服务就需要调用多个微服务，需要知道微服务对外提供的rpc注册服务名称
// 每一个remote service都会有一个服务名称和一个对外提供的服务方法列表
//
// 所以存储channel所在的Peer，outbound call，则会有多个remote services，每个subchannel存储一个remote service
func newSubChannel(serviceName string, ch *Channel) *SubChannel {
	logger := ch.Logger().WithFields(LogField{"subchannel", serviceName})
	return &SubChannel{
		serviceName:   serviceName,
		peers:         ch.peers,
		topChannel:    ch,
		handler:       &handlerMap{}, // use handlerMap by default
		logger:        logger,
		statsReporter: ch.StatsReporter(),
	}
}

// ServiceName方法获取subchannel的remote service name
func (c *SubChannel) ServiceName() string {
	return c.serviceName
}

// BeginCall方法发起一个对SubChannel存储的remote service name和对应的请求处理方法
// 并填充协议帧的header、payload部分数据和arg1等数据
//
// 同时注意一点，既然是同一个remote service name， 为什么还存在PeerList呢？
// 答案是，一个服务名，为了容错和负载，保证service的高可用，则会存在多个相同实例，这个是由host:port确定的
// 也就是说，一个服务名称对应多个实例，每个实例都提供相同的服务
func (c *SubChannel) BeginCall(ctx context.Context, methodName string, callOptions *CallOptions) (*OutboundCall, error) {
	if callOptions == nil {
		callOptions = defaultCallOptions
	}

	// 从subchannel中的peerList中获取多实例中的一个实例peer
	// 这里采用了一个简单算法，就是尽可能地覆盖所以提供相同服务的实例，采用轮询策略
	peer, err := c.peers.Get(callOptions.RequestState.PrevSelectedPeers())
	if err != nil {
		return nil, err
	}

	// 发起封装帧，并返回OutboundCall，这里面都是通过reqres.go, fragmenting_writer.go, message.go, frame.go，typed等封装解析
	return peer.BeginCall(ctx, c.ServiceName(), methodName, callOptions)
}

// Peers方法获取remote service name的多实例列表
func (c *SubChannel) Peers() *PeerList {
	return c.peers
}

// Isolated方法返回channel与subchannel是否共享PeerList
func (c *SubChannel) Isolated() bool {
	c.RLock()
	defer c.RUnlock()
	return c.topChannel.Peers() != c.peers
}

// Register方法进行service对外提供的服务方法列表注册，
// 当rpc请求到达rpc service端时，会根据method找到对应的业务逻辑处理方法
//
// 则subchannel存在一个method:handler的map映射, 通过这个是在服务注册时，通过Registrar interface进行注册
func (c *SubChannel) Register(h Handler, methodName string) {
	handlers, ok := c.handler.(*handlerMap)
	if !ok {
		panic(fmt.Sprintf(
			"handler for SubChannel(%v) was changed to disallow method registration",
			c.ServiceName(),
		))
	}
	handlers.register(h, methodName)
}

// GetHandlers方法获取remote service所有对外提供rpc服务的方法列表
func (c *SubChannel) GetHandlers() map[string]Handler {
	handlers, ok := c.handler.(*handlerMap)
	if !ok {
		panic(fmt.Sprintf(
			"handler for SubChannel(%v) was changed to disallow method registration",
			c.ServiceName(),
		))
	}

	handlers.RLock()
	handlersMap := make(map[string]Handler, len(handlers.handlers))
	for k, v := range handlers.handlers {
		handlersMap[k] = v
	}
	handlers.RUnlock()
	return handlersMap
}

// SetHandler方法用于改变服务注册的method与提供服务的处理方法映射map
func (c *SubChannel) SetHandler(h Handler) {
	c.handler = h
}

// Logger方法用于返回日志实例
func (c *SubChannel) Logger() Logger {
	return c.logger
}

// StatsReporter方法返回统计报告
func (c *SubChannel) StatsReporter() StatsReporter {
	return c.topChannel.StatsReporter()
}

// StatsTags方法设置subchannel映射的service name
func (c *SubChannel) StatsTags() map[string]string {
	tags := c.topChannel.StatsTags()
	tags["subchannel"] = c.serviceName
	return tags
}

// Tracer returns OpenTracing Tracer from the top channel.
func (c *SubChannel) Tracer() opentracing.Tracer {
	return c.topChannel.Tracer()
}

// Register方法用于新建一个channel下的subchannel，专门用来rpc client获取指定服务的client，为后面业务的需要做准备
//
func (subChMap *subChannelMap) registerNewSubChannel(serviceName string, ch *Channel) (_ *SubChannel, added bool) {
	subChMap.Lock()
	defer subChMap.Unlock()

	if subChMap.subchannels == nil {
		subChMap.subchannels = make(map[string]*SubChannel)
	}

	if sc, ok := subChMap.subchannels[serviceName]; ok {
		return sc, false
	}

	sc := newSubChannel(serviceName, ch)
	subChMap.subchannels[serviceName] = sc
	return sc, true
}

// Get subchannel if, we have one
func (subChMap *subChannelMap) get(serviceName string) (*SubChannel, bool) {
	subChMap.RLock()
	sc, ok := subChMap.subchannels[serviceName]
	subChMap.RUnlock()
	return sc, ok
}

// GetOrAdd方法用于获取channel下的指定service name的subchannel，并最后通过传入method，从多实例中找到个peer，然后发起rpc调用
//
// 这里要说明的是，subChannelMap存储在channel中，一个微服务如果想使用很多微服务提供的对外服务，则需要提供通过remote service name，创建client，并存储在channel中的subChannelMap下
func (subChMap *subChannelMap) getOrAdd(serviceName string, ch *Channel) (_ *SubChannel, added bool) {
	if sc, ok := subChMap.get(serviceName); ok {
		return sc, false
	}

	return subChMap.registerNewSubChannel(serviceName, ch)
}

func (subChMap *subChannelMap) updatePeer(p *Peer) {
	subChMap.RLock()
	for _, subCh := range subChMap.subchannels {
		if subCh.Isolated() {
			subCh.RLock()
			subCh.Peers().onPeerChange(p)
			subCh.RUnlock()
		}
	}
	subChMap.RUnlock()
}
