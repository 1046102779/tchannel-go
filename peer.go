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
	"container/heap"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/uber/tchannel-go/trand"

	"github.com/uber-go/atomic"
	"golang.org/x/net/context"
)

// RootPeerList存放自己的peer，并包含所有PeerList中的peer

var (
	// ErrInvalidConnectionState indicates that the connection is not in a valid state.
	// This may be due to a race between selecting the connection and it closing, so
	// it is a network failure that can be retried.
	ErrInvalidConnectionState = NewSystemError(ErrCodeNetwork, "connection is in an invalid state")

	// ErrNoPeers indicates that there are no peers.
	ErrNoPeers = errors.New("no peers available")

	// ErrPeerNotFound indicates that the specified peer was not found.
	ErrPeerNotFound = errors.New("peer not found")

	// ErrNoNewPeers indicates that no previously unselected peer is available.
	ErrNoNewPeers = errors.New("no new peer available")

	peerRng = trand.NewSeeded()
)

// Connectable interface用于发起并建立连接，一般都是使用的channel中Connect方法
type Connectable interface {
	// Connect tries to connect to the given hostPort.
	Connect(ctx context.Context, hostPort string) (*Connection, error)
	// Logger returns the logger to use.
	Logger() Logger
}

// PeerList维护着channel的子child列表
type PeerList struct {
	sync.RWMutex

	// 服务注册的服务channel的RootPeerList实例
	parent *RootPeerList
	// host:port映射的peerScore
	peersByHostPort map[string]*peerScore
	// peerHeap最小堆，主要用于score获取分值较高的peer
	peerHeap *peerHeap
	// score分值计算算法
	scoreCalculator ScoreCalculator
	//
	lastSelected uint64
}

// 创建RootPeerList的child
func newPeerList(root *RootPeerList) *PeerList {
	return &PeerList{
		parent:          root,
		peersByHostPort: make(map[string]*peerScore),
		// 分值计算算法
		scoreCalculator: newPreferIncomingCalculator(),
		// 创建PeerHeap最小堆
		peerHeap: newPeerHeap(),
	}
}

// SetStrategy自定义分值计算算法
func (l *PeerList) SetStrategy(sc ScoreCalculator) {
	l.Lock()
	defer l.Unlock()

	l.scoreCalculator = sc
	for _, ps := range l.peersByHostPort {
		// 重新计算PeerList中的所有peer分数值, 并更新PeerHeap
		newScore := l.scoreCalculator.GetScore(ps.Peer)
		l.updatePeer(ps, newScore)
	}
}

// 创建PeerList的兄弟PeerList
func (l *PeerList) newSibling() *PeerList {
	return newPeerList(l.parent)
}

// 添加一个host:port的Peer到PeerList中
func (l *PeerList) Add(hostPort string) *Peer {
	l.Lock()
	defer l.Unlock()

	if p, ok := l.peersByHostPort[hostPort]; ok {
		return p.Peer
	}

	// 添加host:port的Peer到RootPeerList
	p := l.parent.Add(hostPort)
	// 创建一个Peer的引用. PeerList
	p.addSC()
	// 新建PeerScore
	ps := newPeerScore(p, l.scoreCalculator.GetScore(p))

	// 添加PeerScore到PeerList
	l.peersByHostPort[hostPort] = ps
	// 添加PeerScore到PeerHeap
	l.peerHeap.addPeer(ps)
	return p
}

// 从PeerList找一个在prevSelected不存在的分值最小的Peer
func (l *PeerList) GetNew(prevSelected map[string]struct{}) (*Peer, error) {
	l.Lock()
	defer l.Unlock()
	if l.peerHeap.Len() == 0 {
		return nil, ErrNoPeers
	}

	// 先是小范围搜索，在大范围搜索
	peer := l.choosePeer(prevSelected, false /* avoidHost */)
	if peer == nil {
		peer = l.choosePeer(prevSelected, true /* avoidHost */)
	}
	if peer == nil {
		return nil, ErrNoNewPeers
	}
	return peer, nil
}

// 在PeerList找到一个在prevSelected不存在的PeerHeap
func (l *PeerList) Get(prevSelected map[string]struct{}) (*Peer, error) {
	peer, err := l.GetNew(prevSelected)
	if err == ErrNoNewPeers {
		l.Lock()
		// 直接取一个最小堆第一个元素Peer
		peer = l.choosePeer(nil, false /* avoidHost */)
		l.Unlock()
	} else if err != nil {
		return nil, err
	}
	if peer == nil {
		return nil, ErrNoPeers
	}
	return peer, nil
}

// 从PeerList移除host:port
func (l *PeerList) Remove(hostPort string) error {
	l.Lock()
	defer l.Unlock()

	// 不存在，则直接返回错误; 否则，PeerList移除host:port
	p, ok := l.peersByHostPort[hostPort]
	if !ok {
		return ErrPeerNotFound
	}

	// Peer的引用-1，因为从PeerList移除了
	p.delSC()
	delete(l.peersByHostPort, hostPort)
	// 从PeerHeap移除host:port
	l.peerHeap.removePeer(p)

	return nil
}

// 从PeerList的最小堆HeapPeer中找到在prevSelected不存在的peer
func (l *PeerList) choosePeer(prevSelected map[string]struct{}, avoidHost bool) *Peer {
	var psPopList []*peerScore
	var ps *peerScore

	// 函数变量
	canChoosePeer := func(hostPort string) bool {
		if _, ok := prevSelected[hostPort]; ok {
			return false
		}
		if avoidHost {
			if _, ok := prevSelected[getHost(hostPort)]; ok {
				return false
			}
		}
		return true
	}

	// 找到一个在prevSelected不存在的peer，并break
	size := l.peerHeap.Len()
	for i := 0; i < size; i++ {
		popped := l.peerHeap.popPeer()

		if canChoosePeer(popped.HostPort()) {
			ps = popped
			break
		}
		psPopList = append(psPopList, popped)
	}

	// 把从PeerHeap最小堆中移除的peer列表，重新添加到最小堆中
	for _, p := range psPopList {
		heap.Push(l.peerHeap, p)
	}

	if ps == nil {
		return nil
	}

	// 添加找到的peerScore
	l.peerHeap.pushPeer(ps)
	// 选择peer的次数加1
	ps.chosenCount.Inc()
	return ps.Peer
}

// GetOrAdd方法，获取host:port的peer，如果不存在，则直接添加
func (l *PeerList) GetOrAdd(hostPort string) *Peer {
	if ps, ok := l.exists(hostPort); ok {
		return ps.Peer
	}
	return l.Add(hostPort)
}

// 获取PeerList的快照克隆
func (l *PeerList) Copy() map[string]*Peer {
	l.RLock()
	defer l.RUnlock()

	listCopy := make(map[string]*Peer)
	for k, v := range l.peersByHostPort {
		listCopy[k] = v.Peer
	}
	return listCopy
}

// 获取PeerList的Peer数量
func (l *PeerList) Len() int {
	l.RLock()
	len := l.peerHeap.Len()
	l.RUnlock()
	return len
}

// 从PeerList获取key为host:port的PeerScore
func (l *PeerList) exists(hostPort string) (*peerScore, bool) {
	l.RLock()
	ps, ok := l.peersByHostPort[hostPort]
	l.RUnlock()

	return ps, ok
}

// 从PeerList获取key为host:port的PeerScore
func (l *PeerList) getPeerScore(hostPort string) (*peerScore, uint64, bool) {
	ps, ok := l.exists(hostPort)
	if !ok {
		return nil, 0, false
	}
	return ps, ps.score, ok
}

// 当Peer变化时，修改peer的Score分数值，并更新PeerHeap
func (l *PeerList) onPeerChange(p *Peer) {
	ps, score, ok := l.getPeerScore(p.hostPort)
	if !ok {
		return
	}
	sc := l.scoreCalculator

	// 如果分数值不变，则直接返回
	newScore := sc.GetScore(ps.Peer)
	if newScore == score {
		return
	}

	l.Lock()
	l.updatePeer(ps, newScore)
	l.Unlock()
}

// 更新PeerHeap的分数值
func (l *PeerList) updatePeer(ps *peerScore, newScore uint64) {
	if ps.score == newScore {
		return
	}

	ps.score = newScore
	l.peerHeap.updatePeer(ps)
}

// peerScore
type peerScore struct {
	*Peer

	// 最小堆的各个节点值
	score uint64
	// index为peerScore在container/heap的位置
	index int
	// order is the tiebreaker for when score is equal. It is set when a peer
	// is pushed to the heap based on peerHeap.order with jitter.
	order uint64
}

// 创建一个PeerScore实例
func newPeerScore(p *Peer, score uint64) *peerScore {
	return &peerScore{
		Peer:  p,
		score: score,
		index: -1,
	}
}

// Peer host:port与别人建立好的connections维护, 包括inbound和outbound direction
type Peer struct {
	sync.RWMutex

	// 这个Connectable interface继承Channel的Connect方法
	channel Connectable
	// Peer host:port
	hostPort string
	// 当Peer发生变化的事件
	onStatusChanged func(*Peer)
	// 当Peer关闭时的事件
	onClosedConnRemoved func(*Peer)

	// scCount是一个计数器Peer
	scCount uint32

	// connections are mutable, and are protected by the mutex.
	newConnLock sync.Mutex
	// Channel发起与该Peer建立的connections连接池
	inboundConnections []*Connection
	// 该Peer发起与服务注册的服务Channel建立的connections连接池
	outboundConnections []*Connection
	chosenCount         atomic.Uint64

	// onUpdate is a test-only hook.
	onUpdate func(*Peer)
}

// 创建Peer实例
func newPeer(channel Connectable,
	hostPort string,
	onStatusChanged func(*Peer),
	onClosedConnRemoved func(*Peer)) *Peer {
	if hostPort == "" {
		panic("Cannot create peer with blank hostPort")
	}
	if onStatusChanged == nil {
		onStatusChanged = noopOnStatusChanged
	}
	return &Peer{
		channel:             channel,
		hostPort:            hostPort,
		onStatusChanged:     onStatusChanged,
		onClosedConnRemoved: onClosedConnRemoved,
	}
}

// HostPort方法返回Peer的host:port
func (p *Peer) HostPort() string {
	return p.hostPort
}

// 注意：对于Peer的inbound和outbound的connections两个列表，
// 在取一个index的connection时，是把两个connection合成一个列表，inbound在前，outbound在后。
func (p *Peer) getConn(i int) *Connection {
	// 如果上面的index在inbound connections范围内，则直接返回
	// 否则，用index减去inbound connections的长度
	inboundLen := len(p.inboundConnections)
	if i < inboundLen {
		return p.inboundConnections[i]
	}

	return p.outboundConnections[i-inboundLen]
}

// 从Peer中获取一个active connection，因为一个connection是双向的，所以与Peer-to-Peer的发起者无关，也就是与direction无关
//
// 这里取active connection，采用了随机策略，尽量均衡Connection的负载
func (p *Peer) getActiveConnLocked() (*Connection, bool) {
	allConns := len(p.inboundConnections) + len(p.outboundConnections)
	if allConns == 0 {
		return nil, false
	}

	startOffset := peerRng.Intn(allConns)
	for i := 0; i < allConns; i++ {
		connIndex := (i + startOffset) % allConns
		if conn := p.getConn(connIndex); conn.IsActive() {
			return conn, true
		}
	}

	return nil, false
}

// TODO(prashant): Should we clear inactive connections?
// TODO(prashant): Do we want some sort of scoring for connections?
// 互斥获取一个active connection
func (p *Peer) getActiveConn() (*Connection, bool) {
	p.RLock()
	conn, ok := p.getActiveConnLocked()
	p.RUnlock()

	return conn, ok
}

// Peer所在的channel，获取一个与peer 服务端口为host:port的active connection，
// 如果不存在，则通过Connect方法建立一个active connection
func (p *Peer) GetConnection(ctx context.Context) (*Connection, error) {
	if activeConn, ok := p.getActiveConn(); ok {
		return activeConn, nil
	}

	// 在获取不到active connection时，总会下一次在重试一次
	p.newConnLock.Lock()
	defer p.newConnLock.Unlock()

	if activeConn, ok := p.getActiveConn(); ok {
		return activeConn, nil
	}

	// channel发起一个调用host:port的连接，为outbound direction
	return p.Connect(ctx)
}

// 获取一个active connection，
// 如果不存在，则创建一个connection，且这个连接上下文传输的超时时间为timeout
func (p *Peer) getConnectionRelay(timeout time.Duration) (*Connection, error) {
	if conn, ok := p.getActiveConn(); ok {
		return conn, nil
	}

	// 在获取不到的active connection时，总会下一次重试
	p.newConnLock.Lock()
	defer p.newConnLock.Unlock()

	if activeConn, ok := p.getActiveConn(); ok {
		return activeConn, nil
	}

	// 构建context, 发起outbound的调用
	ctx, cancel := NewContextBuilder(timeout).HideListeningOnOutbound().Build()
	defer cancel()

	return p.Connect(ctx)
}

// Peer增加引用PeerList
func (p *Peer) addSC() {
	p.Lock()
	p.scCount++
	p.Unlock()
}

// Peer减少引用PeerList
func (p *Peer) delSC() {
	p.Lock()
	p.scCount--
	p.Unlock()
}

// 校验Peer是否可以移除
// 判断条件：inbound和outbound connections为空，且peer的引用计数为0
func (p *Peer) canRemove() bool {
	p.RLock()
	count := len(p.inboundConnections) + len(p.outboundConnections) + int(p.scCount)
	p.RUnlock()
	return count == 0
}

// 在新建active connection时, 为channel发起的outbound调用
func (p *Peer) addConnection(c *Connection, direction connectionDirection) error {
	// 返回Peer的direction connection
	conns := p.connectionsFor(direction)

	if c.readState() != connectionActive {
		return ErrInvalidConnectionState
	}

	p.Lock()
	*conns = append(*conns, c)
	p.Unlock()

	// 创建Peer的事件触发
	p.onStatusChanged(p)

	return nil
}

// 获取Peer指定方向的Connections指针
func (p *Peer) connectionsFor(direction connectionDirection) *[]*Connection {
	if direction == inbound {
		return &p.inboundConnections
	}
	return &p.outboundConnections
}

// 从Peer指定方向的connections中移除changed connection
//
// 这个方法设计的有些不合理，应该只需要传方向和connection, 不然也需要校验connsPtr与Peer中的connections是否相同
func (p *Peer) removeConnection(connsPtr *[]*Connection, changed *Connection) bool {
	conns := *connsPtr
	for i, c := range conns {
		if c == changed {
			// 移除某个元素，最常用做法，index与最后一个交换，再缩容一个
			last := len(conns) - 1
			conns[i], conns[last] = conns[last], nil
			*connsPtr = conns[:last]
			return true
		}
	}

	return false
}

// 关闭连接状态为inactive的connection
func (p *Peer) connectionCloseStateChange(changed *Connection) {
	// 如果connection state仍未active，则直接返回
	if changed.IsActive() {
		return
	}

	// 从Peer的inbound和outbound中移除changed连接
	p.Lock()
	found := p.removeConnection(&p.inboundConnections, changed)
	if !found {
		found = p.removeConnection(&p.outboundConnections, changed)
	}
	p.Unlock()

	if found {
		// 这里的设计也存在问题，输入的两个参数都是Peer
		p.onClosedConnRemoved(p)
		// Peer状态变化，触发事件
		p.onStatusChanged(p)
	}
}

// 存储Peer信息所在的服务channel发起outbound调用
func (p *Peer) Connect(ctx context.Context) (*Connection, error) {
	return p.channel.Connect(ctx, p.hostPort)
}

// 封装协议帧，填充arg1协议
func (p *Peer) BeginCall(ctx context.Context, serviceName, methodName string, callOptions *CallOptions) (*OutboundCall, error) {
	if callOptions == nil {
		callOptions = defaultCallOptions
	}
	callOptions.RequestState.AddSelectedPeer(p.HostPort())

	// 校验serviceName、methodName和context
	if err := validateCall(ctx, serviceName, methodName, callOptions); err != nil {
		return nil, err
	}

	// 获取一个存储Peer的Channel服务与Peer Host:Port的active connection
	// 如果不存在，则发起一个connection
	conn, err := p.GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	// connection封装协议帧，arg1，version，msg id等
	call, err := conn.beginCall(ctx, serviceName, methodName, callOptions)
	if err != nil {
		return nil, err
	}

	return call, err
}

// 返回存储Peer的Channel服务与Peer建立的inbound和outbound connections
func (p *Peer) NumConnections() (int, int) {
	p.RLock()
	inbound, outbound := len(p.inboundConnections), len(p.outboundConnections)
	p.RUnlock()
	return inbound, outbound
}

// NumPendingOutbound returns the number of pending outbound calls.
// ::TODO
func (p *Peer) NumPendingOutbound() int {
	count := 0
	p.RLock()
	for _, c := range p.outboundConnections {
		count += c.outbound.count()
	}

	for _, c := range p.inboundConnections {
		count += c.outbound.count()
	}
	p.RUnlock()
	return count
}

// ::TODO
func (p *Peer) runWithConnections(f func(*Connection)) {
	p.RLock()
	for _, c := range p.inboundConnections {
		f(c)
	}

	for _, c := range p.outboundConnections {
		f(c)
	}
	p.RUnlock()
}

// 当Peer更新时，执行事件
func (p *Peer) callOnUpdateComplete() {
	p.RLock()
	f := p.onUpdate
	p.RUnlock()

	if f != nil {
		f(p)
	}
}

func noopOnStatusChanged(*Peer) {}

// isEphemeralHostPort returns if hostPort is the default ephemeral hostPort.
func isEphemeralHostPort(hostPort string) bool {
	return hostPort == "" || hostPort == ephemeralHostPort || strings.HasSuffix(hostPort, ":0")
}
