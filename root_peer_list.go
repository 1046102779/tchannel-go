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

import "sync"

// 注意通过Peer，才能获得到具体的connection，并进行read/write,
// 注意：所有的peer在direction都存放着两个connection, 并在channel.mutable.conns存储着所有连接

// RootPeerList用于维护本channel与所有remote peer建立连接的相关peer信息
// 如果本channel与其他host:port建立起的connection，则会从找到remote peer拥有的connection，
// 并进行read/write
//
// 其中，Connectable用于发起一个新的连接
type RootPeerList struct {
	sync.RWMutex

	channel             Connectable
	onPeerStatusChanged func(*Peer)
	peersByHostPort     map[string]*Peer
}

// 服务注册的服务channel，创建RootPeerList实例
//
// ch Connectable为channel实例，该channel实现了Connectable interface
func newRootPeerList(ch Connectable, onPeerStatusChanged func(*Peer)) *RootPeerList {
	return &RootPeerList{
		channel:             ch,
		onPeerStatusChanged: onPeerStatusChanged,
		peersByHostPort:     make(map[string]*Peer),
	}
}

// newChild创建一个子PeerList，为channel的RootPeerList的child
// ::TODO
func (l *RootPeerList) newChild() *PeerList {
	return newPeerList(l)
}

// 服务注册的服务channel, 保存
//
// 注意两点：
//  1. 如果服务注册的服务channel发起调用，则remote service的host:port保存到RootPeerList的peersByHostPort
//  2. 如果是remote service发起与本channel的connection调用，则把remote service的host:port保存到...
//  3. 如果peersByHostPort中host:port已存在，则返回Peer，并指明这个Peer中的connection direction
func (l *RootPeerList) Add(hostPort string) *Peer {
	// 再次读取peersByHostPort，如果不存在，则写入
	l.Lock()
	defer l.Unlock()

	if p, ok := l.peersByHostPort[hostPort]; ok {
		return p
	}

	var p *Peer
	// To avoid duplicate connections, only the root list should create new
	// peers. All other lists should keep refs to the root list's peers.
	p = newPeer(l.channel, hostPort, l.onPeerStatusChanged, l.onClosedConnRemoved)
	l.peersByHostPort[hostPort] = p
	return p
}

// 校验host:port在channel中的peersByHostPort是否存在
func (l *RootPeerList) GetOrAdd(hostPort string) *Peer {
	if peer, ok := l.Get(hostPort); ok {
		return peer
	}

	return l.Add(hostPort)
}

// 从channel中通过peersByHostPort，获取key为host:port的Peer
func (l *RootPeerList) Get(hostPort string) (*Peer, bool) {
	l.RLock()
	p, ok := l.peersByHostPort[hostPort]
	l.RUnlock()
	return p, ok
}

// 当关闭connection时，会从channel的RootPeerList去掉peer
//
// 注意：host:port对应的Peer，有inbound和outbound的两个方向, 当peer-to-peer的connections全部为空时，则可清除
func (l *RootPeerList) onClosedConnRemoved(peer *Peer) {
	// 获取host:port对应的peer，并校验是否在peersByHostPort存在
	hostPort := peer.HostPort()
	p, ok := l.Get(hostPort)
	if !ok {
		return
	}

	// 评估peer是否可以在channel中删除
	if p.canRemove() {
		l.Lock()
		delete(l.peersByHostPort, hostPort)
		l.Unlock()
		l.channel.Logger().WithFields(
			LogField{"remoteHostPort", hostPort},
		).Debug("Removed peer from root peer list.")
	}
}

// 快照克隆一份peersByHostPort
func (l *RootPeerList) Copy() map[string]*Peer {
	l.RLock()
	defer l.RUnlock()

	listCopy := make(map[string]*Peer)
	for k, v := range l.peersByHostPort {
		listCopy[k] = v
	}
	return listCopy
}
