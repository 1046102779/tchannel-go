// Copyright (c) 2017 Uber Technologies, Inc.

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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"time"

	"golang.org/x/net/context"
)

func (ch *Channel) outboundHandshake(ctx context.Context, c net.Conn, outboundHP string, events connectionEvents) (_ *Connection, err error) {
	defer setInitDeadline(ctx, c)()
	defer func() {
		err = ch.initError(c, outbound, 1, err)
	}()

	msg := &initReq{initMessage: ch.getInitMessage(ctx, 1)}
	if err := ch.writeMessage(c, msg); err != nil {
		return nil, err
	}

	res := &initRes{}
	id, err := ch.readMessage(c, res)
	if err != nil {
		return nil, err
	}

	if id != msg.id {
		return nil, NewSystemError(ErrCodeProtocol, "received initRes with invalid ID, wanted %v, got %v", msg.id, id)
	}

	if res.Version != CurrentProtocolVersion {
		return nil, unsupportedProtocolVersion(res.Version)
	}

	remotePeer, remotePeerAddress, err := parseRemotePeer(res.initParams, c.RemoteAddr())
	if err != nil {
		return nil, NewWrappedSystemError(ErrCodeProtocol, err)
	}

	return ch.newConnection(c, 1 /* initialID */, outboundHP, remotePeer, remotePeerAddress, events), nil
}

// 当rpc service端获取到一个rpc client的请求Accept后，就开始解析协议帧，并分发处理
func (ch *Channel) inboundHandshake(ctx context.Context, c net.Conn, events connectionEvents) (_ *Connection, err error) {
	id := uint32(math.MaxUint32)

	defer setInitDeadline(ctx, c)()
	defer func() {
		err = ch.initError(c, inbound, id, err)
	}()

	// 从net.Conn获取msg_id, version, 以及nheaders: host_port, tchannel_language&version, tchannel_version
	req := &initReq{}
	id, err = ch.readMessage(c, req)
	if err != nil {
		return nil, err
	}

	if req.Version < CurrentProtocolVersion {
		return nil, unsupportedProtocolVersion(req.Version)
	}

	// 这里一般只需要通过initParams，就可以获取到host_port和process_name
	// language version and tchannel_version
	remotePeer, remotePeerAddress, err := parseRemotePeer(req.initParams, c.RemoteAddr())
	if err != nil {
		return nil, NewWrappedSystemError(ErrCodeProtocol, err)
	}

	// 封装服务端的response信息，信息通过msg_id从channel获取
	res := &initRes{initMessage: ch.getInitMessage(ctx, id)}
	// 写入到协议frame中
	if err := ch.writeMessage(c, res); err != nil {
		return nil, err
	}

	// 创建一个可以在channel中管理的新连接, 并起两个goroutine，用于发送rpc client的请求和响应
	return ch.newConnection(c, 0 /* initialID */, "" /* outboundHP */, remotePeer, remotePeerAddress, events), nil
}

// 获取服务端的相关配置信息, 用于响应rpc client请求，写入到协议帧
func (ch *Channel) getInitParams() initParams {
	localPeer := ch.PeerInfo()
	return initParams{
		InitParamHostPort:                localPeer.HostPort,
		InitParamProcessName:             localPeer.ProcessName,
		InitParamTChannelLanguage:        localPeer.Version.Language,
		InitParamTChannelLanguageVersion: localPeer.Version.LanguageVersion,
		InitParamTChannelVersion:         localPeer.Version.TChannelVersion,
	}
}

// 获取initMessage的id，version和服务端相关配置信息
func (ch *Channel) getInitMessage(ctx context.Context, id uint32) initMessage {
	msg := initMessage{
		id:         id,
		Version:    CurrentProtocolVersion,
		initParams: ch.getInitParams(),
	}
	if p := getTChannelParams(ctx); p != nil && p.hideListeningOnOutbound {
		msg.initParams[InitParamHostPort] = ephemeralHostPort
	}

	return msg
}

// 写入响应rpc client请求的系统错误信息
func (ch *Channel) initError(c net.Conn, connDir connectionDirection, id uint32, err error) error {
	if err == nil {
		return nil
	}

	ch.log.WithFields(LogFields{
		{"connectionDirection", connDir},
		{"localAddr", c.LocalAddr().String()},
		{"remoteAddr", c.RemoteAddr().String()},
		ErrField(err),
	}...).Error("Failed during connection handshake.")

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		err = ErrTimeout
	}
	if err == io.EOF {
		err = NewWrappedSystemError(ErrCodeNetwork, io.EOF)
	}
	ch.writeMessage(c, &errorMessage{
		id:      id,
		errCode: GetSystemErrorCode(err),
		message: err.Error(),
	})
	c.Close()
	return err
}

func (ch *Channel) writeMessage(c net.Conn, msg message) error {
	frame := ch.connectionOptions.FramePool.Get()
	defer ch.connectionOptions.FramePool.Release(frame)

	if err := frame.write(msg); err != nil {
		return err
	}
	return frame.WriteOut(c)
}

func (ch *Channel) readMessage(c net.Conn, msg message) (uint32, error) {
	frame := ch.connectionOptions.FramePool.Get()
	defer ch.connectionOptions.FramePool.Release(frame)

	if err := frame.ReadIn(c); err != nil {
		return 0, err
	}

	if frame.Header.messageType != msg.messageType() {
		if frame.Header.messageType == messageTypeError {
			return frame.Header.ID, readError(frame)
		}
		return frame.Header.ID, NewSystemError(ErrCodeProtocol, "expected message type %v, got %v", msg.messageType(), frame.Header.messageType)
	}

	return frame.Header.ID, frame.read(msg)
}

func parseRemotePeer(p initParams, remoteAddr net.Addr) (PeerInfo, peerAddressComponents, error) {
	var (
		remotePeer        PeerInfo
		remotePeerAddress peerAddressComponents
		ok                bool
	)

	if remotePeer.HostPort, ok = p[InitParamHostPort]; !ok {
		return remotePeer, remotePeerAddress, fmt.Errorf("header %v is required", InitParamHostPort)
	}
	if remotePeer.ProcessName, ok = p[InitParamProcessName]; !ok {
		return remotePeer, remotePeerAddress, fmt.Errorf("header %v is required", InitParamProcessName)
	}

	// If the remote host:port is ephemeral, use the socket address as the
	// host:port and set IsEphemeral to true.
	if isEphemeralHostPort(remotePeer.HostPort) {
		remotePeer.HostPort = remoteAddr.String()
		remotePeer.IsEphemeral = true
	}

	remotePeer.Version.Language = p[InitParamTChannelLanguage]
	remotePeer.Version.LanguageVersion = p[InitParamTChannelLanguageVersion]
	remotePeer.Version.TChannelVersion = p[InitParamTChannelVersion]

	address := remotePeer.HostPort
	if sHost, sPort, err := net.SplitHostPort(address); err == nil {
		address = sHost
		if p, err := strconv.ParseUint(sPort, 10, 16); err == nil {
			remotePeerAddress.port = uint16(p)
		}
	}
	if address == "localhost" {
		remotePeerAddress.ipv4 = 127<<24 | 1
	} else if ip := net.ParseIP(address); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			remotePeerAddress.ipv4 = binary.BigEndian.Uint32(ip4)
		} else {
			remotePeerAddress.ipv6 = address
		}
	} else {
		remotePeerAddress.hostname = address
	}

	return remotePeer, remotePeerAddress, nil
}

func setInitDeadline(ctx context.Context, c net.Conn) func() {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}

	c.SetDeadline(deadline)
	return func() {
		c.SetDeadline(time.Time{})
	}
}

func readError(frame *Frame) error {
	errMsg := &errorMessage{
		id: frame.Header.ID,
	}
	if err := frame.read(errMsg); err != nil {
		return err
	}

	return errMsg.AsSystemError()
}

func unsupportedProtocolVersion(got uint16) error {
	return NewSystemError(ErrCodeProtocol, "unsupported protocol version %d from peer, expected %v", got, CurrentProtocolVersion)
}
