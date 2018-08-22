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
	"time"

	"github.com/1046102779/tchannel-go/typed"
)

// message表示协议帧完整的一个包，包括的消息类型为：
// init req, init res
// call req, call res
// call req continue, call res continue
// ping req, ping res

// message与frame的转化，在frame.go文件中
// 该文件的message只是对协议帧包进行相应的读写操作

// messageType defines a type of message
type messageType byte

const (
	messageTypeInitReq         messageType = 0x01
	messageTypeInitRes         messageType = 0x02
	messageTypeCallReq         messageType = 0x03
	messageTypeCallRes         messageType = 0x04
	messageTypeCallReqContinue messageType = 0x13
	messageTypeCallResContinue messageType = 0x14
	messageTypePingReq         messageType = 0xd0
	messageTypePingRes         messageType = 0xd1
	messageTypeError           messageType = 0xFF
)

//go:generate stringer -type=messageType

// message interface: 所有类型协议帧通用接口
type message interface {
	// ID方法返回协议帧的msg id
	ID() uint32

	// messageType方法返回协议帧的消息类型: init req, call req等
	messageType() messageType

	// read方法从网络数据中读取指定长度的数据字段
	read(r *typed.ReadBuffer) error

	// write方法写入指定长度的数据字段
	write(w *typed.WriteBuffer) error
}

// 默认提供一个没有发生数据读写的noBodyMsg
type noBodyMsg struct{}

func (noBodyMsg) read(r *typed.ReadBuffer) error   { return nil }
func (noBodyMsg) write(w *typed.WriteBuffer) error { return nil }

// initParams表示：协议帧类型为init req时
// version:2 nh:2(key~2 value~2){nh}
// 其中的nh键值对包括：host_port, process_name, tchannel_language, tchannel_language_version与tchannel_version
type initParams map[string]string

const (
	// InitParamHostPort contains the host and port of the peer process
	InitParamHostPort = "host_port"
	// InitParamProcessName contains the name of the peer process
	InitParamProcessName = "process_name"
	// InitParamTChannelLanguage contains the library language.
	InitParamTChannelLanguage = "tchannel_language"
	// InitParamTChannelLanguageVersion contains the language build/runtime version.
	InitParamTChannelLanguageVersion = "tchannel_language_version"
	// InitParamTChannelVersion contains the library version.
	InitParamTChannelVersion = "tchannel_version"
)

// initMessage包括两部分内容
/*
协议帧
position	Contents
0-7			size:2 type:1 reserved:1 id:4
8-15		reserved:8
16+			payload - based on type

其中payload数据部分：version:2 nh:2 (key~2 value~2){nh}


由上面协议帧，我们可以知道initMessage获取的数据分别是：
id: 4
version: payload的前两个字节
initParams: payload的后面nheaders数据, map[string]string]{
	"host_port": ...,
	"process_name": ...,
	"tchannel_language": ...,
	"tchannel_language_version": ...,
	"tchannel_version": ....,
}
*/
// 这个为init req/init res协议帧类型消息, 因为init req和init res消息格式相同
type initMessage struct {
	id         uint32
	Version    uint16
	initParams initParams
}

// 从网络获取的协议帧进行数据读取，并保存到initMessage中
//
// 这个就借助了typed中的ReadBuffer
func (m *initMessage) read(r *typed.ReadBuffer) error {
	//  从这个读取可以看到，ReadBuffer中已经把headers部分数据读取了，
	// 只剩下payload部分数据未读取, 先读取2字节的version
	m.Version = r.ReadUint16()

	m.initParams = initParams{}
	// 首先读取存放键值对的总数量, 2字节nh
	np := r.ReadUint16()
	// 获取到的键值对数量，在一个个读取key占2字节和value占2字节的数据
	// 并存储到initParams中
	for i := 0; i < int(np); i++ {
		k := r.ReadLen16String()
		v := r.ReadLen16String()
		m.initParams[k] = v
	}

	return r.Err()
}

// 把initMessage中的数据写入到WriteBuffer中的remaining部分
//
// 这个也是payload部分，因为headers是通用的，占用16字节
func (m *initMessage) write(w *typed.WriteBuffer) error {
	// 先写入2字节的version到WriteBuffer中的buffer部分
	w.WriteUint16(m.Version)
	// 在写入键值对的总数量到占用2字节的nh中
	w.WriteUint16(uint16(len(m.initParams)))

	// 再遍历键值对，并写入到key占2字节，value占2字节的空闲内存区
	for k, v := range m.initParams {
		w.WriteLen16String(k)
		w.WriteLen16String(v)
	}

	return w.Err()
}

// 返回init req/init res协议帧的msg id
func (m *initMessage) ID() uint32 {
	return m.id
}

// init res/init req的协议帧payload部分是相同格式，所以只需要继承initMessage，并添加一个返回messageType的方法，就实现了message interface
type initReq struct {
	initMessage
}

func (m *initReq) messageType() messageType { return messageTypeInitReq }

// An initRes contains context information returned to an initiating peer
type initRes struct {
	initMessage
}

func (m *initRes) messageType() messageType { return messageTypeInitRes }

// 接下来就是call req, 注意call res与前者有些区别, 所以没有提供类似于initMessage的底层实现
//
// 但是这二者payload部分的Headers是相同的，都是使用Transport Headers

// Transport Headers包括：
/*
name	req	res	description          中文解释
as	   Y	Y	the Arg Scheme		  上层协议格式：json/thrift/sthrift/http/raw
cas	   Y	N	Claim At Start		  rpc发起调用，对方Peer的host:port
caf	   Y	N	Claim At Finish		  rpc发起调用，对方Peer响应后返回时的调用方host:port
cn	   Y	N	Caller Name			  rpc调用对方Peer的服务名称
re	   Y	N	Retry Flags			  重试机制，具体详见协议。n，c，t
se	   Y	N	Speculative Execution 指定的执行次数，指定多少个节点运行这个rpc调用请求
fd	   Y	Y	Failure Domain		  熔断机制
sk	   Y	N	Shard key			  通过这个hash key，可以指定这个rpc请求由哪些或者哪个节点执行
rd	   Y	N	Routing Delegate	  路由委托给指定的服务名处理这个rpc请求
*/
type TransportHeaderName string

func (cn TransportHeaderName) String() string { return string(cn) }

// 对于transport header的所有key在上面已经介绍
const (
	ArgScheme            TransportHeaderName = "as"
	ClaimAtStart         TransportHeaderName = "cas"
	ClaimAtFinish        TransportHeaderName = "caf"
	CallerName           TransportHeaderName = "cn"
	RetryFlags           TransportHeaderName = "re"
	SpeculativeExecution TransportHeaderName = "se"
	FailureDomain        TransportHeaderName = "fd"
	ShardKey             TransportHeaderName = "sk"
	RoutingDelegate      TransportHeaderName = "rd"
	// ::TODO RoutingKey在tchannel协议规范中没有看到
	RoutingKey TransportHeaderName = "rk"
)

// transportHeaders类型变量值用于协议帧中的payload部分的transport headers
type transportHeaders map[TransportHeaderName]string

// 对于call req和call res两种协议帧，transport headers格式是相同的
//  nh:1 (hk~1 hv~1){nh}
// ReadBuffer存储的协议帧，remaining内存数据部分只剩下payload的transport headers
func (ch transportHeaders) read(r *typed.ReadBuffer) {
	// 先读取占用1字节的键值对总数量
	nh := r.ReadSingleByte()
	// 然后再for读取数量为nh的键值对, 并存储到Transport Headers中
	for i := 0; i < int(nh); i++ {
		k := r.ReadLen8String()
		v := r.ReadLen8String()
		ch[TransportHeaderName(k)] = v
	}
}

// 写入transportHeaders数据到WriteBuffer的remaining区域，remaining之前的内存区域已经填满了协议帧的headers和payload transport headers之前部分
func (ch transportHeaders) write(w *typed.WriteBuffer) {
	// 写入键值对总数量到nh中
	w.WriteSingleByte(byte(len(ch)))

	// 遍历transportHeaders，并以key占用1字节，value占用1字节的数据存储到协议帧中
	for k, v := range ch {
		w.WriteLen8String(k.String())
		w.WriteLen8String(v)
	}
}

// 下面是call req协议帧类型的payload部分, 对网络数据的读写操作
// 直接实现message interface
/*
flags:1 ttl:4 tracing:25
service~1 nh:1 (hk~1 hv~1){nh}
csumtype:1 (csum:4){0,1} arg1~2 arg2~2 arg3~2
*/
type callReq struct {
	id         uint32
	TimeToLive time.Duration
	Tracing    Span
	Headers    transportHeaders
	Service    string
}

func (m *callReq) ID() uint32               { return m.id }
func (m *callReq) messageType() messageType { return messageTypeCallReq }

// 从这里我们可以看到1字节的flags在外层已经读取了
// 读取4字节的ttl，25字节的tracing，1字节的服务名，以及通过transportHeaders读取transport headers部分
// 注意参数没有读取
func (m *callReq) read(r *typed.ReadBuffer) error {
	m.TimeToLive = time.Duration(r.ReadUint32()) * time.Millisecond
	m.Tracing.read(r)
	m.Service = r.ReadLen8String()
	m.Headers = transportHeaders{}
	m.Headers.read(r)
	return r.Err()
}

// 从这里也可以看到写入callReq数据到WriteBuffer中remaining部分, payload的前半部分
func (m *callReq) write(w *typed.WriteBuffer) error {
	w.WriteUint32(uint32(m.TimeToLive / time.Millisecond))
	m.Tracing.write(w)
	w.WriteLen8String(m.Service)
	m.Headers.write(w)
	return w.Err()
}

// 对于call req continue协议帧类型的payload部分
// flags:1 csumtype:1 (csum:4){0,1} {continuation}
//  它无需读写payload前半部分，所以可以继承noBodyMsg，无需读写
type callReqContinue struct {
	noBodyMsg
	id uint32
}

func (c *callReqContinue) ID() uint32               { return c.id }
func (c *callReqContinue) messageType() messageType { return messageTypeCallReqContinue }

// 对于call res协议帧类型的payload部分
/*
flags:1 code:1 tracing:25
nh:1 (hk~1 hv~1){nh}
csumtype:1 (csum:4){0,1} arg1~2 arg2~2 arg3~2
*/

// code字段值：
/*
code	name	description
0x00	OK		everything is great and we value your contribution.
0x01	Error	application error, details are in the args.

0x00: 表示没有错误；0x01: 表示上层的业务逻辑处理失败
*/
type ResponseCode byte

const (
	responseOK               ResponseCode = 0x00
	responseApplicationError ResponseCode = 0x01
)

// call res与call req不同的是，多了一个1字节的code，少了4字节的ttl和1字节的service name
type callRes struct {
	id           uint32
	ResponseCode ResponseCode
	Tracing      Span
	Headers      transportHeaders
}

func (m *callRes) ID() uint32               { return m.id }
func (m *callRes) messageType() messageType { return messageTypeCallRes }

// callRes读取ReadBuffer中剩余的remaining部分数据，remaining之前已经读取了headers和flags
func (m *callRes) read(r *typed.ReadBuffer) error {
	// 读取1字节的code
	m.ResponseCode = ResponseCode(r.ReadSingleByte())
	// 读取25字节的tracing
	m.Tracing.read(r)
	// 读取transportHeaders
	m.Headers = transportHeaders{}
	m.Headers.read(r)
	return r.Err()
}

// callRes写入payload前半部分数据到WriteBuffer的remaining部分
func (m *callRes) write(w *typed.WriteBuffer) error {
	// 写入1字节的code
	w.WriteSingleByte(byte(m.ResponseCode))
	// 写入25字节的tracing
	m.Tracing.write(w)
	// 写入transport headers
	m.Headers.write(w)
	return w.Err()
}

// callResContinue is a continuation of a previous CallRes
type callResContinue struct {
	noBodyMsg
	id uint32
}

func (c *callResContinue) ID() uint32               { return c.id }
func (c *callResContinue) messageType() messageType { return messageTypeCallResContinue }

// An errorMessage is a system-level error response to a request or a protocol level error
type errorMessage struct {
	id      uint32
	errCode SystemErrCode
	tracing Span
	message string
}

func (m *errorMessage) ID() uint32               { return m.id }
func (m *errorMessage) messageType() messageType { return messageTypeError }
func (m *errorMessage) read(r *typed.ReadBuffer) error {
	m.errCode = SystemErrCode(r.ReadSingleByte())
	m.tracing.read(r)
	m.message = r.ReadLen16String()
	return r.Err()
}

func (m *errorMessage) write(w *typed.WriteBuffer) error {
	w.WriteSingleByte(byte(m.errCode))
	m.tracing.write(w)
	w.WriteLen16String(m.message)
	return w.Err()
}

func (m errorMessage) AsSystemError() error {
	// TODO(mmihic): Might be nice to return one of the well defined error types
	return NewSystemError(m.errCode, m.message)
}

// Error returns the error message from the converted
func (m errorMessage) Error() string {
	return m.AsSystemError().Error()
}

// 对于ping req与ping res协议，只是健康心跳检查, 没有实际数据
type pingReq struct {
	noBodyMsg
	id uint32
}

func (c *pingReq) ID() uint32               { return c.id }
func (c *pingReq) messageType() messageType { return messageTypePingReq }

// pingRes is a ping response to a protocol level ping request.
type pingRes struct {
	noBodyMsg
	id uint32
}

func (c *pingRes) ID() uint32               { return c.id }
func (c *pingRes) messageType() messageType { return messageTypePingRes }

func callReqSpan(f *Frame) Span {
	rdr := typed.NewReadBuffer(f.Payload[_spanIndex : _spanIndex+_spanLength])
	var s Span
	s.read(rdr)
	return s
}
