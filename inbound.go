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
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"golang.org/x/net/context"
)

var errInboundRequestAlreadyActive = errors.New("inbound request is already active; possible duplicate client id")

// handleCallReq处理接收到的call req frame
func (c *Connection) handleCallReq(frame *Frame) bool {
	now := c.timeNow()
	// 校验当前connection的状态, 必须为active connection
	switch state := c.readState(); state {
	case connectionActive:
		break
	case connectionStartClose, connectionInboundClosed, connectionClosed:
		c.SendSystemError(frame.Header.ID, callReqSpan(frame), ErrChannelClosed)
		return true
	default:
		panic(fmt.Errorf("unknown connection state for call req: %v", state))
	}

	callReq := new(callReq)
	// 获取call req的msg id, 并解析frame到initialFragment
	callReq.id = frame.Header.ID
	initialFragment, err := parseInboundFragment(c.opts.FramePool, frame, callReq)
	if err != nil {
		// TODO(mmihic): Probably want to treat this as a protocol error
		c.log.WithFields(
			LogField{"header", frame.Header},
			ErrField(err),
		).Error("Couldn't decode initial fragment.")
		return true
	}

	// 这里的InboundCall：
	// 1. 封装解析frame和response引用；
	// 2. 通过frame的methodname找到handle，并通过反射，进行具体协议json/thrift等进行业务逻辑处理；
	// 3. 把业务逻辑数据的返回写入到response中, 这样也就写入到了connection中
	// 4. 发起的call req frame的Peer，正阻塞等待response返回
	call := new(InboundCall)
	call.conn = c
	ctx, cancel := newIncomingContext(call, callReq.TimeToLive)

	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return true
	}
	defer c.pendingExchangeMethodDone()

	// 新建一个message exchange, 并通过msg id与message exchange建立map存储
	mex, err := c.inbound.newExchange(ctx, c.opts.FramePool, callReq.messageType(), frame.Header.ID, mexChannelBufferSize)
	if err != nil {
		if err == errDuplicateMex {
			err = errInboundRequestAlreadyActive
		}
		c.log.WithFields(LogField{"header", frame.Header}).Error("Couldn't register exchange.")
		c.protocolError(frame.Header.ID, errInboundRequestAlreadyActive)
		return true
	}

	// Close may have been called between the time we checked the state and us creating the exchange.
	if c.readState() != connectionActive {
		mex.shutdown()
		return true
	}

	response := new(InboundCallResponse)
	response.call = call
	response.calledAt = now
	response.timeNow = c.timeNow
	response.span = c.extractInboundSpan(callReq)
	if response.span != nil {
		mex.ctx = opentracing.ContextWithSpan(mex.ctx, response.span)
	}
	response.mex = mex
	response.conn = c
	response.cancel = cancel
	response.log = c.log.WithFields(LogField{"In-Response", callReq.ID()})
	response.contents = newFragmentingWriter(response.log, response, initialFragment.checksumType.New())
	response.headers = transportHeaders{}
	response.messageForFragment = func(initial bool) message {
		if initial {
			callRes := new(callRes)
			callRes.Headers = response.headers
			callRes.ResponseCode = responseOK
			if response.applicationError {
				callRes.ResponseCode = responseApplicationError
			}
			return callRes
		}

		return new(callResContinue)
	}

	call.mex = mex
	call.initialFragment = initialFragment
	call.serviceName = string(callReq.Service)
	call.headers = callReq.Headers
	call.response = response
	call.log = c.log.WithFields(LogField{"In-Call", callReq.ID()})
	call.messageForFragment = func(initial bool) message { return new(callReqContinue) }
	call.contents = newFragmentingReader(call.log, call)
	call.statsReporter = c.statsReporter
	call.createStatsTags(c.commonStatsTags)

	response.statsReporter = c.statsReporter
	response.commonStatsTags = call.commonStatsTags

	setResponseHeaders(call.headers, response.headers)
	// 这里需要启用一个goroutine吗？是的，需要
	// 因为connection可以复用，并不是一个业务处理完成，才能复用这个connection，而是随时都可以
	//
	// 这里是分发业务处理
	go c.dispatchInbound(c.connID, callReq.ID(), call, frame)
	return false
}

// 因为call req已经发起请求了，后续的所有相同的msg id，都是后续处理，Peer-To-Peer都是两边阻塞着message exchange等待frame到来的处理
func (c *Connection) handleCallReqContinue(frame *Frame) bool {
	if err := c.inbound.forwardPeerFrame(frame); err != nil {
		// If forward fails, it's due to a timeout. We can free this frame.
		return true
	}
	return false
}

// createStatsTags创建统计tags
func (call *InboundCall) createStatsTags(connectionTags map[string]string) {
	call.commonStatsTags = map[string]string{
		"calling-service": call.CallerName(),
	}
	for k, v := range connectionTags {
		call.commonStatsTags[k] = v
	}
}

// 这个用在call req frame的请求分发处理, 并通过具体协议指定的Handle方法，通过反射映射到注册的业务处理函数, 并把结果返回到connection中
func (c *Connection) dispatchInbound(_ uint32, _ uint32, call *InboundCall, frame *Frame) {
	if call.log.Enabled(LogLevelDebug) {
		call.log.Debugf("Received incoming call for %s from %s", call.ServiceName(), c.remotePeerInfo)
	}

	if err := call.readMethod(); err != nil {
		call.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			ErrField(err),
		).Error("Couldn't read method.")
		c.opts.FramePool.Release(frame)
		return
	}

	call.commonStatsTags["endpoint"] = call.methodString
	call.statsReporter.IncCounter("inbound.calls.recvd", call.commonStatsTags, 1)
	if span := call.response.span; span != nil {
		span.SetOperationName(call.methodString)
	}

	// TODO(prashant): This is an expensive way to check for cancellation. Use a heap for timeouts.
	go func() {
		select {
		case <-call.mex.ctx.Done():
			// checking if message exchange timedout or was cancelled
			// only two possible errors at this step:
			// context.DeadlineExceeded
			// context.Canceled
			if call.mex.ctx.Err() != nil {
				call.mex.inboundExpired()
			}
		case <-call.mex.errCh.c:
			if c.log.Enabled(LogLevelDebug) {
				call.log.Debugf("Wait for timeout/cancellation interrupted by error: %v", call.mex.errCh.err)
			}
			// when an exchange errors out, mark the exchange as expired
			// and call cancel so the server handler's context is canceled
			// TODO: move the cancel to the parent context at connnection level
			call.response.cancel()
			call.mex.inboundExpired()
		}
	}()

	// 这里就是设计到具体的协议json/thrift，例如: HandleFunc(ctx Context, inbound *InboundCall)
	// 业务逻辑方法处理
	c.handler.Handle(call.mex.ctx, call)
}

// 接收一个frame，并通过InboundCall进行存储frame和response引用
// 它作为具体协议业务处理函数的第二个参数
type InboundCall struct {
	reqResReader

	// active connection
	conn *Connection
	// response引用，通过这个可以在上层协议json/thrift中，把业务返回值写入到response的connection中
	response        *InboundCallResponse
	serviceName     string
	method          []byte
	methodString    string
	headers         transportHeaders
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// ServiceName方法返回服务名称
func (call *InboundCall) ServiceName() string {
	return call.serviceName
}

// 返回方法名称
func (call *InboundCall) Method() []byte {
	return call.method
}

// 返回方法名称
func (call *InboundCall) MethodString() string {
	return call.methodString
}

// 在transport headers中，返回另一方Peer使用的具体协议: json, thrift, ...
func (call *InboundCall) Format() Format {
	return Format(call.headers[ArgScheme])
}

// 在transport headers中，返回CallerName名称,也是服务名
func (call *InboundCall) CallerName() string {
	return call.headers[CallerName]
}

// 在transport headers中，返回ShardKey
func (call *InboundCall) ShardKey() string {
	return call.headers[ShardKey]
}

// 在transport headers中，返回RoutingKey
func (call *InboundCall) RoutingKey() string {
	return call.headers[RoutingKey]
}

// 在transport headers中，返回RoutingDelegate
func (call *InboundCall) RoutingDelegate() string {
	return call.headers[RoutingDelegate]
}

// 返回InboundCall所在服务的PeerInfo。
func (call *InboundCall) LocalPeer() LocalPeerInfo {
	return call.conn.localPeerInfo
}

// 返回InboundCall所在connection的另一边Peer信息
func (call *InboundCall) RemotePeer() PeerInfo {
	return call.conn.RemotePeerInfo()
}

// 返回CallOptions, 主要是transport headers中的相关参数, 具体见tchannel协议规范
func (call *InboundCall) CallOptions() *CallOptions {
	return &CallOptions{
		callerName:      call.CallerName(),
		Format:          call.Format(),
		ShardKey:        call.ShardKey(),
		RoutingDelegate: call.RoutingDelegate(),
		RoutingKey:      call.RoutingKey(),
	}
}

// 获取调用方法名称
func (call *InboundCall) readMethod() error {
	var arg1 []byte
	if err := NewArgReader(call.arg1Reader()).Read(&arg1); err != nil {
		return call.failed(err)
	}

	call.method = arg1
	call.methodString = string(arg1)
	return nil
}

// Arg2Reader方法读取arg2参数, 到ArgReader实例中
func (call *InboundCall) Arg2Reader() (ArgReader, error) {
	return call.arg2Reader()
}

// Arg3Reader读取arg3参数，到ArgReader实例中
func (call *InboundCall) Arg3Reader() (ArgReader, error) {
	return call.arg3Reader()
}

// 返回InboundCall中的Response引用
func (call *InboundCall) Response() *InboundCallResponse {
	if call.err != nil {
		// While reading Thrift, we cannot distinguish between malformed Thrift and other errors,
		// and so we may try to respond with a bad request. We should ensure that the response
		// is marked as failed if the request has failed so that we don't try to shutdown the exchange
		// a second time.
		call.response.err = call.err
	}
	return call.response
}

func (call *InboundCall) doneReading(unexpected error) {}

// 当业务逻辑处理完后，InboundCallResponse会把业务数据结果写入到connection中
type InboundCallResponse struct {
	reqResWriter

	call   *InboundCall
	cancel context.CancelFunc
	// calledAt is the time the inbound call was routed to the application.
	calledAt         time.Time
	timeNow          func() time.Time
	applicationError bool
	systemError      bool
	headers          transportHeaders
	span             opentracing.Span
	statsReporter    StatsReporter
	commonStatsTags  map[string]string
}

// SendSystemError returns a system error response to the peer.  The call is considered
// complete after this method is called, and no further data can be written.
func (response *InboundCallResponse) SendSystemError(err error) error {
	if response.err != nil {
		return response.err
	}
	// Fail all future attempts to read fragments
	response.state = reqResWriterComplete
	response.systemError = true
	response.doneSending()
	response.call.releasePreviousFragment()

	span := CurrentSpan(response.mex.ctx)

	return response.conn.SendSystemError(response.mex.msgID, *span, err)
}

// SetApplicationError marks the response as being an application error.  This method can
// only be called before any arguments have been sent to the calling peer.
func (response *InboundCallResponse) SetApplicationError() error {
	if response.state > reqResWriterPreArg2 {
		return response.failed(errReqResWriterStateMismatch{
			state:         response.state,
			expectedState: reqResWriterPreArg2,
		})
	}
	response.applicationError = true
	return nil
}

// Blackhole indicates no response will be sent, and cleans up any resources
// associated with this request. This allows for services to trigger a timeout in
// clients without holding on to any goroutines on the server.
func (response *InboundCallResponse) Blackhole() {
	response.cancel()
}

// 把arg2参数写入到Response的协议帧中
func (response *InboundCallResponse) Arg2Writer() (ArgWriter, error) {
	if err := NewArgWriter(response.arg1Writer()).Write(nil); err != nil {
		return nil, err
	}
	return response.arg2Writer()
}

// 把arg3参数写入到Response的协议帧中
func (response *InboundCallResponse) Arg3Writer() (ArgWriter, error) {
	return response.arg3Writer()
}

// doneSending方法表示当一个msg id的调用全部完成后，也就是一个完整的调用流程。
// 则关闭message exchange
func (response *InboundCallResponse) doneSending() {
	// TODO(prashant): Move this to when the message is actually being sent.
	now := response.timeNow()

	if span := response.span; span != nil {
		if response.applicationError || response.systemError {
			ext.Error.Set(span, true)
		}
		span.FinishWithOptions(opentracing.FinishOptions{FinishTime: now})
	}

	latency := now.Sub(response.calledAt)
	response.statsReporter.RecordTimer("inbound.calls.latency", response.commonStatsTags, latency)

	if response.systemError {
		// TODO(prashant): Report the error code type as per metrics doc and enable.
		// response.statsReporter.IncCounter("inbound.calls.system-errors", response.commonStatsTags, 1)
	} else if response.applicationError {
		response.statsReporter.IncCounter("inbound.calls.app-errors", response.commonStatsTags, 1)
	} else {
		response.statsReporter.IncCounter("inbound.calls.success", response.commonStatsTags, 1)
	}

	// Cancel the context since the response is complete.
	response.cancel()

	// 关闭message exchange
	if response.err == nil {
		response.mex.shutdown()
	}
}
