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
	"time"

	"github.com/uber/tchannel-go/typed"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"golang.org/x/net/context"
)

// maxMethodSize is the maximum size of arg1.
const maxMethodSize = 16 * 1024

// 存储connection所在的Peer发起一个outbound调用
// beginCall方法， 封装协议帧的arg1参数
func (c *Connection) beginCall(ctx context.Context, serviceName, methodName string, callOptions *CallOptions) (*OutboundCall, error) {
	now := c.timeNow()

	// 校验connection state是否为active
	switch state := c.readState(); state {
	case connectionActive:
		break
	case connectionStartClose, connectionInboundClosed, connectionClosed:
		return nil, ErrConnectionClosed
	default:
		return nil, errConnectionUnknownState{"beginCall", state}
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		// This case is handled by validateCall, so we should
		// never get here.
		return nil, ErrTimeoutRequired
	}

	// 校验一次完整的rpc调用的context是否已经超时
	timeToLive := deadline.Sub(now)
	if timeToLive < time.Millisecond {
		return nil, ErrTimeout
	}

	if err := ctx.Err(); err != nil {
		return nil, GetContextError(err)
	}

	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return nil, ErrInvalidConnectionState
	}
	defer c.pendingExchangeMethodDone()

	// 发起一个rpc调用，则需要新建msg id
	requestID := c.NextMessageID()
	// 新建一个message exchange, 并通过msg id与message exchange建立map
	mex, err := c.outbound.newExchange(ctx, c.opts.FramePool, messageTypeCallReq, requestID, mexChannelBufferSize)
	if err != nil {
		return nil, err
	}

	// Close may have been called between the time we checked the state and us creating the exchange.
	if state := c.readState(); state != connectionActive {
		mex.shutdown()
		return nil, ErrConnectionClosed
	}

	// Transport Header为协议帧的payload部分数据
	headers := transportHeaders{
		CallerName: c.localPeerInfo.ServiceName,
	}
	callOptions.setHeaders(headers)
	if opts := currentCallOptions(ctx); opts != nil {
		opts.overrideHeaders(headers)
	}

	// 新建一个发起OutboundCall的对外调用所需要的存储，包括call的调用协议帧和response引用
	// response引用主要用于message exchange的阻塞等待另一边response的返回
	call := new(OutboundCall)
	call.mex = mex
	call.conn = c
	call.callReq = callReq{
		id:         requestID,
		Headers:    headers,
		Service:    serviceName,
		TimeToLive: timeToLive,
	}
	call.statsReporter = c.statsReporter
	call.createStatsTags(c.commonStatsTags, callOptions, methodName)
	call.log = c.log.WithFields(LogField{"Out-Call", requestID})

	// TODO(mmihic): It'd be nice to do this without an fptr
	call.messageForFragment = func(initial bool) message {
		if initial {
			return &call.callReq
		}

		return new(callReqContinue)
	}

	call.contents = newFragmentingWriter(call.log, call, c.opts.ChecksumType.New())

	response := new(OutboundCallResponse)
	response.startedAt = now
	response.timeNow = c.timeNow
	response.requestState = callOptions.RequestState
	response.mex = mex
	response.log = c.log.WithFields(LogField{"Out-Response", requestID})
	response.span = c.startOutboundSpan(ctx, serviceName, methodName, call, now)
	response.messageForFragment = func(initial bool) message {
		if initial {
			return &response.callRes
		}

		return new(callResContinue)
	}
	response.contents = newFragmentingReader(response.log, response)
	response.statsReporter = call.statsReporter
	response.commonStatsTags = call.commonStatsTags

	call.response = response

	if err := call.writeMethod([]byte(methodName)); err != nil {
		return nil, err
	}
	return call, nil
}

// handleCallRes方法，因为call req已经发起了，所以直接找到message exchange的channel队列发送，因为connection正在阻塞等待response
func (c *Connection) handleCallRes(frame *Frame) bool {
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		return true
	}
	return false
}

// handleCallResContinue同上
func (c *Connection) handleCallResContinue(frame *Frame) bool {
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		return true
	}
	return false
}

// OutboundCall存储outbound调用的相关信息, 并存储将要返回的response信息
type OutboundCall struct {
	reqResWriter

	callReq         callReq
	response        *OutboundCallResponse
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// Response方法返回发起outbound调用的对方Peer Response
func (call *OutboundCall) Response() *OutboundCallResponse {
	return call.response
}

// createStatsTags创建tags
func (call *OutboundCall) createStatsTags(connectionTags map[string]string, callOptions *CallOptions, method string) {
	call.commonStatsTags = map[string]string{
		"target-service": call.callReq.Service,
	}
	for k, v := range connectionTags {
		call.commonStatsTags[k] = v
	}
	if callOptions.Format != HTTP {
		call.commonStatsTags["target-endpoint"] = string(method)
	}
}

// writeMethod方法写入method到arg1参数中
func (call *OutboundCall) writeMethod(method []byte) error {
	call.statsReporter.IncCounter("outbound.calls.send", call.commonStatsTags, 1)
	return NewArgWriter(call.arg1Writer()).Write(method)
}

// Arg2Writer方法写入arg2到协议帧中
func (call *OutboundCall) Arg2Writer() (ArgWriter, error) {
	return call.arg2Writer()
}

// Arg3Writer方法写入arg3到协议帧中
func (call *OutboundCall) Arg3Writer() (ArgWriter, error) {
	return call.arg3Writer()
}

// LocalPeer方法返回存储OutboundCall所在的Peer信息
func (call *OutboundCall) LocalPeer() LocalPeerInfo {
	return call.conn.localPeerInfo
}

// RemotePeer方法返回存储OutboundCall所在的另一边Peer信息
func (call *OutboundCall) RemotePeer() PeerInfo {
	return call.conn.RemotePeerInfo()
}

func (call *OutboundCall) doneSending() {}

// OutboundCallResponse方法存储outbound调用，并阻塞等待对方Peer的response
type OutboundCallResponse struct {
	reqResReader

	callRes callRes

	requestState *RequestState
	// startedAt is the time at which the outbound call was started.
	startedAt       time.Time
	timeNow         func() time.Time
	span            opentracing.Span
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// ApplicationError returns true if the call resulted in an application level error
// TODO(mmihic): In current implementation, you must have called Arg2Reader before this
// method returns the proper value.  We should instead have this block until the first
// fragment is available, if the first fragment hasn't been received.
func (response *OutboundCallResponse) ApplicationError() bool {
	// TODO(mmihic): Wait for first fragment
	return response.callRes.ResponseCode == responseApplicationError
}

// Format方法返回payload中的transport header中的the arg scheme： json/thrift
func (response *OutboundCallResponse) Format() Format {
	return Format(response.callRes.Headers[ArgScheme])
}

// Arg2Reader方法获取arg2参数
func (response *OutboundCallResponse) Arg2Reader() (ArgReader, error) {
	var method []byte
	if err := NewArgReader(response.arg1Reader()).Read(&method); err != nil {
		return nil, err
	}

	return response.arg2Reader()
}

// Arg3Reader方法获取arg3参数
func (response *OutboundCallResponse) Arg3Reader() (ArgReader, error) {
	return response.arg3Reader()
}

// handleError handles an error coming back from the peer. If the error is a
// protocol level error, the entire connection will be closed.  If the error is
// a request specific error, it will be written to the request's response
// channel and converted into a SystemError returned from the next reader or
// access call.
// The return value is whether the frame should be released immediately.
func (c *Connection) handleError(frame *Frame) bool {
	errMsg := errorMessage{
		id: frame.Header.ID,
	}
	rbuf := typed.NewReadBuffer(frame.SizedPayload())
	if err := errMsg.read(rbuf); err != nil {
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			ErrField(err),
		).Warn("Unable to read error frame.")
		c.connectionError("parsing error frame", err)
		return true
	}

	if errMsg.errCode == ErrCodeProtocol {
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			LogField{"error", errMsg.message},
		).Warn("Peer reported protocol error.")
		c.connectionError("received protocol error", errMsg.AsSystemError())
		return true
	}

	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		c.log.WithFields(
			LogField{"frameHeader", frame.Header.String()},
			LogField{"id", errMsg.id},
			LogField{"errorMessage", errMsg.message},
			LogField{"errorCode", errMsg.errCode},
			ErrField(err),
		).Info("Failed to forward error frame.")
		return true
	}

	// If the frame was forwarded, then the other side is responsible for releasing the frame.
	return false
}

// 快照克隆tags, 很多地方不一致，有些直接使用的互斥，并作为某个struct的行为
func cloneTags(tags map[string]string) map[string]string {
	newTags := make(map[string]string, len(tags))
	for k, v := range tags {
		newTags[k] = v
	}
	return newTags
}

// doneReading方法，表示当存储OutboundCallResponse所在的Peer，发起rpc调用，并阻塞直到另一方Peer的Response后，这个msg id表示调用结束。则可以关闭这个message exchange
func (response *OutboundCallResponse) doneReading(unexpected error) {
	now := response.timeNow()

	isSuccess := unexpected == nil && !response.ApplicationError()
	lastAttempt := isSuccess || !response.requestState.HasRetries(unexpected)

	// TODO how should this work with retries?
	if span := response.span; span != nil {
		if unexpected != nil {
			span.LogEventWithPayload("error", unexpected)
		}
		if !isSuccess && lastAttempt {
			ext.Error.Set(span, true)
		}
		span.FinishWithOptions(opentracing.FinishOptions{FinishTime: now})
	}

	latency := now.Sub(response.startedAt)
	response.statsReporter.RecordTimer("outbound.calls.per-attempt.latency", response.commonStatsTags, latency)
	if lastAttempt {
		requestLatency := response.requestState.SinceStart(now, latency)
		response.statsReporter.RecordTimer("outbound.calls.latency", response.commonStatsTags, requestLatency)
	}
	if retryCount := response.requestState.RetryCount(); retryCount > 0 {
		retryTags := cloneTags(response.commonStatsTags)
		retryTags["retry-count"] = fmt.Sprint(retryCount)
		response.statsReporter.IncCounter("outbound.calls.retries", retryTags, 1)
	}

	if unexpected != nil {
		// TODO(prashant): Report the error code type as per metrics doc and enable.
		// response.statsReporter.IncCounter("outbound.calls.system-errors", response.commonStatsTags, 1)
	} else if response.ApplicationError() {
		// TODO(prashant): Figure out how to add "type" to tags, which TChannel does not know about.
		response.statsReporter.IncCounter("outbound.calls.per-attempt.app-errors", response.commonStatsTags, 1)
		if lastAttempt {
			response.statsReporter.IncCounter("outbound.calls.app-errors", response.commonStatsTags, 1)
		}
	} else {
		response.statsReporter.IncCounter("outbound.calls.success", response.commonStatsTags, 1)
	}

	// 关闭message exchange
	response.mex.shutdown()
}

// rpc调用时之前，校验service name，method name和transport headers参数
func validateCall(ctx context.Context, serviceName, methodName string, callOpts *CallOptions) error {
	if serviceName == "" {
		return ErrNoServiceName
	}

	if len(methodName) > maxMethodSize {
		return ErrMethodTooLarge
	}

	if _, ok := ctx.Deadline(); !ok {
		return ErrTimeoutRequired
	}

	return nil
}
