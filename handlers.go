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
	"reflect"
	"runtime"
	"sync"

	"golang.org/x/net/context"
)

// Handler interface用于tchannel的rpc服务端底层统一接收，
// 再通过上层的具体协议json/thrift/http/raw等进行反射到具体的业务逻辑处理方法
type Handler interface {
	// Handles an incoming call for service
	Handle(ctx context.Context, call *InboundCall)
}

//  HandlerFunc函数类型类似于HTTP的func(ResponseWriter, *Request)
type HandlerFunc func(ctx context.Context, call *InboundCall)

// Handle calls f(ctx, call)
func (f HandlerFunc) Handle(ctx context.Context, call *InboundCall) { f(ctx, call) }

// An ErrorHandlerFunc is an adapter to allow the use of ordinary functions as
// Channel handlers, with error handling convenience.  If f is a function with
// the appropriate signature, then ErrorHandlerFunc(f) is a Handler object that
// calls f.
type ErrorHandlerFunc func(ctx context.Context, call *InboundCall) error

// Handle calls f(ctx, call)
func (f ErrorHandlerFunc) Handle(ctx context.Context, call *InboundCall) {
	if err := f(ctx, call); err != nil {
		if GetSystemErrorCode(err) == ErrCodeUnexpected {
			call.log.WithFields(f.getLogFields()...).WithFields(ErrField(err)).Error("Unexpected handler error")
		}
		call.Response().SendSystemError(err)
	}
}

func (f ErrorHandlerFunc) getLogFields() LogFields {
	ptr := reflect.ValueOf(f).Pointer()
	handlerFunc := runtime.FuncForPC(ptr) // can't be nil
	fileName, fileLine := handlerFunc.FileLine(ptr)
	return LogFields{
		{"handlerFuncName", handlerFunc.Name()},
		{"handlerFuncFileName", fileName},
		{"handlerFuncFileLine", fileLine},
	}
}

// 在subchannel.go中已经说明了subchannel中存储service name对外提供的所有服务方法列表
type handlerMap struct {
	sync.RWMutex

	handlers map[string]Handler
}

// Registers方法注册subchannel中service name对外提供的服务方法
//
// 注意大家可能有疑问的一点:
// Handler func(context.Context, *InboundCall)
// 但是理论上Handler应该是rpc服务端的方法列表中的一个，与具体的业务逻辑相关的，肯定不是上面这个参数形式
// 这是因为在上层具体的协议中，如：json，会把注册的方法进行封装成这种形式：tchannel.HandlerFunc
//
// 具体见: json/handler.go 第96行
func (hmap *handlerMap) register(h Handler, method string) {
	hmap.Lock()
	defer hmap.Unlock()

	if hmap.handlers == nil {
		hmap.handlers = make(map[string]Handler)
	}

	hmap.handlers[method] = h
}

// Finds方法通过method方法名找到对应的业务处理函数
func (hmap *handlerMap) find(method []byte) Handler {
	hmap.RLock()
	handler := hmap.handlers[string(method)]
	hmap.RUnlock()

	return handler
}

// 当一个peer rpc service接收到rpc client的请求后，并通过subchannel的handleMap和method找到对应的上层协议封装的handle，最后通过反射到业务逻辑处理函数
func (hmap *handlerMap) Handle(ctx context.Context, call *InboundCall) {
	c := call.conn
	// 找到handle
	h := hmap.find(call.Method())
	if h == nil {
		c.log.WithFields(
			LogField{"serviceName", call.ServiceName()},
			LogField{"method", call.MethodString()},
		).Error("Couldn't find handler.")
		call.Response().SendSystemError(
			NewSystemError(ErrCodeBadRequest, "no handler for service %q and method %q", call.ServiceName(), call.Method()))
		return
	}

	if c.log.Enabled(LogLevelDebug) {
		c.log.Debugf("Dispatching %s:%s from %s", call.ServiceName(), call.Method(), c.remotePeerInfo)
	}
	// 交给上层具体协议进行解析和业务逻辑处理
	h.Handle(ctx, call)
}

// channelHandler的Handle，我们可以发现最终channel的处理最终还是交给subchannel处理的
type channelHandler struct{ ch *Channel }

func (c channelHandler) Handle(ctx context.Context, call *InboundCall) {
	c.ch.GetSubChannel(call.ServiceName()).handler.Handle(ctx, call)
}
