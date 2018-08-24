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

package json

import (
	"fmt"
	"reflect"

	"github.com/1046102779/tchannel-go"

	"context"

	"github.com/opentracing/opentracing-go"
)

var (
	typeOfError   = reflect.TypeOf((*error)(nil)).Elem()
	typeOfContext = reflect.TypeOf((*Context)(nil)).Elem()
)

// Handlers is the map from method names to handlers.
type Handlers map[string]interface{}

// func(json.Context, *ArgType)(*ResType, error)
// verifyHandler方法验证服务注册的对外提供的业务处理函数列表是否满足要求
func verifyHandler(t reflect.Type) error {
	// 1. Handler的输入和输出必须为2个参数，
	// 输入：context，in
	// 输出：out, error
	if t.NumIn() != 2 || t.NumOut() != 2 {
		return fmt.Errorf("handler should be of format func(json.Context, *ArgType) (*ResType, error)")
	}

	// 提供校验输入参数in，是否为struct或者为map参数，如果不是，则返回错误
	isStructPtr := func(t reflect.Type) bool {
		return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct
	}
	isMap := func(t reflect.Type) bool {
		return t.Kind() == reflect.Map && t.Key().Kind() == reflect.String
	}
	// 校验返回参数out，是否为struct指针类型或者map类型
	validateArgRes := func(t reflect.Type, name string) error {
		if !isStructPtr(t) && !isMap(t) {
			return fmt.Errorf("%v should be a pointer to a struct, or a map[string]interface{}", name)
		}
		return nil
	}

	// 校验输入第一个参数是否为context
	if t.In(0) != typeOfContext {
		return fmt.Errorf("arg0 should be of type json.Context")
	}
	// 校验输入的第二个参数，和返回的第一个参数
	if err := validateArgRes(t.In(1), "second argument"); err != nil {
		return err
	}
	if err := validateArgRes(t.Out(0), "first return value"); err != nil {
		return err
	}
	// 校验输出的第二个参数是否为error
	if !t.Out(1).AssignableTo(typeOfError) {
		return fmt.Errorf("second return value should be an error")
	}

	return nil
}

// 这里就是tchannel底层协议的接收函数处理，并通过该handler进行反射和组装成函数调用
// 提供给上层业务逻辑函数处理
type handler struct {
	handler  reflect.Value
	argType  reflect.Type
	isArgMap bool
	tracer   func() opentracing.Tracer
}

// 对于服务注册的服务方法列表，需要一个个校验并解析为handle中的元素属性
func toHandler(f interface{}) (*handler, error) {
	hV := reflect.ValueOf(f)
	// 校验服务注册的方法是否满足要求
	if err := verifyHandler(hV.Type()); err != nil {
		return nil, err
	}
	// 获取输入参数类型
	argType := hV.Type().In(1)
	return &handler{handler: hV, argType: argType, isArgMap: argType.Kind() == reflect.Map}, nil
}

// func(context.Context, *ArgType)(*ResType, error)
// Register方法进行服务注册到指定的subchannel中的handlerMap
// 这里就是把服务注册的方法封装成tchannel底层能够接受的统一函数处理形式
func Register(registrar tchannel.Registrar, funcs Handlers, onError func(context.Context, error)) error {
	handlers := make(map[string]*handler)

	handler := tchannel.HandlerFunc(func(ctx context.Context, call *tchannel.InboundCall) {
		// 通过method找到handler，并通过反射组装函数调用，也就是调用了服务注册的服务方法
		// reflect.Call
		h, ok := handlers[string(call.Method())]
		if !ok {
			onError(ctx, fmt.Errorf("call for unregistered method: %s", call.Method()))
			return
		}

		if err := h.Handle(ctx, call); err != nil {
			onError(ctx, err)
		}
	})

	// 遍历服务注册的方法列表，并校验方法和创建Handle
	for m, f := range funcs {
		h, err := toHandler(f)
		if err != nil {
			return fmt.Errorf("%v cannot be used as a handler: %v", m, err)
		}
		h.tracer = func() opentracing.Tracer {
			return tchannel.TracerFromRegistrar(registrar)
		}
		handlers[m] = h
		// 注册到指定的subchannel的handlerMap中
		registrar.Register(handler, m)
	}

	return nil
}

// Handle方法是tchannel底层最终会通过subchannel的handlerMap找到handle, 并通过反射调用进行业务逻辑处理
func (h *handler) Handle(tctx context.Context, call *tchannel.InboundCall) error {
	var headers map[string]string
	// 读取InboundCall中的frame帧arg2参数
	if err := tchannel.NewArgReader(call.Arg2Reader()).ReadJSON(&headers); err != nil {
		return fmt.Errorf("arg2 read failed: %v", err)
	}
	tctx = tchannel.ExtractInboundSpan(tctx, call, headers, h.tracer())
	ctx := WithHeaders(tctx, headers)

	var arg3 reflect.Value
	var callArg reflect.Value
	// 根据输入的第二个参数类型，来创建数据值，主要是struct指针数据，或者是map数据
	// 并把值赋给callArg
	if h.isArgMap {
		arg3 = reflect.New(h.argType)
		// New returns a pointer, but the method accepts the map directly.
		callArg = arg3.Elem()
	} else {
		arg3 = reflect.New(h.argType.Elem())
		callArg = arg3
	}
	// 读取InboundCall中的frame帧arg3数据, 作为业务处理并返回的业务结果
	// 读取的arg3，也写入到了callArg
	if err := tchannel.NewArgReader(call.Arg3Reader()).ReadJSON(arg3.Interface()); err != nil {
		return fmt.Errorf("arg3 read failed: %v", err)
	}

	// 构建函数的两个入参值，分别是context和arg3
	args := []reflect.Value{reflect.ValueOf(ctx), callArg}
	// 发起调用，这样也就是上层业务逻辑函数处理
	results := h.handler.Call(args)

	// 获取业务逻辑返回结果：out，error
	res := results[0].Interface()
	err := results[1].Interface()
	// If an error was returned, we create an error arg3 to respond with.
	if err != nil {
		// TODO(prashantv): More consistent error handling between json/raw/thrift..
		if serr, ok := err.(tchannel.SystemError); ok {
			return call.Response().SendSystemError(serr)
		}

		call.Response().SetApplicationError()
		// TODO(prashant): Allow client to customize the error in more ways.
		res = struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}{
			Type:    "error",
			Message: err.(error).Error(),
		}
	}

	if err := tchannel.NewArgWriter(call.Response().Arg2Writer()).WriteJSON(ctx.ResponseHeaders()); err != nil {
		return err
	}

	// 并把返回结果res写入到InboundCall中的response
	return tchannel.NewArgWriter(call.Response().Arg3Writer()).WriteJSON(res)
}
