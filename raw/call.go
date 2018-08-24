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

package raw

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/1046102779/tchannel-go"
)

// ErrAppError is returned if the application sets an error response.
var ErrAppError = errors.New("application error")

// ReadArgsV2 reads arg2 and arg3 from a reader.
func ReadArgsV2(r tchannel.ArgReadable) ([]byte, []byte, error) {
	var arg2, arg3 []byte

	if err := tchannel.NewArgReader(r.Arg2Reader()).Read(&arg2); err != nil {
		return nil, nil, err
	}

	if err := tchannel.NewArgReader(r.Arg3Reader()).Read(&arg3); err != nil {
		return nil, nil, err
	}

	return arg2, arg3, nil
}

// WriteArgs方法写入arg2，arg3到OutboundCall的frame中，并发起rpc调用, 并阻塞等待调用的响应, 读取数据arg2和arg3
//
// 注意，这里没有所谓的业务逻辑处理函数之类的, 就是直接对frame操作
func WriteArgs(call *tchannel.OutboundCall, arg2, arg3 []byte) ([]byte, []byte, *tchannel.OutboundCallResponse, error) {
	if err := tchannel.NewArgWriter(call.Arg2Writer()).Write(arg2); err != nil {
		return nil, nil, nil, err
	}

	if err := tchannel.NewArgWriter(call.Arg3Writer()).Write(arg3); err != nil {
		return nil, nil, nil, err
	}

	resp := call.Response()
	var respArg2 []byte
	if err := tchannel.NewArgReader(resp.Arg2Reader()).Read(&respArg2); err != nil {
		return nil, nil, nil, err
	}

	var respArg3 []byte
	if err := tchannel.NewArgReader(resp.Arg3Reader()).Read(&respArg3); err != nil {
		return nil, nil, nil, err
	}

	return respArg2, respArg3, resp, nil
}

// Call方法直接传入所有协议帧需要的字段数据
func Call(ctx context.Context, ch *tchannel.Channel, hostPort string, serviceName, method string,
	arg2, arg3 []byte) ([]byte, []byte, *tchannel.OutboundCallResponse, error) {

	// 封装OutboundCall，并在frame中写入header、payload前半部分和arg1
	call, err := ch.BeginCall(ctx, hostPort, serviceName, method, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	// 写入arg2、arg3到OutboundCall的frame中，并阻塞返回frame中的arg2、arg3数据
	return WriteArgs(call, arg2, arg3)
}

// CallSC方法直接传入subchannel，并通过它发起rpc调用
func CallSC(ctx context.Context, sc *tchannel.SubChannel, method string, arg2, arg3 []byte) (
	[]byte, []byte, *tchannel.OutboundCallResponse, error) {

	// 封装OutboundCall的frame的header、payload全半部分和arg1
	call, err := sc.BeginCall(ctx, method, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	// rpc调用，并阻塞等待返回frame中的arg2、arg3
	return WriteArgs(call, arg2, arg3)
}

// 这个是对上面的参数method、arg2、arg3封装, 与上面的功能是一样的
// CArgs are the call arguments passed to CallV2.
type CArgs struct {
	Method      string
	Arg2        []byte
	Arg3        []byte
	CallOptions *tchannel.CallOptions
}

// CRes is the result of making a call.
type CRes struct {
	Arg2     []byte
	Arg3     []byte
	AppError bool
}

// CallV2 makes a call and does not attempt any retries.
func CallV2(ctx context.Context, sc *tchannel.SubChannel, cArgs CArgs) (*CRes, error) {
	call, err := sc.BeginCall(ctx, cArgs.Method, cArgs.CallOptions)
	if err != nil {
		return nil, err
	}

	arg2, arg3, res, err := WriteArgs(call, cArgs.Arg2, cArgs.Arg3)
	if err != nil {
		return nil, err
	}

	return &CRes{
		Arg2:     arg2,
		Arg3:     arg3,
		AppError: res.ApplicationError(),
	}, nil
}
