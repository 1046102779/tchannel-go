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

package thrift

import (
	"github.com/1046102779/tchannel-go"
	"github.com/1046102779/tchannel-go/internal/argreader"

	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

// client implements TChanClient and makes outgoing Thrift calls.
type client struct {
	ch          *tchannel.Channel
	sc          *tchannel.SubChannel
	serviceName string
	opts        ClientOptions
}

// ClientOptions are options to customize the client.
type ClientOptions struct {
	// HostPort specifies a specific server to hit.
	HostPort string
}

// NewClient方法创建一个TChanClient实例,用于发起一个rpc调用
func NewClient(ch *tchannel.Channel, serviceName string, opts *ClientOptions) TChanClient {
	client := &client{
		ch:          ch,
		sc:          ch.GetSubChannel(serviceName),
		serviceName: serviceName,
	}
	if opts != nil {
		client.opts = *opts
	}
	return client
}

// 封装一个frame的header、payload前半部分和arg1参数
func (c *client) startCall(ctx context.Context, method string, callOptions *tchannel.CallOptions) (*tchannel.OutboundCall, error) {
	if c.opts.HostPort != "" {
		return c.ch.BeginCall(ctx, c.opts.HostPort, c.serviceName, method, callOptions)
	}
	return c.sc.BeginCall(ctx, method, callOptions)
}

func writeArgs(call *tchannel.OutboundCall, headers map[string]string, req thrift.TStruct) error {
	// 获取OutboundCall的arg2空闲内存引用空间
	writer, err := call.Arg2Writer()
	if err != nil {
		return err
	}
	// 写入headers到frame的arg2内存区域
	headers = tchannel.InjectOutboundSpan(call.Response(), headers)
	if err := WriteHeaders(writer, headers); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	// 获取OutboundCall的arg3空闲内存引用空间
	writer, err = call.Arg3Writer()
	if err != nil {
		return err
	}

	// 写入业务逻辑数据到arg3内存区域
	if err := WriteStruct(writer, req); err != nil {
		return err
	}

	return writer.Close()
}

// readResponse方法是rpc调用阻塞等待响应frame，并把结果赋值给thrift.TStruct
func readResponse(response *tchannel.OutboundCallResponse, resp thrift.TStruct) (map[string]string, bool, error) {
	// 获取读取frame arg2的内存引用空间
	reader, err := response.Arg2Reader()
	if err != nil {
		return nil, false, err
	}

	// 读取arg2到headers
	headers, err := ReadHeaders(reader)
	if err != nil {
		return nil, false, err
	}

	if err := argreader.EnsureEmpty(reader, "reading response headers"); err != nil {
		return nil, false, err
	}

	if err := reader.Close(); err != nil {
		return nil, false, err
	}

	success := !response.ApplicationError()
	// 获取frame的arg3内存引用空间
	reader, err = response.Arg3Reader()
	if err != nil {
		return headers, success, err
	}

	// 把arg3数据读取到resp
	if err := ReadStruct(reader, resp); err != nil {
		return headers, success, err
	}

	if err := argreader.EnsureEmpty(reader, "reading response body"); err != nil {
		return nil, false, err
	}

	return headers, success, reader.Close()
}

func (c *client) Call(ctx Context, thriftService, methodName string, req, resp thrift.TStruct) (bool, error) {
	var (
		headers = ctx.Headers()

		respHeaders map[string]string
		isOK        bool
	)

	err := c.ch.RunWithRetry(ctx, func(ctx context.Context, rs *tchannel.RequestState) error {
		respHeaders, isOK = nil, false

		// 创建一个OutboundCall和封装frame中的header、payload前半部分和arg1
		call, err := c.startCall(ctx, thriftService+"::"+methodName, &tchannel.CallOptions{
			Format:       tchannel.Thrift,
			RequestState: rs,
		})
		if err != nil {
			return err
		}

		// 封装frame的arg2和arg3, 并发起rpc调用
		if err := writeArgs(call, headers, req); err != nil {
			return err
		}

		// 阻塞等待rpc调用响应, 并解析arg3到resp中
		respHeaders, isOK, err = readResponse(call.Response(), resp)
		return err
	})
	if err != nil {
		return false, err
	}

	ctx.SetResponseHeaders(respHeaders)
	return isOK, nil
}
