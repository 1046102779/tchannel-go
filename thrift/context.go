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
	"time"

	"context"

	"github.com/1046102779/tchannel-go"
)

// Context is a Thrift Context which contains request and response headers.
type Context tchannel.ContextWithHeaders

// NewContext returns a Context that can be used to make Thrift calls.
func NewContext(timeout time.Duration) (Context, context.CancelFunc) {
	ctx, cancel := tchannel.NewContext(timeout)
	return Wrap(ctx), cancel
}

// Wrap returns a Thrift Context that wraps around a Context.
func Wrap(ctx context.Context) Context {
	return tchannel.Wrap(ctx)
}

// WithHeaders returns a Context that can be used to make a call with request headers.
func WithHeaders(ctx context.Context, headers map[string]string) Context {
	return tchannel.WrapWithHeaders(ctx, headers)
}