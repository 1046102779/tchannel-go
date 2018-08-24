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
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/1046102779/tchannel-go/internal/argreader"
)

// ArgReader在OutboundCallResponse与InboundCall读取arg2和arg3
type ArgReader io.ReadCloser

// ArgWriter在OutboundCall与InboundCallResponse上写入arg2和arg3
type ArgWriter interface {
	io.WriteCloser

	// Flush flushes the currently written bytes without waiting for the frame
	// to be filled.
	Flush() error
}

// ArgWritable interface获取arg2与arg3的frame相应位置的空闲内存引用
// reqResWriter e.g. OutboundCall and InboundCallResponse
type ArgWritable interface {
	Arg2Writer() (ArgWriter, error)
	Arg3Writer() (ArgWriter, error)
}

// ArgReadable interface获取arg2和arg3的数据流，并读取传入的内存空间
//  reqResReader e.g. InboundCall and OutboundCallResponse.
type ArgReadable interface {
	Arg2Reader() (ArgReader, error)
	Arg3Reader() (ArgReader, error)
}

// ArgReadHelper是一个读取arg参数的简单接口
type ArgReadHelper struct {
	reader ArgReader
	err    error
}

// NewArgReader方法创建一个ArgReadHelper实例，ArgReader存放Frame存储arg的内存空间bytes流
// interface for reading arguments.
func NewArgReader(reader ArgReader, err error) ArgReadHelper {
	return ArgReadHelper{reader, err}
}

// 传入ArgReaderHelper的闭包函数值, 用于下面的Read方法
func (r ArgReadHelper) read(f func() error) error {
	if r.err != nil {
		return r.err
	}
	if err := f(); err != nil {
		return err
	}
	if err := argreader.EnsureEmpty(r.reader, "read arg"); err != nil {
		return err
	}
	return r.reader.Close()
}

// Read方法读取ArgReaderHelper中的Frame指定位置的arg参数到传入参数的内存空间
func (r ArgReadHelper) Read(bs *[]byte) error {
	return r.read(func() error {
		var err error
		// 读取协议帧的arg到bs中
		*bs, err = ioutil.ReadAll(r.reader)
		return err
	})
}

// ReadJSON方法以json协议格式把frame中的arg参数解析存储到data中
func (r ArgReadHelper) ReadJSON(data interface{}) error {
	return r.read(func() error {
		// TChannel allows for 0 length values (not valid JSON), so we use a bufio.Reader
		// to check whether data is of 0 length.
		reader := bufio.NewReader(r.reader)
		if _, err := reader.Peek(1); err == io.EOF {
			// If the data is 0 length, then we don't try to read anything.
			return nil
		} else if err != nil {
			return err
		}

		d := json.NewDecoder(reader)
		return d.Decode(data)
	})
}

// ArgWriteHelper用于把获取到的frame写入到io.WriterCloser中
type ArgWriteHelper struct {
	writer io.WriteCloser
	err    error
}

// NewArgWriter方法创建一个ArgWriteHelper实例, 并把rpc请求数据处理后并封装到Frame, 并写入到io.WriterCloser
func NewArgWriter(writer io.WriteCloser, err error) ArgWriteHelper {
	return ArgWriteHelper{writer, err}
}

// write方法闭包函数，用于下面使用
func (w ArgWriteHelper) write(f func() error) error {
	if w.err != nil {
		return w.err
	}

	if err := f(); err != nil {
		return err
	}

	return w.writer.Close()
}

// Write方法写入bs []byte流到ArgWriteHelper的io.WriterCloser中
func (w ArgWriteHelper) Write(bs []byte) error {
	return w.write(func() error {
		_, err := w.writer.Write(bs)
		return err
	})
}

// WriteJSON方法把data数据已json协议写入到io.WriteCloser
func (w ArgWriteHelper) WriteJSON(data interface{}) error {
	return w.write(func() error {
		e := json.NewEncoder(w.writer)
		return e.Encode(data)
	})
}
