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
	"fmt"
	"io"

	"github.com/1046102779/tchannel-go/typed"
)

// thrift frame的arg2参数，上层协议给出了自己的arg2协议格式：
//     len~2 (k~2 v~2)~len
// WriteHeaders方法用于把thrift headers写入io.Writer, 也就是frame arg2的内存空间引用
func WriteHeaders(w io.Writer, headers map[string]string) error {
	// 存储arg2的占用总空间: 2字节
	size := 2
	// 每对键值对：key-value都占用2字节的空间，分别存储字符串的长度
	for k, v := range headers {
		size += 4 /* size of key/value lengths */
		size += len(k) + len(v)
	}

	buf := make([]byte, size)
	writeBuffer := typed.NewWriteBuffer(buf)
	// 写入键值对的总数量到2字节的存储空间
	writeBuffer.WriteUint16(uint16(len(headers)))
	// 遍历headers，并写入到buf中
	for k, v := range headers {
		writeBuffer.WriteLen16String(k)
		writeBuffer.WriteLen16String(v)
	}

	if err := writeBuffer.Err(); err != nil {
		return err
	}

	// Safety check to ensure the bytes written calculation is correct.
	if writeBuffer.BytesWritten() != size {
		return fmt.Errorf(
			"writeHeaders size calculation wrong, expected to write %v bytes, only wrote %v bytes",
			size, writeBuffer.BytesWritten())
	}

	// 直接把buf写入到io.Writer, 也就写入到了frame的arg2内存空间引用部分
	_, err := writeBuffer.FlushTo(w)
	return err
}

// 从frame的arg2内存引用空间, 读取并以map形式返回, 格式如上面所示：
func readHeaders(reader *typed.Reader) (map[string]string, error) {
	// 读取arg2占用空间的总长度
	numHeaders := reader.ReadUint16()
	if numHeaders == 0 {
		return nil, reader.Err()
	}

	// 遍历读取键值对
	headers := make(map[string]string, numHeaders)
	for i := 0; i < int(numHeaders) && reader.Err() == nil; i++ {
		k := reader.ReadLen16String()
		v := reader.ReadLen16String()
		headers[k] = v
	}

	return headers, reader.Err()
}

// 这里可以既可以使用ReaderBuffer，也可以使用Reader小内存, 小内存临时对象池
func ReadHeaders(r io.Reader) (map[string]string, error) {
	reader := typed.NewReader(r)
	m, err := readHeaders(reader)
	reader.Release()

	return m, err
}
