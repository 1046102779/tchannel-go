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

package typed

import (
	"encoding/binary"
	"io"
	"sync"
)

const maxPoolStringLen = 32

// Reader小内存从io.Reader中读取数据, 并临时存储在buf中返回
// 这个小内存分配使用，采用了临时对象池分配，减少内存滥用
type Reader struct {
	reader io.Reader
	err    error
	buf    [maxPoolStringLen]byte
}

// 临时对象池
var readerPool = sync.Pool{
	New: func() interface{} {
		return &Reader{}
	},
}

// NewReader方法从临时对象池获取一个Reader实例
func NewReader(reader io.Reader) *Reader {
	r := readerPool.Get().(*Reader)
	r.reader = reader
	r.err = nil
	return r
}

// ReadUint16方法从Reader的属性io.Reader中读取2字节的数据
func (r *Reader) ReadUint16() uint16 {
	if r.err != nil {
		return 0
	}

	// 从32字节的内存获取2字节内存空间，读取io.Reader数据到buf中，并返回
	buf := r.buf[:2]

	var readN int
	readN, r.err = io.ReadFull(r.reader, buf)
	if readN < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(buf)
}

// ReadString读取N字节的内存空间
func (r *Reader) ReadString(n int) string {
	if r.err != nil {
		return ""
	}

	// 如果要读取N字节的数据超过了32字节，则只能使用临时分配内存
	// 这个Reader是支持小内存，大内存不支持
	var buf []byte
	if n <= maxPoolStringLen {
		buf = r.buf[:n]
	} else {
		buf = make([]byte, n)
	}

	var readN int
	readN, r.err = io.ReadFull(r.reader, buf)
	if readN < n {
		return ""
	}
	// 这里有个内存拷贝
	s := string(buf)

	return s
}

// ReadLen16String方法类似于
//
// ReadBuffer中的ReadLenXXString与WriteBuffer中的WriteLenXXString
func (r *Reader) ReadLen16String() string {
	len := r.ReadUint16()
	return r.ReadString(int(len))
}

// Err returns any errors hit while reading from the underlying reader.
func (r *Reader) Err() error {
	return r.err
}

// Release方法释放临时对象池
func (r *Reader) Release() {
	readerPool.Put(r)
}
