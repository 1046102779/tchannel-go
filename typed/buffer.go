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
	"errors"
	"io"
)

var (
	// ErrEOF is returned when trying to read past end of buffer
	ErrEOF = errors.New("buffer is too small")

	// ErrBufferFull is returned when trying to write past end of buffer
	ErrBufferFull = errors.New("no more room in buffer")
)

// ReadBuffer存储要读取的网络数据
// buffer：存储原始数据，remaining：存储还未读取的数据
// buffer =  |--已读数据--| 未读数据remaining |
// 上面这个buffer表示了buffer与remaining的关系, 前半段是已读数据，后半段是未读数据
type ReadBuffer struct {
	buffer    []byte
	remaining []byte
	err       error
}

// NewReadBuffer方法新建一个ReadBuffer实例
func NewReadBuffer(buffer []byte) *ReadBuffer {
	return &ReadBuffer{buffer: buffer, remaining: buffer}
}

// NewReadBufferWithSizes方法新建一个长度为size的空字节列表的ReadBuffer实例
func NewReadBufferWithSize(size int) *ReadBuffer {
	return &ReadBuffer{buffer: make([]byte, size), remaining: nil}
}

// ReadSingleByte方法从ReadBuffer的remaining内存区读取一个字节
func (r *ReadBuffer) ReadSingleByte() byte {
	b, _ := r.ReadByte()
	return b
}

// ReadByte方法从ReadBuffer中的remaining读取一个字节
func (r *ReadBuffer) ReadByte() (byte, error) {
	// 校验ReadBuffer的error
	if r.err != nil {
		return 0, r.err
	}

	// 如果remaining未读消息字节数小于1，则表示没有可读数据了
	if len(r.remaining) < 1 {
		r.err = ErrEOF
		return 0, r.err
	}

	// 取remaining的第一个字节, 并缩容一字节
	b := r.remaining[0]
	r.remaining = r.remaining[1:]
	return b, nil
}

// ReadBytes从ReadBuffer的remaining数据读取n个字节
func (r *ReadBuffer) ReadBytes(n int) []byte {
	// 校验ReadBuffer的error
	if r.err != nil {
		return nil
	}

	// 校验ReadBuffer的剩余remaining长度与n字节对比
	if len(r.remaining) < n {
		r.err = ErrEOF
		return nil
	}

	// 从ReadBuffer的remaining读取n字节，并从remaining缩容n字节
	b := r.remaining[:n]
	r.remaining = r.remaining[n:]
	return b
}

// ReadString方法读取n字节，并返回字符串。这个是最终调用了ReadBytes方法
// 注意一点：这个很容易产生内存拷贝, string(b)
func (r *ReadBuffer) ReadString(n int) string {
	if b := r.ReadBytes(n); b != nil {
		// TODO(mmihic): This creates a copy, which sucks
		return string(b)
	}

	return ""
}

// ReadUint16方法读取2字节，并通过binary进行大端数据转换
//
// 注意：因为ReadBuffer存储的是网络数据，存在大端转换
func (r *ReadBuffer) ReadUint16() uint16 {
	if b := r.ReadBytes(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}

	return 0
}

// ReadUint32方法读取4字节，并通过binary进行大端数据转换
func (r *ReadBuffer) ReadUint32() uint32 {
	if b := r.ReadBytes(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}

	return 0
}

// ReadUint64方法读取8字节，并通过binary进行大端数据转换
func (r *ReadBuffer) ReadUint64() uint64 {
	if b := r.ReadBytes(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}

	return 0
}

// ReadUvarint reads an unsigned varint from the buffer.
// ::TODO
func (r *ReadBuffer) ReadUvarint() uint64 {
	v, _ := binary.ReadUvarint(r)
	return v
}

// 以下为ReadLenXXString方法，是根据tchannel协议规范读取相关数据
// 这里都是读取tchannel协议中的payload部分。主要是transport headers
// 如：nh~1(hk~1 hv~1){nh}
// 先读取存储数据的长度，再根据长度，读取实际字段数据部分
//
// ReadLen8String方法读取1字节存储的长度，然后再根据长度，获取bytes流并转换为字符串
func (r *ReadBuffer) ReadLen8String() string {
	n := r.ReadSingleByte()
	return r.ReadString(int(n))
}

// ReadLen16String方法读取2字节存储数据的长度，并根据长度，获取bytes流并转换为字符串
func (r *ReadBuffer) ReadLen16String() string {
	n := r.ReadUint16()
	return r.ReadString(int(n))
}

// BytesRemaining方法返回ReadBuffer的未读取数据长度
func (r *ReadBuffer) BytesRemaining() int {
	return len(r.remaining)
}

// FillFrom方法从io.Reader流中读取n字节的数据，并存储到ReadBuffer的buffer中
func (r *ReadBuffer) FillFrom(ior io.Reader, n int) (int, error) {
	if len(r.buffer) < n {
		return 0, ErrEOF
	}

	r.err = nil
	r.remaining = r.buffer[:n]
	return io.ReadFull(ior, r.remaining)
}

// Wrap方法：ReadBuffer重置, 写入传入的bytes流
func (r *ReadBuffer) Wrap(b []byte) {
	r.buffer = b
	r.remaining = b
	r.err = nil
}

// Err方法返回err
func (r *ReadBuffer) Err() error { return r.err }

// 写入bytes流到WriteBuffer
// buffer = | ---已写入--- | 空闲部分remaining |
// buffer有两部分构成：前半部是已写入；后半部是空闲待写入
type WriteBuffer struct {
	buffer    []byte
	remaining []byte
	err       error
}

// NewWriteBuffer方法创建一个大小为len(buffer)的空闲内存大小
func NewWriteBuffer(buffer []byte) *WriteBuffer {
	return &WriteBuffer{buffer: buffer, remaining: buffer}
}

// NewWriteBufferWithSize方法创建一个大小为size的空闲内存大小
func NewWriteBufferWithSize(size int) *WriteBuffer {
	return NewWriteBuffer(make([]byte, size))
}

// WriteSingleByte方法写入1个字节到buffer中
func (w *WriteBuffer) WriteSingleByte(n byte) {
	// 如果error有错误，直接返回
	if w.err != nil {
		return
	}

	// 若可用内存区的空闲大小为0，则返回buffer为full的错误
	if len(w.remaining) == 0 {
		w.err = ErrBufferFull
		return
	}

	// 占用空闲内存remaining一个字节， 并缩容1字节
	w.remaining[0] = n
	w.remaining = w.remaining[1:]
}

// WriteBytes方法写入n字节的bytes流
func (w *WriteBuffer) WriteBytes(in []byte) {
	// 从remaining获取n字节的空闲内存引用, 并通过copy内置函数进行内存拷贝
	if b := w.reserve(len(in)); b != nil {
		copy(b, in)
	}
}

// WriteUint16方法写入值为n的2字节数据, 都是先通过reserve获取空闲内存引用
func (w *WriteBuffer) WriteUint16(n uint16) {
	if b := w.reserve(2); b != nil {
		binary.BigEndian.PutUint16(b, n)
	}
}

// WriteUint32方法写入值为n的4字节数据，都是先通过reserve获取空闲内存引用
// 因为是传输到网络上的数据，所以是网络大端，需要转换
func (w *WriteBuffer) WriteUint32(n uint32) {
	if b := w.reserve(4); b != nil {
		binary.BigEndian.PutUint32(b, n)
	}
}

// WriteUint64方法写入值为n的8字节数据，都是先通过reserve获取8字节的空闲内存引用
func (w *WriteBuffer) WriteUint64(n uint64) {
	if b := w.reserve(8); b != nil {
		binary.BigEndian.PutUint64(b, n)
	}
}

// WriteUvarint writes an unsigned varint to the buffer
func (w *WriteBuffer) WriteUvarint(n uint64) {
	// A uvarint could be up to 10 bytes long.
	buf := make([]byte, 10)
	varBytes := binary.PutUvarint(buf, n)
	if b := w.reserve(varBytes); b != nil {
		copy(b, buf[0:varBytes])
	}
}

// WriteString方法写入字符串为s的len(s)长度空闲内存区域，并copy内置函数写入到引用内存
func (w *WriteBuffer) WriteString(s string) {
	// NB(mmihic): Don't just call WriteBytes; that will make a double copy
	// of the string due to the cast
	if b := w.reserve(len(s)); b != nil {
		copy(b, s)
	}
}

// WriteLen8String方法与ReadBuffer的ReadLenXXString方法
// nh~1(hk~1 hv~1){nh}
// WriteLenXXString方法写入tchannel协议规范的payload部分的transport headers
func (w *WriteBuffer) WriteLen8String(s string) {
	// 先写入长度为len(s)并占用1字节的空间
	w.WriteSingleByte(byte(len(s)))
	// 再写入s字符串到内存中
	w.WriteString(s)
}

// WriteLen16String方法写入存储数据长度为2字节的长度大小，并存储s字符串
func (w *WriteBuffer) WriteLen16String(s string) {
	// 先写入长度len(s)到2字节空间中
	w.WriteUint16(uint16(len(s)))
	// 再写入s字符串到内存空间
	w.WriteString(s)
}

// DeferByte方法获取1字节的空闲内存
func (w *WriteBuffer) DeferByte() ByteRef {
	// 如果remaining空闲内存为0，则表示buffer已满错误
	if len(w.remaining) == 0 {
		w.err = ErrBufferFull
		return ByteRef(nil)
	}

	// 从remaining获取1字节空闲内存, 缩容1字节
	w.remaining[0] = 0
	bufRef := ByteRef(w.remaining[0:])
	w.remaining = w.remaining[1:]
	return bufRef
}

// DeferUint16方法获取2字节的空闲内存引用
func (w *WriteBuffer) DeferUint16() Uint16Ref {
	return Uint16Ref(w.deferred(2))
}

// DeferUint32方法获取4字节的空闲内存引用
func (w *WriteBuffer) DeferUint32() Uint32Ref {
	return Uint32Ref(w.deferred(4))
}

// DeferUint64方法获取8字节的空闲内存引用
func (w *WriteBuffer) DeferUint64() Uint64Ref {
	return Uint64Ref(w.deferred(8))
}

// DeferBytes方法获取n字节的空闲内存引用
func (w *WriteBuffer) DeferBytes(n int) BytesRef {
	return BytesRef(w.deferred(n))
}

// 获取已初始化的n字节内存空闲引用
func (w *WriteBuffer) deferred(n int) []byte {
	bs := w.reserve(n)
	for i := range bs {
		bs[i] = 0
	}
	return bs
}

// reserve方法从WriteBuffer的remaining空闲内存区域获取n字节的内存引用
func (w *WriteBuffer) reserve(n int) []byte {
	if w.err != nil {
		return nil
	}

	// 如果空闲内存不够，则报空闲内存不够的错误
	if len(w.remaining) < n {
		w.err = ErrBufferFull
		return nil
	}

	// 获取remaining的空闲内存N个字节, 并缩容n字节
	b := w.remaining[0:n]
	w.remaining = w.remaining[n:]
	return b
}

// BytesRemaining方法返回空闲剩余内存大小
func (w *WriteBuffer) BytesRemaining() int {
	return len(w.remaining)
}

// FlushTo方法把已写入的内存数据写入到io.Writer中
func (w *WriteBuffer) FlushTo(iow io.Writer) (int, error) {
	dirty := w.buffer[0:w.BytesWritten()]
	return iow.Write(dirty)
}

// BytesWritten方法返回已写入的内存空间大小
func (w *WriteBuffer) BytesWritten() int { return len(w.buffer) - len(w.remaining) }

// Reset重置Buffer为全部内存空闲区域
func (w *WriteBuffer) Reset() {
	w.remaining = w.buffer
	w.err = nil
}

// Err方法返回error
func (w *WriteBuffer) Err() error { return w.err }

// Wrap重置WriteBuffer的内存空间指向传入的bytes空间，全部为空闲内存区域
func (w *WriteBuffer) Wrap(b []byte) {
	w.buffer = b
	w.remaining = b
}

// A ByteRef is a reference to a byte in a bufffer
type ByteRef []byte

// Update updates the byte in the buffer
func (ref ByteRef) Update(b byte) {
	if ref != nil {
		ref[0] = b
	}
}

// A Uint16Ref is a reference to a uint16 placeholder in a buffer
type Uint16Ref []byte

// Update updates the uint16 in the buffer
func (ref Uint16Ref) Update(n uint16) {
	if ref != nil {
		binary.BigEndian.PutUint16(ref, n)
	}
}

// A Uint32Ref is a reference to a uint32 placeholder in a buffer
type Uint32Ref []byte

// Update updates the uint32 in the buffer
func (ref Uint32Ref) Update(n uint32) {
	if ref != nil {
		binary.BigEndian.PutUint32(ref, n)
	}
}

// A Uint64Ref is a reference to a uin64 placeholder in a buffer
type Uint64Ref []byte

// Update updates the uint64 in the buffer
func (ref Uint64Ref) Update(n uint64) {
	if ref != nil {
		binary.BigEndian.PutUint64(ref, n)
	}
}

// A BytesRef is a reference to a multi-byte placeholder in a buffer
type BytesRef []byte

// Update updates the bytes in the buffer
func (ref BytesRef) Update(b []byte) {
	if ref != nil {
		copy(ref, b)
	}
}

// UpdateString updates the bytes in the buffer from a string
func (ref BytesRef) UpdateString(s string) {
	if ref != nil {
		copy(ref, s)
	}
}
