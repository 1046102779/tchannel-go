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

package tchannel

import (
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/1046102779/tchannel-go/typed"
)

// 整个协议帧的数据字段定义，包括两部分：固定的header(16字节)+payload
// payload中的大部分数据都是在message.go中解析进行读写网络数据
/*
Position	Contents
0-7			size:2 type:1 reserved:1 id:4
8-15		reserved:8
16+			payload - based on type
*/
const (
	// MaxFrameSize is the total maximum size for a frame
	MaxFrameSize = math.MaxUint16

	// FrameHeaderSize为协议帧Header占用的字节数, 固定长度：16字节
	FrameHeaderSize = 16

	// MaxFramePayloadSize为payload占用最大字节数量
	MaxFramePayloadSize = MaxFrameSize - FrameHeaderSize
)

// FrameHeader为协议帧头部，占用16字节
// 包括：
// 1. 2字节存储整个协议帧的总长度，
// 2. 1字节的协议帧类型：init req, call req等等
// 3. 1字节的保留
// 4. 4字节的msg id
// 5. 8字节的保留
type FrameHeader struct {
	size uint16

	messageType messageType

	reserved1 byte

	ID uint32

	reserved [8]byte
}

// SetPayloadSize方法设置payload占用字节大小
func (fh *FrameHeader) SetPayloadSize(size uint16) {
	fh.size = size + FrameHeaderSize
}

// PayloadSize方法获取payload占用字节大小
func (fh FrameHeader) PayloadSize() uint16 {
	return fh.size - FrameHeaderSize
}

// FrameSize方法获取总字节大小
func (fh FrameHeader) FrameSize() uint16 {
	return fh.size
}

// 提供两种序列化FrameHeader方式：1. String；2. json Marshal
// 提供String，返回FrameHeader相关信息字符串化
func (fh FrameHeader) String() string { return fmt.Sprintf("%v[%d]", fh.messageType, fh.ID) }

// MarshalJSON returns a `{"id":NNN, "msgType":MMM, "size":SSS}` representation
func (fh FrameHeader) MarshalJSON() ([]byte, error) {
	s := struct {
		ID      uint32      `json:"id"`
		MsgType messageType `json:"msgType"`
		Size    uint16      `json:"size"`
	}{fh.ID, fh.messageType, fh.size}
	return json.Marshal(s)
}

// 这个与message模块类似，message模块主要解析读写payload部分数据
// FrameHeader则主要解析读写Header数据
//
// ReadBuffer中的buffer存储完整的协议帧，remaining存储未读的网络数据
func (fh *FrameHeader) read(r *typed.ReadBuffer) error {
	// 从remaining读取2字节的协议帧总长度
	fh.size = r.ReadUint16()
	// 读取1字节的消息类型
	fh.messageType = messageType(r.ReadSingleByte())
	// 读取1字节的保留位
	fh.reserved1 = r.ReadSingleByte()
	// 读取4字节的msg id
	fh.ID = r.ReadUint32()
	// 读取8字节的保留位
	r.ReadBytes(len(fh.reserved))
	return r.Err()
}

// 写入FrameHeader数据到协议帧的Header, 也就是WriteBuffer的remaining部分
func (fh *FrameHeader) write(w *typed.WriteBuffer) error {
	// 写入2字节的协议帧长度
	w.WriteUint16(fh.size)
	// 写入1字节的消息类型
	w.WriteSingleByte(byte(fh.messageType))
	// 写入1字节的保留位
	w.WriteSingleByte(fh.reserved1)
	// 写入4字节的msg id
	w.WriteUint32(fh.ID)
	// 写入8字节的保留位
	w.WriteBytes(fh.reserved[:])
	return w.Err()
}

// Frame包括两部分：前面的FrameHeader+message部分的payload
// 其中协议帧的头部由FrameHeader进行网络数据读写
// payload数据部分由message部分读写
type Frame struct {
	buffer       []byte // 完整的协议帧
	headerBuffer []byte // 协议帧的头部

	// 对协议帧头部进行数据读写的类
	Header FrameHeader

	// 协议帧的payload
	Payload []byte
}

// NewFrame方法创建一个Frame实例，指定payload的size
// 注意，这是一个空协议帧
func NewFrame(payloadCapacity int) *Frame {
	f := &Frame{}
	f.buffer = make([]byte, payloadCapacity+FrameHeaderSize)
	// 注意：Frame的Payload与headerBuffer都是对buffer的前后半段内存引用
	f.Payload = f.buffer[FrameHeaderSize:]
	f.headerBuffer = f.buffer[:FrameHeaderSize]
	return f
}

// ReadBody方法，读取完整的协议帧消息，并存储到Frame中
func (f *Frame) ReadBody(header []byte, r io.Reader) error {
	// copy内置函数， 拷贝header到Frame的buffer中，这个为协议帧的header
	copy(f.buffer, header)

	// 通过FrameHeader读协议帧的header, 并存储到FrameHeader中
	if err := f.Header.read(typed.NewReadBuffer(header)); err != nil {
		return err
	}

	// 从io.Reader中读取payload数据，这个数据长度由FrameHeader给出
	// 并存储到Frame的payload中
	switch payloadSize := f.Header.PayloadSize(); {
	case payloadSize > MaxFramePayloadSize:
		return fmt.Errorf("invalid frame size %v", f.Header.size)
	case payloadSize > 0:
		_, err := io.ReadFull(r, f.SizedPayload())
		return err
	default:
		// No payload to read
		return nil
	}
	// 注意，由于NewFrame创建Frame实例时，
	// 已经说明了Frame中的payload与headerBuffer对于buffer的内存引用
	//
	// 所以，把header写入到Frame，以及读取指定长度的io.Reader到Frame的payload中
	// 最终都是在Frame中的buffer中完整填充了整个协议帧
}

// WriteOut方法写入Frame的header和payload到io.Writer中, 也就是写入到网络流中
func (f *Frame) WriteOut(w io.Writer) error {
	// 把Frame的HeaderFrame数据写入到headerBuffer中, 也就是写入到的buffer的前半部分
	// 则也就是buffer完整的协议帧
	//
	// 我怀疑这里，不用把Frame的FrameHeader写入到buffer
	// 因为可能Frame的headerBuffer已经存储了header
	wbuf := typed.NewWriteBuffer(f.headerBuffer)

	if err := f.Header.write(&wbuf); err != nil {
		return err
	}

	fullFrame := f.buffer[:f.Header.FrameSize()]
	if _, err := w.Write(fullFrame); err != nil {
		return err
	}

	return nil
}

// SizedPayload方法返回payload数据,或者内存引用
func (f *Frame) SizedPayload() []byte {
	return f.Payload[:f.Header.PayloadSize()]
}

// messageType方法返回协议帧的消息类型
func (f *Frame) messageType() messageType {
	return f.Header.messageType
}

// write方法，message值为payload部分数据，通过WriteBuffer写入到Frame的payload中
func (f *Frame) write(msg message) error {
	// 写入frame的payload数据, 也就是message， 到Frame的payload
	var wbuf typed.WriteBuffer
	wbuf.Wrap(f.Payload[:])
	if err := msg.write(&wbuf); err != nil {
		return err
	}

	// header填充到Frame的HeaderFrame中
	f.Header.ID = msg.ID()
	f.Header.reserved1 = 0
	f.Header.messageType = msg.messageType()
	f.Header.SetPayloadSize(uint16(wbuf.BytesWritten()))
	return nil
}

// 把Frame的payload部分数据写入到message中
func (f *Frame) read(msg message) error {
	var rbuf typed.ReadBuffer
	rbuf.Wrap(f.SizedPayload())
	return msg.read(&rbuf)
}
