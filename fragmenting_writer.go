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
	"errors"
	"fmt"

	"github.com/1046102779/tchannel-go/typed"
)

var (
	errAlreadyWritingArgument = errors.New("already writing argument")
	errNotWritingArgument     = errors.New("not writing argument")
	errComplete               = errors.New("last argument already sent")
)

const (
	chunkHeaderSize      = 2    // each chunk is a uint16
	hasMoreFragmentsFlag = 0x01 // flags indicating there are more fragments coming
)

// writableFragment用于读取connection数据流，并形成一个个帧，发送到网络中

// frame.go， message.go与typed已经读写解析和封装了协议帧中header全部和payload部分内容
//
// writableFragment主要包括call req与call res协议帧中相关数据字段的读写
// 包括：flags，csumtype, checksum与arg1, arg2与arg3参数的读写
type writableFragment struct {
	flagsRef    typed.ByteRef
	checksumRef typed.BytesRef
	checksum    Checksum
	contents    *typed.WriteBuffer
	frame       interface{}
}

// finish方法修改writableFragment的相关flag与checksum校验和
func (f *writableFragment) finish(hasMoreFragments bool) {
	// 更新协议帧的校验和
	f.checksumRef.Update(f.checksum.Sum())
	// 更新协议帧中的flag, 如果没有更多的分片，则直接释放掉checksum存储的协议帧
	if hasMoreFragments {
		f.flagsRef.Update(hasMoreFragmentsFlag)
	} else {
		f.checksum.Release()
	}
}

// writableChunk在分片的一个块数据，如分片中的arg1, arg2或者arg3三者之一
type writableChunk struct {
	size     uint16
	sizeRef  typed.Uint16Ref
	checksum Checksum
	contents *typed.WriteBuffer
}

// newWritableChunk方法创建writableChunk实例
func newWritableChunk(checksum Checksum, contents *typed.WriteBuffer) *writableChunk {
	return &writableChunk{
		size: 0,
		// 在WriteBuffer中的remaining首先写入2字节的参数总长度
		// 所有的arg，都是2字节
		sizeRef:  contents.DeferUint16(),
		checksum: checksum,
		contents: contents,
	}
}

// writeAsFits方法写入arg参数值b
func (c *writableChunk) writeAsFits(b []byte) int {
	// 如果WriteBuffer的remaining剩余空间不足的话，就需要截取arg参数值b的长度
	if len(b) > c.contents.BytesRemaining() {
		b = b[:c.contents.BytesRemaining()]
	}

	// 增加checksum要校验的内容
	c.checksum.Add(b)
	// arg参数值写入到WriteBuffer的remaining中
	c.contents.WriteBytes(b)

	// 统计已写入到WriteBuffer的总长度,
	// 其实这个是可以使用WriteBuffer的BytesWritten方法获取的
	written := len(b)
	c.size += uint16(written)
	return written
}

// finish方法更新已写入完成的arg参数总大小，到引用块中
func (c *writableChunk) finish() {
	c.sizeRef.Update(c.size)
}

// fragmentSender interface，reqResWriter实现了它
type fragmentSender interface {
	// newFragment allocates a new fragment
	newFragment(initial bool, checksum Checksum) (*writableFragment, error)

	// flushFragment flushes the given fragment
	flushFragment(f *writableFragment) error

	// doneSending is called when the fragment receiver is finished sending all fragments.
	doneSending()
}

type fragmentingWriterState int

const (
	fragmentingWriteStart fragmentingWriterState = iota
	fragmentingWriteInArgument
	fragmentingWriteInLastArgument
	fragmentingWriteWaitingForArgument
	fragmentingWriteComplete
)

func (s fragmentingWriterState) isWritingArgument() bool {
	return s == fragmentingWriteInArgument || s == fragmentingWriteInLastArgument
}

// A fragmentingWriter writes one or more arguments to an underlying stream,
// breaking them into fragments as needed, and applying an overarching
// checksum.  It relies on an underlying fragmentSender, which creates and
// flushes the fragments as needed
type fragmentingWriter struct {
	logger   Logger
	sender   fragmentSender
	checksum Checksum
	// 通过writableChunk写入一个arg，并同步到writableFragment中
	// 直到arg1，arg2和arg3写入writableFragment中
	curFragment *writableFragment
	curChunk    *writableChunk
	// 写入call req与call res中的payload部分的arg1，arg2和arg3的当前状态
	state fragmentingWriterState
	err   error
}

// newFragmentingWriter方法创建fragmentingWriter实例, 状态为fragmentingWriteStart
// 表示开始准备写入arg三个参数
func newFragmentingWriter(logger Logger, sender fragmentSender, checksum Checksum) *fragmentingWriter {
	return &fragmentingWriter{
		logger:   logger,
		sender:   sender,
		checksum: checksum,
		state:    fragmentingWriteStart,
	}
}

// ArgWriter方法返回一个fragmentingWriter中属性初始化的writableChunk, 写入一个参数
func (w *fragmentingWriter) ArgWriter(last bool) (ArgWriter, error) {
	// 初始化writableChunk和state, 前者用于写入一个arg
	if err := w.BeginArgument(last); err != nil {
		return nil, err
	}
	return w, nil
}

// BeginArgument方法开始获取写入一个arg参数的空闲内存空间writableChunk
// 注意writableChunk是对writableFragment中的空闲内存引用，前部分是已写入的arg。
func (w *fragmentingWriter) BeginArgument(last bool) error {
	if w.err != nil {
		return w.err
	}

	switch {
	case w.state == fragmentingWriteComplete:
		w.err = errComplete
		return w.err
	case w.state.isWritingArgument():
		w.err = errAlreadyWritingArgument
		return w.err
	}

	// If we don't have a fragment, request one
	if w.curFragment == nil {
		initial := w.state == fragmentingWriteStart
		if w.curFragment, w.err = w.sender.newFragment(initial, w.checksum); w.err != nil {
			return w.err
		}
	}

	// If there's no room in the current fragment, freak out.  This will
	// only happen due to an implementation error in the TChannel stack
	// itself
	if w.curFragment.contents.BytesRemaining() <= chunkHeaderSize {
		panic(fmt.Errorf("attempting to begin an argument in a fragment with only %d bytes available",
			w.curFragment.contents.BytesRemaining()))
	}

	// 把writableFragment的contents空间给了writableChunk, 后者用于写入新的arg。
	// 也就是这样的
	// writableFragment：| 已写入的arg | 待写入的arg(writableChunk) |
	//
	w.curChunk = newWritableChunk(w.checksum, w.curFragment.contents)
	// last为true时，表示payload部分的arg1，arg2和arg3已写入完成
	w.state = fragmentingWriteInArgument
	if last {
		w.state = fragmentingWriteInLastArgument
	}
	return nil
}

// Write writes argument data, breaking it into fragments as needed
func (w *fragmentingWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	if !w.state.isWritingArgument() {
		w.err = errNotWritingArgument
		return 0, w.err
	}

	totalWritten := 0
	for {
		// 写入arg参数，如果writableChunk满了，则表示整个协议帧缓冲完成
		// 剩余的则是新的协议帧数据
		bytesWritten := w.curChunk.writeAsFits(b)
		totalWritten += bytesWritten
		// 如果相等，则表示还没写满，需要继续填充协议帧
		if bytesWritten == len(b) {
			// The whole thing fit, we're done
			return totalWritten, nil
		}

		// 需要更多的数据填充这个协议帧
		if w.err = w.Flush(); w.err != nil {
			return totalWritten, w.err
		}

		// 写入了bytesWritten个字节，则继续剩余数据继续写入协议帧
		b = b[bytesWritten:]
	}
}

// Flush方法表示协议帧的部分片段已填充完成，需要继续提供空间继续写入到writableFragment中
func (w *fragmentingWriter) Flush() error {
	// 一个arg参数写入完成
	w.curChunk.finish()
	// 更多的分片需要继续写入
	w.curFragment.finish(true)
	// sender为reqResWriter实例, 实现了fragmentSender interface
	// 发送分片到阻塞等待的rpc调用响应的数据
	if w.err = w.sender.flushFragment(w.curFragment); w.err != nil {
		return w.err
	}

	// 创建一个Fragment实例引用，Write方法继续处理其他分片到来的写入
	if w.curFragment, w.err = w.sender.newFragment(false, w.checksum); w.err != nil {
		return w.err
	}

	// 初始化writableChunk初始化为writableFragments的部分空闲空间引用
	w.curChunk = newWritableChunk(w.checksum, w.curFragment.contents)
	return nil
}

// Close ends the current argument.
func (w *fragmentingWriter) Close() error {
	// 校验call req与call res协议帧的payload部分中的arg三个参数已经填充完成
	last := w.state == fragmentingWriteInLastArgument
	if w.err != nil {
		return w.err
	}

	if !w.state.isWritingArgument() {
		w.err = errNotWritingArgument
		return w.err
	}

	w.curChunk.finish()

	// There are three possibilities here:
	// 1. There are no more arguments
	//      flush with more_fragments=false, mark the stream as complete
	// 2. There are more arguments, but we can't fit more data into this fragment
	//      flush with more_fragments=true, start new fragment, write empty chunk to indicate
	//      the current argument is complete
	// 3. There are more arguments, and we can fit more data into this fragment
	//      update the chunk but leave the current fragment open
	// 写入完成
	if last {
		// No more arguments - flush this final fragment and mark ourselves complete
		w.state = fragmentingWriteComplete
		w.curFragment.finish(false)
		w.err = w.sender.flushFragment(w.curFragment)
		w.sender.doneSending()
		return w.err
	}

	w.state = fragmentingWriteWaitingForArgument
	if w.curFragment.contents.BytesRemaining() > chunkHeaderSize {
		// There's enough room in this fragment for the next argument's
		// initial chunk, so we're done here
		return nil
	}

	// This fragment is full - flush and prepare for another argument
	w.curFragment.finish(true)
	if w.err = w.sender.flushFragment(w.curFragment); w.err != nil {
		return w.err
	}

	if w.curFragment, w.err = w.sender.newFragment(false, w.checksum); w.err != nil {
		return w.err
	}

	// Write an empty chunk to indicate this argument has ended
	w.curFragment.contents.WriteUint16(0)
	return nil
}
