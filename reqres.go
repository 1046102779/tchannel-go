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
	"fmt"

	"github.com/1046102779/tchannel-go/typed"
)

type errReqResWriterStateMismatch struct {
	state         reqResWriterState
	expectedState reqResWriterState
}

func (e errReqResWriterStateMismatch) Error() string {
	return fmt.Sprintf("attempting write outside of expected state, in %v expected %v",
		e.state, e.expectedState)
}

type errReqResReaderStateMismatch struct {
	state         reqResReaderState
	expectedState reqResReaderState
}

func (e errReqResReaderStateMismatch) Error() string {
	return fmt.Sprintf("attempting read outside of expected state, in %v expected %v",
		e.state, e.expectedState)
}

// reqResWriterState定义了call req与call res中的arg1，arg2和arg3参数
// 也就是说当读取/写入arg1后，再次就是arg2，最后为arg3
type reqResWriterState int

const (
	reqResWriterPreArg1 reqResWriterState = iota
	reqResWriterPreArg2
	reqResWriterPreArg3
	reqResWriterComplete
)

//go:generate stringer -type=reqResWriterState

// 该函数类型用于校验是否有更多的fragment
type messageForFragment func(initial bool) message

// 通过frame.go与message.go，我们可以读写解析协议帧的header与payload部分数据
// 但是call req, call res的arg1， arg2与arg3参数没有解析到
//
// 这个就留给了reqResWriter方法解析读取这些参数
type reqResWriter struct {
	conn               *Connection
	contents           *fragmentingWriter
	mex                *messageExchange
	state              reqResWriterState
	messageForFragment messageForFragment
	log                Logger
	err                error
}

//go:generate stringer -type=reqResReaderState

// 获取写入到协议帧的arg1、arg2和arg3, 获取一个完整协议帧还未填充arg参数完毕的空闲内存引用
func (w *reqResWriter) argWriter(last bool, inState reqResWriterState, outState reqResWriterState) (ArgWriter, error) {
	if w.err != nil {
		return nil, w.err
	}

	if w.state != inState {
		return nil, w.failed(errReqResWriterStateMismatch{state: w.state, expectedState: inState})
	}

	argWriter, err := w.contents.ArgWriter(last)
	if err != nil {
		return nil, w.failed(err)
	}

	w.state = outState
	return argWriter, nil
}

// 获取arg1参数的空闲内存引用
func (w *reqResWriter) arg1Writer() (ArgWriter, error) {
	return w.argWriter(false /* last */, reqResWriterPreArg1, reqResWriterPreArg2)
}

// 获取arg2参数的空闲内存引用
func (w *reqResWriter) arg2Writer() (ArgWriter, error) {
	return w.argWriter(false /* last */, reqResWriterPreArg2, reqResWriterPreArg3)
}

// 获取arg3参数的空闲内存引用
func (w *reqResWriter) arg3Writer() (ArgWriter, error) {
	return w.argWriter(true /* last */, reqResWriterPreArg3, reqResWriterComplete)
}

// newFragment方法创建一个writableFragment实例，用于写入一个完整的协议帧
// 通过newFragment方法可以知道，writableFragment是对frame各个数据的内存空间引用
// 协议帧分为三部分数据： header、payload前半部分(message)和payload后半部分(arg1~arg3)
func (w *reqResWriter) newFragment(initial bool, checksum Checksum) (*writableFragment, error) {
	if err := w.mex.checkError(); err != nil {
		return nil, w.failed(err)
	}

	message := w.messageForFragment(initial)

	// 创建一个frame
	frame := w.conn.opts.FramePool.Get()
	// 获取到message exchange建立的msg id发起的rpc调用
	// 填充msg id和message type到header
	frame.Header.ID = w.mex.msgID
	frame.Header.messageType = message.messageType()

	// WriteBuffer用于写入payload数据
	wbuf := typed.NewWriteBuffer(frame.Payload[:])
	// 获取payload部分的flag 1字节空闲内存引用
	fragment := new(writableFragment)
	fragment.frame = frame
	fragment.flagsRef = wbuf.DeferByte()
	// 用于读写payload数据前半部分到wbuf中, 直到transport headers写入完成
	if err := message.write(wbuf); err != nil {
		return nil, err
	}
	// 再就是checksumtype，checksum和arg1，arg2和arg3
	wbuf.WriteSingleByte(byte(checksum.TypeCode()))
	fragment.checksumRef = wbuf.DeferBytes(checksum.Size())
	fragment.checksum = checksum
	fragment.contents = wbuf
	return fragment, wbuf.Err()
}

// flushFragment方法，当writableFragment写入一个完整的frame后，最终通过channel队列发送frame到connection上, 至此，rpc响应请求帧则发送回去了
func (w *reqResWriter) flushFragment(fragment *writableFragment) error {
	if w.err != nil {
		return w.err
	}

	frame := fragment.frame.(*Frame)
	frame.Header.SetPayloadSize(uint16(fragment.contents.BytesWritten()))

	if err := w.mex.checkError(); err != nil {
		return w.failed(err)
	}
	select {
	case <-w.mex.ctx.Done():
		return w.failed(GetContextError(w.mex.ctx.Err()))
	case <-w.mex.errCh.c:
		return w.failed(w.mex.errCh.err)
	case w.conn.sendCh <- frame:
		return nil
	}
}

// failed marks the writer as having failed
func (w *reqResWriter) failed(err error) error {
	w.log.Debugf("writer failed: %v existing err: %v", err, w.err)
	if w.err != nil {
		return w.err
	}

	w.mex.shutdown()
	w.err = err
	return w.err
}

// reqResReaderState defines the state of a request/response reader
type reqResReaderState int

const (
	reqResReaderPreArg1 reqResReaderState = iota
	reqResReaderPreArg2
	reqResReaderPreArg3
	reqResReaderComplete
)

// reqResReader与reqResWriter类似，这个是用来读取数据到协议帧中
type reqResReader struct {
	contents           *fragmentingReader
	mex                *messageExchange
	state              reqResReaderState
	messageForFragment messageForFragment
	initialFragment    *readableFragment
	previousFragment   *readableFragment
	log                Logger
	err                error
}

// arg1Reader方法读取arg1，并把数据存储到ArgReader中
func (r *reqResReader) arg1Reader() (ArgReader, error) {
	return r.argReader(false /* last */, reqResReaderPreArg1, reqResReaderPreArg2)
}

// arg2Reader方法读取arg2，并把数据存储到ArgReader中
func (r *reqResReader) arg2Reader() (ArgReader, error) {
	return r.argReader(false /* last */, reqResReaderPreArg2, reqResReaderPreArg3)
}

// arg3Reader方法读取arg3，并把数据存储到ArgReader中
func (r *reqResReader) arg3Reader() (ArgReader, error) {
	return r.argReader(true /* last */, reqResReaderPreArg3, reqResReaderComplete)
}

// argReader方法获取arg参数，并修改当前读取网络数据arg的状态: arg1, arg2, arg3
func (r *reqResReader) argReader(last bool, inState reqResReaderState, outState reqResReaderState) (ArgReader, error) {
	if r.state != inState {
		return nil, r.failed(errReqResReaderStateMismatch{state: r.state, expectedState: inState})
	}

	// rpc调用并阻塞等待对方Peer的响应，如：handleCallRes方法从connection上获取到frame后，并通过forwardPeerFrame方法把frame发送到一直在阻塞等待的message exchange中, 最后读取数据到argReader中
	argReader, err := r.contents.ArgReader(last)
	if err != nil {
		return nil, r.failed(err)
	}

	r.state = outState
	return argReader, nil
}

// recvNextFragment方法阻塞读取frame，并存储在readableFragment，它包括协议帧的header、payload前半部分(message)、payload后半部分(checksumType, checksum, arg1~arg3)
func (r *reqResReader) recvNextFragment(initial bool) (*readableFragment, error) {
	if r.initialFragment != nil {
		fragment := r.initialFragment
		r.initialFragment = nil
		r.previousFragment = fragment
		return fragment, nil
	}

	// rpc调用并阻塞等待对方Peer的响应，最后通过message exchange发送接收到的frame给channel队列
	message := r.messageForFragment(initial)
	// 获取到的frame
	frame, err := r.mex.recvPeerFrameOfType(message.messageType())
	if err != nil {
		if err, ok := err.(errorMessage); ok {
			// If we received a serialized error from the other side, then we should go through
			// the normal doneReading path so stats get updated with this error.
			r.err = err.AsSystemError()
			return nil, err
		}

		return nil, r.failed(err)
	}

	// 解析这个协议帧frame，并拆分成三部分存储到readableFragment=header+payload前部分+payload后部分
	fragment, err := parseInboundFragment(r.mex.framePool, frame, message)
	if err != nil {
		return nil, r.failed(err)
	}

	// 存储当前正在处理的previousFragment
	r.previousFragment = fragment
	return fragment, nil
}

// releasePreviousFrament方法释放当前的readableFragment
func (r *reqResReader) releasePreviousFragment() {
	fragment := r.previousFragment
	r.previousFragment = nil
	if fragment != nil {
		fragment.done()
	}
}

// failed indicates the reader failed
func (r *reqResReader) failed(err error) error {
	r.log.Debugf("reader failed: %v existing err: %v", err, r.err)
	if r.err != nil {
		return r.err
	}

	r.mex.shutdown()
	r.err = err
	return r.err
}

// parseInboundFragment解析协议帧frame，到readableFragment
func parseInboundFragment(framePool FramePool, frame *Frame, message message) (*readableFragment, error) {
	// 获取ReadBuffer实例，存储payload数据
	rbuf := typed.NewReadBuffer(frame.SizedPayload())
	fragment := new(readableFragment)
	// 先读取payload 1字节的flag
	fragment.flags = rbuf.ReadSingleByte()
	// 在读取payload前半部分，包括service name，transport headers
	if err := message.read(rbuf); err != nil {
		return nil, err
	}

	// 在最后读取payload后半部分，包括checksumType，checksum和arg1，arg2和arg3
	fragment.checksumType = ChecksumType(rbuf.ReadSingleByte())
	fragment.checksum = rbuf.ReadBytes(fragment.checksumType.ChecksumSize())
	fragment.contents = rbuf
	fragment.onDone = func() {
		framePool.Release(frame)
	}
	return fragment, rbuf.Err()
}
