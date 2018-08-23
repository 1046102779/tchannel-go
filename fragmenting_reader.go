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
	"bytes"
	"errors"
	"io"

	"github.com/1046102779/tchannel-go/typed"
)

var (
	errMismatchedChecksumTypes  = errors.New("peer returned different checksum types between fragments")
	errMismatchedChecksums      = errors.New("different checksums between peer and local")
	errChunkExceedsFragmentSize = errors.New("peer chunk size exceeds remaining data in fragment")
	errAlreadyReadingArgument   = errors.New("already reading argument")
	errNotReadingArgument       = errors.New("not reading argument")
	errMoreDataInArgument       = errors.New("closed argument reader when there is more data available to read")
	errExpectedMoreArguments    = errors.New("closed argument reader when there may be more data available to read")
	errNoMoreFragments          = errors.New("no more fragments")
)

// readableFragment用于读取rpc调用后阻塞响应获取协议帧，它与message exchange相关联，当后者获取到对方Peer的响应帧后，比如：handleCallRes获取到frame，这个帧通过forwardPeerFrame方法和channel队列发送到正在阻塞等待rpc调用响应的frame中。再通过readableFragment获取独立帧或者分片帧，知道获取到一个完整的帧后，再关闭

// readableFragment用于读取connection数据流，并截取一个个协议帧, 与writableFragment相反
type readableFragment struct {
	isDone       bool
	flags        byte
	checksumType ChecksumType
	checksum     []byte
	contents     *typed.ReadBuffer
	onDone       func()
}

func (f *readableFragment) done() {
	if f.isDone {
		return
	}
	f.onDone()
	f.isDone = true
}

// fragmentReceiver interface用于接收协议帧的分片，并形成一个完整的帧
type fragmentReceiver interface {
	// recvNextFragment returns the next received fragment, blocking until
	// it's available or a deadline/cancel occurs
	recvNextFragment(intial bool) (*readableFragment, error)

	// doneReading is called when the fragment receiver is finished reading all fragments.
	// If an error frame is the last received frame, then doneReading is called with an error.
	doneReading(unexpectedErr error)
}

// 协议帧的分片处理，最终形成一个完整的协议帧
type fragmentingReadState int

const (
	fragmentingReadStart fragmentingReadState = iota
	fragmentingReadInArgument
	fragmentingReadInLastArgument
	fragmentingReadWaitingForArgument
	fragmentingReadComplete
)

func (s fragmentingReadState) isReadingArgument() bool {
	return s == fragmentingReadInArgument || s == fragmentingReadInLastArgument
}

// 读取一个个arg并形成帧
type fragmentingReader struct {
	logger           Logger
	state            fragmentingReadState
	remainingChunks  [][]byte
	curChunk         []byte
	hasMoreFragments bool
	receiver         fragmentReceiver
	curFragment      *readableFragment
	checksum         Checksum
	err              error
}

// newFragmentingReader方法创建一个fragmentingReader实例
//
// reqResReader实现了fragmentReceiver interface
func newFragmentingReader(logger Logger, receiver fragmentReceiver) *fragmentingReader {
	return &fragmentingReader{
		logger:           logger,
		receiver:         receiver,
		hasMoreFragments: true,
	}
}

// ArgReader方法开始读取网络数据中的arg1、arg2和arg3参数
//
// 如果传入参数last为true，表示分片结束；否则，存在分片
func (r *fragmentingReader) ArgReader(last bool) (ArgReader, error) {
	if err := r.BeginArgument(last); err != nil {
		return nil, err
	}
	return r, nil
}

// BeginArgument方法开始获取第一个协议帧
// 如果存在分片, 且还不是最后一个分片，则fragmentReader的state为分片开始;
// 如果为最后一个帧，则state则为最后一个分片
//
// 初始化fragmentingReader的state，并获取一块读取arg1、arg2和arg3参数
func (r *fragmentingReader) BeginArgument(last bool) error {
	if r.err != nil {
		return r.err
	}

	switch {
	case r.state.isReadingArgument():
		r.err = errAlreadyReadingArgument
		return r.err
	case r.state == fragmentingReadComplete:
		r.err = errComplete
		return r.err
	}

	// 当fragmentingReader的state为开始，读取数据到当前remainingChunks
	// 读取arg1、arg2和arg3参数
	if r.state == fragmentingReadStart {
		if r.err = r.recvAndParseNextFragment(true); r.err != nil {
			return r.err
		}
	}

	r.state = fragmentingReadInArgument
	if last {
		r.state = fragmentingReadInLastArgument
	}
	return nil
}

// Read方法读取fragmentingReader中存储的curChunk, 到入参引用中b []byte, 这个是payload arg1、arg2和arg3上， 当入参b []byte填满后，则返回表示一个完整的协议帧填充完毕
func (r *fragmentingReader) Read(b []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	if !r.state.isReadingArgument() {
		r.err = errNotReadingArgument
		return 0, r.err
	}

	totalRead := 0
	for {
		// 把curChunk数据拷贝到引用内存空间，并缩减b []byte的空闲区域和curChunk的剩余数据
		// 直到传入参数b []byte的空闲空间为0，则表示协议帧全部填满
		n := copy(b, r.curChunk)
		totalRead += n
		r.curChunk = r.curChunk[n:]
		b = b[n:]

		if len(b) == 0 {
			return totalRead, nil
		}

		// There wasn't enough data in the current chunk to satisfy the
		// current read.  If there are more chunks in the current
		// fragment, then we've reach the end of this argument.  Return
		// an io.EOF so functions like ioutil.ReadFully know to finish
		if len(r.remainingChunks) > 0 {
			return totalRead, io.EOF
		}

		// Try to fetch more fragments.  If there are no more
		// fragments, then we've reached the end of the argument
		if !r.hasMoreFragments {
			return totalRead, io.EOF
		}

		// 继续读取connection数据流，这个是读取的rpc调用后阻塞等待响应的message exchange, 并填充到fragmentingReader的readableFragment中
		if r.err = r.recvAndParseNextFragment(false); r.err != nil {
			return totalRead, r.err
		}
	}
}

// fragmentingReader关闭
func (r *fragmentingReader) Close() error {
	// 校验协议帧的分片是否为最后一个
	last := r.state == fragmentingReadInLastArgument
	if r.err != nil {
		return r.err
	}

	if !r.state.isReadingArgument() {
		r.err = errNotReadingArgument
		return r.err
	}

	if len(r.curChunk) > 0 {
		// There was more data remaining in the chunk
		r.err = errMoreDataInArgument
		return r.err
	}

	// Several possibilities here:
	// 1. The caller thinks this is the last argument, but there are chunks in the current
	//    fragment or more fragments in this message
	//       - give them an error
	// 2. The caller thinks this is the last argument, and there are no more chunks and no more
	//    fragments
	//       - the stream is complete
	// 3. The caller thinks there are more arguments, and there are more chunks in this fragment
	//       - advance to the next chunk, this is the first chunk for the next argument
	// 4. The caller thinks there are more arguments, and there are no more chunks in this fragment,
	//    but there are more fragments in the message
	//       - retrieve the next fragment, confirm it has an empty chunk (indicating the end of the
	//         current argument), advance to the next check (which is the first chunk for the next arg)
	// 5. The caller thinks there are more arguments, but there are no more chunks or fragments available
	//      - give them an err
	// 如果是最后一个分片已经读写完成，则fragmentingReader的state为协议帧读写完成
	if last {
		if len(r.remainingChunks) > 0 || r.hasMoreFragments {
			// We expect more arguments
			r.err = errExpectedMoreArguments
			return r.err
		}

		r.doneReading(nil)
		r.curFragment.done()
		r.curChunk = nil
		r.state = fragmentingReadComplete
		return nil
	}

	r.state = fragmentingReadWaitingForArgument

	// 如果没有完成，则继续读取数据到外部的内存空间引用
	// curChunk从remainingChunks读取一个，remainingChunks缩容一个[]byte
	if len(r.remainingChunks) > 0 {
		r.curChunk, r.remainingChunks = r.remainingChunks[0], r.remainingChunks[1:]
		return nil
	}

	// 如果协议帧没有更多的分片，且又不是最后一个，就返回没有更多分片了
	if !r.hasMoreFragments {
		r.err = errNoMoreFragments
		return r.err
	}

	// 还有更多的分片需要读取，这个就阻塞rpc调用的响应返回到message exchange获取，
	// 并通过select channel获取
	if r.err = r.recvAndParseNextFragment(false); r.err != nil {
		return r.err
	}

	return nil
}

// rpc调用并阻塞等待另一端Peer的响应，message exchange接收已经建立call req的msg id,
// 并双方等待调用结束
func (r *fragmentingReader) recvAndParseNextFragment(initial bool) error {
	if r.err != nil {
		return r.err
	}

	if r.curFragment != nil {
		r.curFragment.done()
	}

	// 阻塞等待message exchange的forwardPeerFrame获取frame，并通过channel队列发送frame到receiver中
	r.curFragment, r.err = r.receiver.recvNextFragment(initial)
	if r.err != nil {
		if err, ok := r.err.(errorMessage); ok {
			// Serialized system errors are still reported (e.g. latency, trace reporting).
			r.err = err.AsSystemError()
			r.doneReading(r.err)
		}
		return r.err
	}

	// Set checksum, or confirm new checksum is the same type as the prior checksum
	if r.checksum == nil {
		r.checksum = r.curFragment.checksumType.New()
	} else if r.checksum.TypeCode() != r.curFragment.checksumType {
		return errMismatchedChecksumTypes
	}

	// Split fragment into underlying chunks
	r.hasMoreFragments = (r.curFragment.flags & hasMoreFragmentsFlag) == hasMoreFragmentsFlag
	r.remainingChunks = nil
	for r.curFragment.contents.BytesRemaining() > 0 && r.curFragment.contents.Err() == nil {
		// 读取frame的2字节，也就是整个协议帧的长度
		chunkSize := r.curFragment.contents.ReadUint16()
		if chunkSize > uint16(r.curFragment.contents.BytesRemaining()) {
			return errChunkExceedsFragmentSize
		}
		// 并读取一个帧的总数据
		chunkData := r.curFragment.contents.ReadBytes(int(chunkSize))
		// 所以二维数组的remainingChunks，每个[]byte都是一个完整的协议帧
		r.remainingChunks = append(r.remainingChunks, chunkData)
		r.checksum.Add(chunkData)
	}

	if r.curFragment.contents.Err() != nil {
		return r.curFragment.contents.Err()
	}

	// Validate checksums
	localChecksum := r.checksum.Sum()
	if bytes.Compare(r.curFragment.checksum, localChecksum) != 0 {
		r.err = errMismatchedChecksums
		return r.err
	}

	// 从remainingChunks中取出一个协议帧到curChunk中, 且remainingChunks缩容一个未读的所有协议帧
	r.curChunk, r.remainingChunks = r.remainingChunks[0], r.remainingChunks[1:]
	return nil
}

func (r *fragmentingReader) doneReading(err error) {
	if r.checksum != nil {
		r.checksum.Release()
	}
	r.receiver.doneReading(err)
}
