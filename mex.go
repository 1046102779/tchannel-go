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
	"errors"
	"fmt"
	"sync"

	"github.com/uber/tchannel-go/typed"

	"github.com/uber-go/atomic"
	"golang.org/x/net/context"
)

// 这里系统的讲解下message exchange
//
// 当Peer-To-Peer中的一方发起outbound调用时，则就要监听另一方的Response返回，这样就需要使用message exchange阻塞接收frame
// 例如：
//   服务注册的服务channel，发起一个ping请求。如connection.go的388行
//   ...
/*
	// 发送一个ping请求
	if err := c.sendMessage(req); err !=nil {
		return c.connectionError("send ping", err)
	}

	// 阻塞等待另一方的Peer Response, 这个recvMessage方法就是使用了message exchange的channel队列
	return c.recvMessage(ctx, &pingRes{}, mex)
*/

var (
	errDuplicateMex        = errors.New("multiple attempts to use the message id")
	errMexShutdown         = errors.New("mex has been shutdown")
	errMexSetShutdown      = errors.New("mexset has been shutdown")
	errMexChannelFull      = NewSystemError(ErrCodeBusy, "cannot send frame to message exchange channel")
	errUnexpectedFrameType = errors.New("unexpected frame received")
)

const (
	messageExchangeSetInbound  = "inbound"
	messageExchangeSetOutbound = "outbound"

	// mexChannelBufferSize is the size of the message exchange channel buffer.
	mexChannelBufferSize = 2
)

type errNotifier struct {
	c        chan struct{}
	err      error
	notified atomic.Int32
}

func newErrNotifier() errNotifier {
	return errNotifier{c: make(chan struct{})}
}

// Notify will store the error and notify all waiters on c that there's an error.
func (e *errNotifier) Notify(err error) error {
	// The code should never try to Notify(nil).
	if err == nil {
		panic("cannot Notify with no error")
	}

	// There may be some sort of race where we try to notify the mex twice.
	if !e.notified.CAS(0, 1) {
		return fmt.Errorf("cannot broadcast error: %v, already have: %v", err, e.err)
	}

	e.err = err
	close(e.c)
	return nil
}

// checkErr returns previously notified errors (if any).
func (e *errNotifier) checkErr() error {
	select {
	case <-e.c:
		return e.err
	default:
		return nil
	}
}

// messageExchange
// 每当connection上新建一个msg id时，那么则Peer-To-Peer之间发起了一个rpc调用，这个rpc调用完成前，双方都需要等待对方的response, 则这个阻塞等待消息的到来。这里就用到了message exchange，它使用select channel阻塞获取frame
type messageExchange struct {
	recvCh    chan *Frame
	errCh     errNotifier
	ctx       context.Context
	msgID     uint32
	msgType   messageType
	mexset    *messageExchangeSet
	framePool FramePool

	shutdownAtomic atomic.Uint32
	errChNotified  atomic.Uint32
}

// checkError is called before waiting on the mex channels.
// It returns any existing errors (timeout, cancellation, connection errors).
func (mex *messageExchange) checkError() error {
	if err := mex.ctx.Err(); err != nil {
		return GetContextError(err)
	}

	return mex.errCh.checkErr()
}

// forwardPeerFrame方法调用的地方：
// 1. func (c *Connection) handlePingRes(frame *Frame) bool
// 2. func (c *Connection) handleCallReqContinue(frame *Frame) bool
// 3. func (c *Connection) handleCallRes(frame *Frame) bool
// 4. func (c *Connection) handleCallResContinue(frame *Frame) bool
// 5. func (c *Connection) handleError(frame *Frame) bool
//
// 我们可以看到接收的消息帧类型:
// ping res, call req continue, call res, call res continue和error
// 通过这个我们可以看到在接收到这些消息帧时，msg id已经存在，所以connection的一方peer阻塞获取frame
//
// 例如：当Peer-To-Peer的connection一方发起rpc调用时，则阻塞直到对方Peer返回响应帧时，业务逻辑才会网下走，这个阻塞就是通过message exchange的select channel实现
func (mex *messageExchange) forwardPeerFrame(frame *Frame) error {
	// We want a very specific priority here:
	// 1. Timeouts/cancellation (mex.ctx errors)
	// 2. Whether recvCh has buffer space (non-blocking select over mex.recvCh)
	// 3. Other mex errors (mex.errCh)
	// Which is why we check the context error only (instead of mex.checkError).
	// In the mex.errCh case, we do a non-blocking write to recvCh to prioritize it.
	if err := mex.ctx.Err(); err != nil {
		return GetContextError(err)
	}

	select {
	// 把connection上接收到的frame，返回业务处理结果给正在阻塞的rpc调用端
	case mex.recvCh <- frame:
		return nil
	case <-mex.ctx.Done():
		// Note: One slow reader processing a large request could stall the connection.
		// If we see this, we need to increase the recvCh buffer size.
		return GetContextError(mex.ctx.Err())
	case <-mex.errCh.c:
		// Select will randomly choose a case, but we want to prioritize
		// sending a frame over the errCh. Try a non-blocking write.
		select {
		case mex.recvCh <- frame:
			return nil
		default:
		}
		return mex.errCh.err
	}
}

// 校验connection上的msg id，在已建立的exchange message中的msg id是否相同，如果不相同，则错误
func (mex *messageExchange) checkFrame(frame *Frame) error {
	if frame.Header.ID != mex.msgID {
		mex.mexset.log.WithFields(
			LogField{"msgId", mex.msgID},
			LogField{"header", frame.Header},
		).Error("recvPeerFrame received msg with unexpected ID.")
		return errUnexpectedFrameType
	}
	return nil
}

// 存储messageExchange的Peer，阻塞等待接收msg id剩余的消息帧, 直到msg id流程处理完毕
func (mex *messageExchange) recvPeerFrame() (*Frame, error) {
	// We have to check frames/errors in a very specific order here:
	// 1. Timeouts/cancellation (mex.ctx errors)
	// 2. Any pending frames (non-blocking select over mex.recvCh)
	// 3. Other mex errors (mex.errCh)
	// Which is why we check the context error only (instead of mex.checkError)e
	// In the mex.errCh case, we do a non-blocking read from recvCh to prioritize it.
	if err := mex.ctx.Err(); err != nil {
		return nil, GetContextError(err)
	}

	select {
	// 阻塞等待connection的另一端Peer发送消息帧过来,
	// 因为前面说到的ping res，call res...，会直接使用forwardPeerFrame, 把frame写入到channel队列上
	case frame := <-mex.recvCh:
		if err := mex.checkFrame(frame); err != nil {
			return nil, err
		}
		return frame, nil
	case <-mex.ctx.Done():
		return nil, GetContextError(mex.ctx.Err())
	case <-mex.errCh.c:
		// Select will randomly choose a case, but we want to prioritize
		// receiving a frame over errCh. Try a non-blocking read.
		select {
		case frame := <-mex.recvCh:
			if err := mex.checkFrame(frame); err != nil {
				return nil, err
			}
			return frame, nil
		default:
		}
		return nil, mex.errCh.err
	}
}

//  阻塞获取消息帧，其实是调用上面的message exchange的recvPeerFrame方法
func (mex *messageExchange) recvPeerFrameOfType(msgType messageType) (*Frame, error) {
	frame, err := mex.recvPeerFrame()
	if err != nil {
		return nil, err
	}

	switch frame.Header.messageType {
	case msgType:
		return frame, nil

	case messageTypeError:
		// If we read an error frame, we can release it once we deserialize it.
		defer mex.framePool.Release(frame)

		errMsg := errorMessage{
			id: frame.Header.ID,
		}
		var rbuf typed.ReadBuffer
		rbuf.Wrap(frame.SizedPayload())
		if err := errMsg.read(&rbuf); err != nil {
			return nil, err
		}
		return nil, errMsg

	default:
		// TODO(mmihic): Should be treated as a protocol error
		mex.mexset.log.WithFields(
			LogField{"header", frame.Header},
			LogField{"expectedType", msgType},
			LogField{"expectedID", mex.msgID},
		).Warn("Received unexpected frame.")
		return nil, errUnexpectedFrameType
	}
}

// 当一个connection的msg id调用流程完毕时，则移除exchange message
func (mex *messageExchange) shutdown() {
	// 校验message exchange的shutdown是否已关闭
	if !mex.shutdownAtomic.CAS(0, 1) {
		return
	}

	// 通知message exchange已关闭
	if mex.errChNotified.CAS(0, 1) {
		mex.errCh.Notify(errMexShutdown)
	}

	// 从message exchange set中移除msg id的message exchange
	mex.mexset.removeExchange(mex.msgID)
}

// context超时,移除message exchange
func (mex *messageExchange) inboundExpired() {
	mex.mexset.expireExchange(mex.msgID)
}

// message exchange set用于存储connection已经存在的所有msg id，并Peer阻塞接收的消息帧
// 每个connection有两个message exchange set，一个inbound，一个outbound
type messageExchangeSet struct {
	sync.RWMutex

	log        Logger
	name       string
	onRemoved  func()
	onAdded    func()
	sendChRefs sync.WaitGroup

	// maps are mutable, and are protected by the mutex.
	exchanges        map[uint32]*messageExchange
	expiredExchanges map[uint32]struct{}
	shutdown         bool
}

// newMessageExchangeSet创建一个message exchange set实例
// 参数name，有两个值：inbound和outbound
func newMessageExchangeSet(log Logger, name string) *messageExchangeSet {
	return &messageExchangeSet{
		name:             name,
		log:              log.WithFields(LogField{"exchange", name}),
		exchanges:        make(map[uint32]*messageExchange),
		expiredExchanges: make(map[uint32]struct{}),
	}
}

// addExchange方法，添加一个新的message exchange到message exchange set中
func (mexset *messageExchangeSet) addExchange(mex *messageExchange) error {
	// 校验connection的direction是否已经关闭
	if mexset.shutdown {
		return errMexSetShutdown
	}

	// 校验message exchange是否已经存在
	if _, ok := mexset.exchanges[mex.msgID]; ok {
		return errDuplicateMex
	}

	// 存储message exchange到message exchange set中
	mexset.exchanges[mex.msgID] = mex
	// 增加一个goroutine阻塞 waitgroup
	mexset.sendChRefs.Add(1)
	return nil
}

// newExchange方法新建一个message exchange，并添加到message exchange set中
func (mexset *messageExchangeSet) newExchange(ctx context.Context, framePool FramePool,
	msgType messageType, msgID uint32, bufferSize int) (*messageExchange, error) {
	if mexset.log.Enabled(LogLevelDebug) {
		mexset.log.Debugf("Creating new %s message exchange for [%v:%d]", mexset.name, msgType, msgID)
	}

	// 新建一个message exchange实例
	mex := &messageExchange{
		msgType:   msgType,
		msgID:     msgID,
		ctx:       ctx,
		recvCh:    make(chan *Frame, bufferSize),
		errCh:     newErrNotifier(),
		mexset:    mexset,
		framePool: framePool,
	}

	// 把message exchange添加到message exchange set中
	mexset.Lock()
	addErr := mexset.addExchange(mex)
	mexset.Unlock()

	if addErr != nil {
		logger := mexset.log.WithFields(
			LogField{"msgID", mex.msgID},
			LogField{"msgType", mex.msgType},
			LogField{"exchange", mexset.name},
		)
		if addErr == errMexSetShutdown {
			logger.Warn("Attempted to create new mex after mexset shutdown.")
		} else if addErr == errDuplicateMex {
			logger.Warn("Duplicate msg ID for active and new mex.")
		}

		return nil, addErr
	}

	mexset.onAdded()

	// TODO(mmihic): Put into a deadline ordered heap so we can garbage collected expired exchanges
	return mex, nil
}

// deleteExchange移除msg id映射的message exchange
// 这里互斥，写在外面了
func (mexset *messageExchangeSet) deleteExchange(msgID uint32) (found, timedOut bool) {
	// 在message exchange set中存在msg id的message exchange
	// 如果存在，则删除message exchange
	if _, found := mexset.exchanges[msgID]; found {
		delete(mexset.exchanges, msgID)
		return true, false
	}

	// 校验msg id是否已经存在，存在则移除
	if _, expired := mexset.expiredExchanges[msgID]; expired {
		delete(mexset.expiredExchanges, msgID)
		return false, true
	}

	return false, false
}

// removeExchange方法先删除message exchange
// 并使得message exchange set的goroutine数量减1，并触发移除事件
func (mexset *messageExchangeSet) removeExchange(msgID uint32) {
	if mexset.log.Enabled(LogLevelDebug) {
		mexset.log.Debugf("Removing %s message exchange %d", mexset.name, msgID)
	}

	mexset.Lock()
	found, expired := mexset.deleteExchange(msgID)
	mexset.Unlock()

	if !found && !expired {
		mexset.log.WithFields(
			LogField{"msgID", msgID},
		).Error("Tried to remove exchange multiple times")
		return
	}

	// goroutines阻塞，waitgroup-1
	mexset.sendChRefs.Done()
	mexset.onRemoved()
}

// expireExchange方法, 删除message exchange，并从expired移除
func (mexset *messageExchangeSet) expireExchange(msgID uint32) {
	mexset.log.Debugf(
		"Removing %s message exchange %d due to timeout, cancellation or blackhole",
		mexset.name,
		msgID,
	)

	mexset.Lock()
	// TODO(aniketp): explore if cancel can be called everytime we expire an exchange
	found, expired := mexset.deleteExchange(msgID)
	if found || expired {
		// Record in expiredExchanges if we deleted the exchange.
		mexset.expiredExchanges[msgID] = struct{}{}
	}
	mexset.Unlock()

	if expired {
		mexset.log.WithFields(LogField{"msgID", msgID}).Info("Exchange expired already")
	}

	mexset.onRemoved()
}

// waitForSendCh等待所有的inbound与outbound的message exchanges全部移除
func (mexset *messageExchangeSet) waitForSendCh() {
	mexset.sendChRefs.Wait()
}

// 获取message exchange set的connection当前正在调用的msg id总数量
func (mexset *messageExchangeSet) count() int {
	mexset.RLock()
	count := len(mexset.exchanges)
	mexset.RUnlock()

	return count
}

// forwardPeerFrame方法，通过获取到的frame和其中的msg id
func (mexset *messageExchangeSet) forwardPeerFrame(frame *Frame) error {
	if mexset.log.Enabled(LogLevelDebug) {
		mexset.log.Debugf("forwarding %s %s", mexset.name, frame.Header)
	}

	mexset.RLock()
	mex := mexset.exchanges[frame.Header.ID]
	mexset.RUnlock()

	if mex == nil {
		// This is ok since the exchange might have expired or been cancelled
		mexset.log.WithFields(
			LogField{"frameHeader", frame.Header.String()},
			LogField{"exchange", mexset.name},
		).Info("Received frame for unknown message exchange.")
		return nil
	}

	// 接收到的frame，发送到存储message exchange set的Peer正在阻塞等待rpc调用返回的响应
	if err := mex.forwardPeerFrame(frame); err != nil {
		mexset.log.WithFields(
			LogField{"frameHeader", frame.Header.String()},
			LogField{"frameSize", frame.Header.FrameSize()},
			LogField{"exchange", mexset.name},
			ErrField(err),
		).Info("Failed to forward frame.")
		return err
	}

	return nil
}

// copyExchanges方法, 快照克隆message exchanges
func (mexset *messageExchangeSet) copyExchanges() (shutdown bool, exchanges map[uint32]*messageExchange) {
	// 校验message exchange set是否已关闭
	if mexset.shutdown {
		return true, nil
	}

	exchangesCopy := make(map[uint32]*messageExchange, len(mexset.exchanges))
	for k, mex := range mexset.exchanges {
		exchangesCopy[k] = mex
	}

	return false, exchangesCopy
}

// stopExchanges方法，停止connection的direction中的message exchanges停止接收消息
func (mexset *messageExchangeSet) stopExchanges(err error) {
	if mexset.log.Enabled(LogLevelDebug) {
		mexset.log.Debugf("stopping %v exchanges due to error: %v", mexset.count(), err)
	}

	mexset.Lock()
	shutdown, exchanges := mexset.copyExchanges()
	mexset.shutdown = true
	mexset.Unlock()

	if shutdown {
		mexset.log.Debugf("mexset has already been shutdown")
		return
	}

	for _, mex := range exchanges {
		// 通知所有的message exchange停止接收消息
		if mex.errChNotified.CAS(0, 1) {
			mex.errCh.Notify(err)
		}
	}
}
