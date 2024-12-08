// Copyright 2022 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package netpoll

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

func openPoll() (Poll, error) {
	return openDefaultPoll()
}

// openDefaultPoll 创建epoll 封装多路复用器的对象
func openDefaultPoll() (*defaultPoll, error) {
	poll := new(defaultPoll)

	poll.buf = make([]byte, 8)
	// 调用EpollCreate创建epoll对象
	p, err := EpollCreate(0)
	if err != nil {
		return nil, err
	}
	poll.fd = p

	// eventfd是一种进程/线程通信的机制，他类似信号，不过eventfd只是一种通知机制
	// 无法承载数据（eventfd承载的数据是8个字节），他的好处是简单并且只消耗一个fd
	// 进程间通信机制: https://zhuanlan.zhihu.com/p/383395277
	//此处使用eventFD是为了epoll池关闭的时候，通知那些阻塞在epoll_wait系统调用上的线程可以醒过来，然后结束自己。
	r0, _, e0 := syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if e0 != 0 {
		_ = syscall.Close(poll.fd)
		return nil, e0
	}

	poll.Reset = poll.reset
	poll.Handler = poll.handler
	poll.wop = &FDOperator{FD: int(r0)}

	// 在epoll上注册并监听eventFd的可读事件 -- 监听r0上的可读事件
	if err = poll.Control(poll.wop, PollReadable); err != nil {
		_ = syscall.Close(poll.wop.FD)
		_ = syscall.Close(poll.fd)
		return nil, err
	}

	poll.opcache = newOperatorCache()
	return poll, nil
}

// 封装多路复用器的对象
type defaultPoll struct {
	pollArgs
	fd  int         // epoll fd
	wop *FDOperator // eventfd存在wop属性中,  eventfd 是一种轻量级进程通信机制,其工作方式类似于信号机制,无法传递数据,只能用来唤醒等待进程.
	// netpoll 这里使用 eventfd 是为了在系统终止时,发出信号唤醒阻塞在wait系统调用上的epoll,让其随后终止自己
	buf     []byte         // read wfd trigger msg
	trigger uint32         // trigger flag
	m       sync.Map       //nolint:unused // only used in go:race
	opcache *operatorCache // operator cache
	// fns for handle events
	Reset   func(size, caps int)
	Handler func(events []epollevent) (closed bool) // 发生感兴趣事件时，回调该接口处理这些事件
}

type pollArgs struct {
	size     int
	caps     int
	events   []epollevent // 发送/接收感兴趣事件
	barriers []barrier    // 用于实现分散读/集中写的向量缓冲区
	hups     []func(p Poll) error
}

func (a *pollArgs) reset(size, caps int) {
	a.size, a.caps = size, caps
	a.events, a.barriers = make([]epollevent, size), make([]barrier, size)
	for i := range a.barriers {
		a.barriers[i].bs = make([][]byte, a.caps)
		a.barriers[i].ivs = make([]syscall.Iovec, a.caps)
	}
}

// Wait implements Poll.
// 不断轮询注册到该epoll上的fd事件
func (p *defaultPoll) Wait() (err error) {
	// init
	caps, msec, n := barriercap, -1, 0
	p.Reset(128, caps)
	// wait
	for {
		if n == p.size && p.size < 128*1024 {
			p.Reset(p.size<<1, caps)
		}

		// p.fd 就是 epoll fd
		// events 就是挂载到epoll tree上的epoll item
		// mesc 用于指定阻塞时间，是永久阻塞，还是阻塞一段时间，还是非阻塞IO
		// 等待当前epoll上发生感兴趣的事件
		n, err = EpollWait(p.fd, p.events, msec)
		if err != nil && err != syscall.EINTR {
			return err
		}

		// 如果没有发生感兴趣的事件，则将msec设置为-1，表示下一次采用永久阻塞策略来等待感兴趣的事件发生
		// 然后调用Gosched完成协程调度
		if n <= 0 {
			msec = -1
			runtime.Gosched()
			continue
		}
		msec = 0

		// 处理感兴趣的事件
		//defaultPoll的Handler回调接口是在openDefaultPoll函数中被赋值的
		if p.Handler(p.events[:n]) {
			return nil
		}
		// we can make sure that there is no op remaining if Handler finished
		p.opcache.free()
	}
}

// 当epoll上有感兴趣的事件发生的时候,调用该函数进行处理
func (p *defaultPoll) handler(events []epollevent) (closed bool) {
	var triggerRead, triggerWrite, triggerHup, triggerError bool
	var err error
	// 遍历所有感兴趣的事件
	for i := range events {
		// epollevent.data保存的是与之关联的FDOperator对象
		operator := p.getOperator(0, unsafe.Pointer(&events[i].data))
		if operator == nil || !operator.do() {
			continue
		}

		var totalRead int
		// 判断当前发生了什么事件
		evt := events[i].events
		triggerRead = evt&syscall.EPOLLIN != 0
		triggerWrite = evt&syscall.EPOLLOUT != 0
		triggerHup = evt&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0
		triggerError = evt&syscall.EPOLLERR != 0

		// trigger or exit gracefully
		// 是否是eventFD可读事件发生了
		if operator.FD == p.wop.FD {
			// must clean trigger first
			// 从eventFD中读取数据到buf中
			syscall.Read(p.wop.FD, p.buf)
			atomic.StoreUint32(&p.trigger, 0)
			// if closed & exit
			// 说明接收到了关闭信号，那么就关闭当前epoll
			if p.buf[0] > 0 {
				syscall.Close(p.wop.FD)
				syscall.Close(p.fd)
				operator.done()
				return true
			}
			operator.done()
			continue
		}

		// 发生了可读事件
		if triggerRead {
			// 如果FDOperator上的OnRead回调接口不为空，说明发生的是客户端的accept事件
			if operator.OnRead != nil {
				// for non-connection
				// 调用OnRead来接收并处理客户端连接
				operator.OnRead(p)
			} else if operator.Inputs != nil {
				// for connection
				// 每个poll对象会关联一个barriers结构，该结构用于实现分散读取与集中写入的系统调用
				// 每个poll对象还会关联一个LinkBuffer对象，作为读写数据缓冲区
				// 此处是从LinkBuffer中分配出一块空闲内存
				bs := operator.Inputs(p.barriers[i].bs)
				if len(bs) > 0 {
					// 读取数据到bs缓存区中
					n, err := ioread(operator.FD, bs, p.barriers[i].ivs)
					// 推动读指针，让写入缓冲区的数据对消费者可见，同时调用用户注册的OnRequest回调接口，处理读数据
					operator.InputAck(n)
					totalRead += n
					if err != nil {
						p.appendHup(operator)
						continue
					}
				}
			} else {
				logger.Printf("NETPOLL: operator has critical problem! event=%d operator=%v", evt, operator)
			}
		}
		if triggerHup {
			if triggerRead && operator.Inputs != nil {
				// read all left data if peer send and close
				var leftRead int
				// read all left data if peer send and close
				if leftRead, err = readall(operator, p.barriers[i]); err != nil && !errors.Is(err, ErrEOF) {
					logger.Printf("NETPOLL: readall(fd=%d)=%d before close: %s", operator.FD, total, err.Error())
				}
				totalRead += leftRead
			}
			// only close connection if no further read bytes
			if totalRead == 0 {
				p.appendHup(operator)
				continue
			}
		}
		if triggerError {
			// Under block-zerocopy, the kernel may give an error callback, which is not a real error, just an EAGAIN.
			// So here we need to check this error, if it is EAGAIN then do nothing, otherwise still mark as hup.
			if _, _, _, _, err := syscall.Recvmsg(operator.FD, nil, nil, syscall.MSG_ERRQUEUE); err != syscall.EAGAIN {
				p.appendHup(operator)
			} else {
				operator.done()
			}
			continue
		}
		if triggerWrite {
			if operator.OnWrite != nil {
				// for non-connection
				operator.OnWrite(p)
			} else if operator.Outputs != nil {
				// for connection
				bs, supportZeroCopy := operator.Outputs(p.barriers[i].bs)
				if len(bs) > 0 {
					// TODO: Let the upper layer pass in whether to use ZeroCopy.
					n, err := iosend(operator.FD, bs, p.barriers[i].ivs, false && supportZeroCopy)
					operator.OutputAck(n)
					if err != nil {
						p.appendHup(operator)
						continue
					}
				}
			} else {
				logger.Printf("NETPOLL: operator has critical problem! event=%d operator=%v", evt, operator)
			}
		}
		operator.done()
	}
	// hup conns together to avoid blocking the poll.
	p.onhups()
	return false
}

// Close will write 10000000
func (p *defaultPoll) Close() error {
	_, err := syscall.Write(p.wop.FD, []byte{1, 0, 0, 0, 0, 0, 0, 0})
	return err
}

// Trigger implements Poll.
func (p *defaultPoll) Trigger() error {
	if atomic.AddUint32(&p.trigger, 1) > 1 {
		return nil
	}
	// MAX(eventfd) = 0xfffffffffffffffe
	_, err := syscall.Write(p.wop.FD, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	return err
}

// Control implements Poll.
// 注册感兴趣的事件，netpoll在epoll_ctl基础之上又封装了一层
// 从Control函数中可以看出来，netpoll会在epollevent的data字段中保存监听的fd对象信息，这里fd对象是netpoll经过封装后的FDOperator对象。
func (p *defaultPoll) Control(operator *FDOperator, event PollEvent) error {
	// DON'T move `fd=operator.FD` behind inuse() call, we can only access operator before op.inuse() for avoid race
	// G1:              G2:
	// op.inuse()       op.unused()
	// op.FD  -- T1     op.FD = 0  -- T2
	// T1 and T2 may happen together
	fd := operator.FD
	var op int
	var evt epollevent
	// 将epollevent对象的data指针指向传入的FDOperator对象
	p.setOperator(unsafe.Pointer(&evt.data), operator)
	switch event {
	case PollReadable: // server accept a new connection and wait read
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollWritable: // client create a new connection and wait connect finished
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, EPOLLET|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollDetach: // deregister
		p.delOperator(operator)
		op, evt.events = syscall.EPOLL_CTL_DEL, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollR2RW: // connection wait read/write
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollRW2R: // connection wait read
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	}
	return EpollCtl(p.fd, op, fd, &evt)
}
