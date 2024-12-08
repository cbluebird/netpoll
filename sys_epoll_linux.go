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

//go:build !arm64 && !loong64
// +build !arm64,!loong64

package netpoll

import (
	"log"
	"syscall"
	"unsafe"
)

const EPOLLET = -syscall.EPOLLET

//epoll的事件触发细节看这篇https://blog.csdn.net/Long_xu/article/details/132527959
// https://www.cnblogs.com/theseventhson/p/15829130.html

type epollevent struct {
	events uint32  // events：表示要监听的事件类型，如可读、可写等。这是一个位掩码，可以设置多个事件类型，例如 EPOLLIN 表示可读事件，EPOLLOUT 表示可写事件。
	data   [8]byte // 可以携带用户数据。这里的用户数据通常是对应的fd（如 ev.data.fd），以便于识别对应的文件描述符。
}

// EpollCreate implements epoll_create1.
// 内核中间加一个 ep 对象，把所有需要监听的 socket 都放到 ep 对象中
func EpollCreate(flag int) (fd int, err error) {
	log.Println("EpollCreate")
	var r0 uintptr
	// 执行epoll_create系统调用
	r0, _, err = syscall.RawSyscall(syscall.SYS_EPOLL_CREATE1, uintptr(flag), 0, 0)
	if err == syscall.Errno(0) {
		err = nil
	}
	return int(r0), err
}

// EpollCtl implements epoll_ctl.
// epoll_ctl 负责在 epollfd 对象上,把 socket fd 增加、删除(ADD/DEL/...)到内核红黑树,并设置感兴趣的事件(EPOLLIN/EPOLLOUT/...)
func EpollCtl(epfd, op, fd int, event *epollevent) (err error) {
	_, _, err = syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(epfd), uintptr(op), uintptr(fd), uintptr(unsafe.Pointer(event)), 0, 0)
	if err == syscall.Errno(0) {
		err = nil
	}
	return err
}

// EpollWait implements epoll_wait.
// epoll_wait: 阻塞等待直到 epollfd 内有就绪事件便返回，返回值为有效事件数，并且有效事件会记录再传⼊的 events 地址中
func EpollWait(epfd int, events []epollevent, msec int) (n int, err error) {
	var r0 uintptr
	_p0 := unsafe.Pointer(&events[0])
	if msec == 0 {
		r0, _, err = syscall.RawSyscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(_p0), uintptr(len(events)), 0, 0, 0)
	} else {
		r0, _, err = syscall.Syscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(_p0), uintptr(len(events)), uintptr(msec), 0, 0)
	}
	if err == syscall.Errno(0) {
		err = nil
	}
	return int(r0), err
}
