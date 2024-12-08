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

//go:build !windows
// +build !windows

package netpoll

import (
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
)

const (
	managerUninitialized = iota
	managerInitializing
	managerInitialized
)

func newManager(numLoops int) *manager {
	m := new(manager)
	m.SetLoadBalance(RoundRobin)
	m.SetNumLoops(numLoops)
	return m
}

// LoadBalance is used to do load balancing among multiple pollers.
// a single poller may not be optimal if the number of cores is large (40C+).
// manager 用于管理多个 poller, 将fd平均分配到多个 poller 上,来负载均衡多个epoll上的fd数量,避免单个epoll上的fd数量过多
// 需要理解的是netpoll并不关心fd的依赖关系,只是负责将fd平均分配到多个poller上，避免单个poller也就是epoll上的fd数量过多
type manager struct {
	numLoops int32
	status   int32       // 0: uninitialized, 1: initializing, 2: initialized
	balance  loadbalance // load balancing method
	polls    []Poll      // all the polls
}

// SetNumLoops will return error when set numLoops < 1
// 初始化多路复用池 / 运行时调用该函数实现动态扩缩容
func (m *manager) SetNumLoops(numLoops int) (err error) {
	if numLoops < 1 {
		return fmt.Errorf("set invalid numLoops[%d]", numLoops)
	}
	// note: set new numLoops first and then change the status
	atomic.StoreInt32(&m.numLoops, int32(numLoops))
	atomic.StoreInt32(&m.status, managerUninitialized)
	return nil
}

// SetLoadBalance set load balance.
func (m *manager) SetLoadBalance(lb LoadBalance) error {
	if m.balance != nil && m.balance.LoadBalance() == lb {
		return nil
	}
	m.balance = newLoadbalance(lb, m.polls)
	return nil
}

// Close release all resources.
func (m *manager) Close() (err error) {
	for _, poll := range m.polls {
		err = poll.Close()
	}
	m.numLoops = 0
	m.balance = nil
	m.polls = nil
	return err
}

// Run all pollers.
func (m *manager) Run() (err error) {
	defer func() {
		if err != nil {
			_ = m.Close()
		}
	}()

	numLoops := int(atomic.LoadInt32(&m.numLoops))
	log.Println("NETPOLL: run with numLoops:", numLoops)
	if numLoops == len(m.polls) {
		return nil
	}
	polls := make([]Poll, numLoops)
	if numLoops < len(m.polls) {
		// shrink polls
		// 对于无需缩减的部分，直接重新指向即可
		copy(polls, m.polls[:numLoops])
		for idx := numLoops; idx < len(m.polls); idx++ {
			// close redundant polls
			// 对于需要缩减的部分，直接Close关闭该多路复用器
			if err = m.polls[idx].Close(); err != nil {
				logger.Printf("NETPOLL: poller close failed: %v\n", err)
			}
		}
	} else {
		// growth polls
		copy(polls, m.polls)
		for idx := len(m.polls); idx < numLoops; idx++ {
			var poll Poll
			// 创建新的 poll 到资源池中
			poll, err = openPoll()
			if err != nil {
				return err
			}
			polls[idx] = poll
			// 每个多路复用器绑定一个协程，不断轮询注册到该epoll上的fd事件
			go poll.Wait()
		}
	}
	m.polls = polls

	// LoadBalance must be set before calling Run, otherwise it will panic.
	m.balance.Rebalance(m.polls)
	return nil
}

// Reset pollers, this operation is very dangerous, please make sure to do this when calling !
func (m *manager) Reset() error {
	for _, poll := range m.polls {
		poll.Close()
	}
	m.polls = nil
	return m.Run()
}

// Pick will select the poller for use each time based on the LoadBalance.
func (m *manager) Pick() Poll {
START:
	// fast path
	if atomic.LoadInt32(&m.status) == managerInitialized {
		return m.balance.Pick()
	}
	// slow path
	// try to get initializing lock failed, wait others finished the init work, and try again
	if !atomic.CompareAndSwapInt32(&m.status, managerUninitialized, managerInitializing) {
		runtime.Gosched()
		goto START
	}
	// adjust polls
	// m.Run() will finish very quickly, so will not many goroutines block on Pick.
	_ = m.Run()

	if !atomic.CompareAndSwapInt32(&m.status, managerInitializing, managerInitialized) {
		// SetNumLoops called during m.Run() which cause CAS failed
		// The polls will be adjusted next Pick
	}
	return m.balance.Pick()
}
