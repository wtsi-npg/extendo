/*
 * Copyright (C) 2019, 2020. Genome Research Ltd. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License,
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @file client_pool_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo_test

import (
	_ "net/http/pprof"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	ex "github.com/kjsanger/extendo"
)


var _ = Describe("Make a client pool", func() {
	var pool *ex.ClientPool

	When("a pool is created", func() {
		It("should be open", func() {
			pool = ex.NewClientPool(10, time.Second)
			Expect(pool.IsOpen()).To(BeTrue())
		})

		It("should be closeable", func() {
			pool.Close()
			Expect(pool.IsOpen()).To(BeFalse())
		})
	})
})

var _ = Describe("Get clients from the pool", func() {
	var poolSize = uint8(10)
	var poolTimout = time.Millisecond * 250
	var pool *ex.ClientPool
	var clients []*ex.Client

	BeforeEach(func() {
		pool = ex.NewClientPool(poolSize, poolTimout)
	})

	AfterEach(func() {
		pool.Close()
	})

	When("a pool of size n is created", func() {
		It("Get should supply n isRunning clients before timing out", func() {

		loop:
			for timeout := time.After(time.Second * 10); ; {
				select {
				case <-timeout:
					break loop // Fallback test timeout if something goes wrong

				default:
					c, err := pool.Get()
					if err == nil {
						clients = append(clients, c)
					} else {
						Expect(err).To(MatchError(MatchRegexp(`\d tries`)))
						break loop // Test timeout if pool times out (as expected)
					}
				}
			}

			Expect(len(clients)).To(Equal(int(poolSize)))
			for _, c := range clients {
				Expect(c.IsRunning()).To(BeTrue())
			}
		})
	})

	When("a pool is closed", func() {
		BeforeEach(func() {
			pool.Close()
		})

		It("should not be possible to get clients from it", func() {
			c, err := pool.Get()
			Expect(c).To(BeNil())
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("Return clients to the pool", func() {
	var poolSize = uint8(10)
	var poolTimout = time.Millisecond * 250
	var pool *ex.ClientPool
	var clients []*ex.Client

	BeforeEach(func() {
		pool = ex.NewClientPool(poolSize, poolTimout)

		var newClients []*ex.Client
		for i := 0; i < int(poolSize); i++ {
			c, err := pool.Get()
			Expect(err).NotTo(HaveOccurred())
			if err == nil {
				newClients = append(newClients, c)
			}
		}

		clients = newClients
	})

	AfterEach(func() {
		pool.Close()
	})

	When("a pool is open", func() {
		It("should be possible to return isRunning clients to it", func() {
			for _, c := range clients {
				Expect(c.IsRunning()).To(BeTrue())

				err := pool.Return(c)
				Expect(err).NotTo(HaveOccurred())

				Expect(c.IsRunning()).To(BeTrue())
			}
		})

		It("should be possible to return stopped clients to it", func() {
			for _, c := range clients {
				c.StopIgnoreError()
				err := pool.Return(c)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	When("a pool is closed", func() {
		It("should be possible to return isRunning clients to it", func() {
			pool.Close()
			for _, c := range clients {
				Expect(c.IsRunning()).To(BeTrue())

				err := pool.Return(c)
				Expect(err).NotTo(HaveOccurred())

				Expect(c.IsRunning()).To(BeFalse())
			}
		})

		It("should be possible to return stopped clients to it", func() {
			pool.Close()
			for _, c := range clients {
				c.StopIgnoreError()
				err := pool.Return(c)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
})
