/*
 * Copyright (C) 2019. Genome Research Ltd. All rights reserved.
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
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	ex "github.com/kjsanger/extendo"
)

var _ = Describe("Make a client pool", func() {
	var pool *ex.ClientPool
	var poolErr error

	When("a pool is created", func() {
		It("should be open", func() {
			pool, poolErr = ex.NewClientPool(10, time.Second)
			Expect(poolErr).NotTo(HaveOccurred())
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
	var poolErr error
	var clients []*ex.Client

	BeforeEach(func() {
		poolSize = 10
		pool, poolErr = ex.NewClientPool(poolSize, poolTimout)
		Expect(poolErr).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		pool.Close()
	})

	When("a pool of size n is created", func() {
		It("should supply n running clients before Get times out", func() {

		loop:
			for timeout := time.After(time.Second * 10) ;; {
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
})

var _ = Describe("Return clients to the pool", func() {
	var poolSize = uint8(10)
	var poolTimout = time.Millisecond * 250
	var pool *ex.ClientPool
	var poolErr error
	var clients []*ex.Client

	BeforeEach(func() {
		pool, poolErr = ex.NewClientPool(poolSize, poolTimout)
		Expect(poolErr).NotTo(HaveOccurred())

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
		It("should be possible to return the running clients", func() {
			for _, c := range clients {
				Expect(c.IsRunning()).To(BeTrue())

				err := pool.Return(c)
				Expect(err).NotTo(HaveOccurred())

				Expect(c.IsRunning()).To(BeTrue())
			}
		})
	})

	When("a pool is closed", func() {
		It("should be possible to return the running clients", func() {
			pool.Close()
			for _, c := range clients {
				Expect(c.IsRunning()).To(BeTrue())

				err := pool.Return(c)
				Expect(err).NotTo(HaveOccurred())

				Expect(c.IsRunning()).To(BeFalse())
			}
		})
	})
})

