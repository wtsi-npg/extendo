/*
 * Copyright (C) 2019, 2020, 2021. Genome Research Ltd. All rights reserved.
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

	ex "github.com/wtsi-npg/extendo/v2"
)

var _ = Describe("Make a client pool", func() {
	var pool *ex.ClientPool

	When("a pool is created", func() {
		It("should be open", func() {
			pool = ex.NewClientPool(ex.DefaultClientPoolParams)
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
		params := ex.DefaultClientPoolParams
		params.MaxSize = poolSize
		params.GetTimeout = poolTimout
		pool = ex.NewClientPool(params)
	})

	AfterEach(func() {
		pool.Close()
	})

	When("a pool of size n is created", func() {
		It("Get should supply n running clients before timing out", func() {

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
		params := ex.DefaultClientPoolParams
		params.MaxSize = poolSize
		params.GetTimeout = poolTimout
		pool = ex.NewClientPool(params)

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
		It("should be possible to return running clients to it", func() {
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
		It("should be possible to return running clients to it", func() {
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

var _ = Describe("Pool client runtime timeout", func() {
	var poolSize = uint8(10)
	var poolTimout = time.Millisecond * 250
	var pool *ex.ClientPool
	var clients []*ex.Client

	AfterEach(func() {
		pool.Close()
	})

	When("a pool is open", func() {
		When("clients have run longer than MaxClientRuntime", func() {
			BeforeEach(func() {
				params := ex.DefaultClientPoolParams
				params.MaxSize = poolSize
				params.GetTimeout = poolTimout
				params.CheckClientFreq = time.Millisecond * 500
				params.MaxClientRuntime = time.Millisecond * 500
				params.MaxClientIdleTime = time.Minute
				pool = ex.NewClientPool(params)

				for i := 0; i < int(poolSize); i++ {
					c, err := pool.Get()
					Expect(err).NotTo(HaveOccurred())
					if err == nil {
						clients = append(clients, c)
					}
				}

				for _, c := range clients {
					err := pool.Return(c)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("should stop those clients returned to it", func() {
				time.Sleep(time.Second * 2)

				for _, c := range clients {
					Expect(c.IsRunning()).To(BeFalse())
				}
			})
		})

		When("clients have been idle longer than MaxClientIdleTime", func() {
			BeforeEach(func() {
				params := ex.DefaultClientPoolParams
				params.MaxSize = poolSize
				params.GetTimeout = poolTimout
				params.CheckClientFreq = time.Millisecond * 500
				params.MaxClientRuntime = time.Minute
				params.MaxClientIdleTime = time.Millisecond * 500
				pool = ex.NewClientPool(params)

				for i := 0; i < int(poolSize); i++ {
					c, err := pool.Get()
					Expect(err).NotTo(HaveOccurred())
					if err == nil {
						clients = append(clients, c)
					}
				}

				for _, c := range clients {
					err := pool.Return(c)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("should stop those clients returned to it", func() {
				time.Sleep(time.Second * 2)

				for _, c := range clients {
					Expect(c.IsRunning()).To(BeFalse())
				}
			})
		})
	})
})
