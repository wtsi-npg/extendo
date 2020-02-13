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
 * @file client_pool.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"sync"
	"time"

	logs "github.com/kjsanger/logshim"
	"github.com/pkg/errors"

	"github.com/kjsanger/extendo/v2/utilities"
)

// ClientPool is a pool of iRODS Clients for use by applications. It provides
// a way for an application to obtain a running Client without itself having to
// manage the number of connections or handle retries when a Client connection
// can't be obtained for some reason (e.g. the maximum number of connections is
// reached, a network error occurs, the iRODS server fails).
//
// Applications should use NewClientPool() to create a pool and then use
// the ClientPool's Get() and Return() methods to obtain and release Clients.
//
// Once a ClientPool has been created it may be closed. A closed pool will
// return an error on Get(), but will allow Return(). A closed pool may not
// be re-opened.
type ClientPool struct {
	clientArgs        []string      // baton-do arguments.
	getTimeout        time.Duration // Timeout for Get().
	getMaxRetries     uint8         // Max retries for Get().
	checkClientFreq   time.Duration // Frequency at which clients are checked.
	maxClientIdleTime time.Duration // Idle time after which clients will be stopped.
	maxClientRuntime  time.Duration // Runtime after which clients will be stopped.
	sync.RWMutex                    // Lock for IsOpen(), Get(), Return() and Close().
	isOpen            bool          // True if the pool is open.
	clients           []*Client     // Running clients in the pool.
	numClients        uint8         // The number of clients created by the pool.
	maxSize           uint8         // The maximum number of clients permitted.
}

var (
	errPoolClosed = errors.New("the client pool is closed")
	errPoolEmpty  = errors.New("the client pool is empty")
	errDeadClient = errors.New("dead client in the client pool")
	errGetTimeout = errors.New("timeout getting client from the pool")
)

// ClientPoolParams describes the available parameters for pool creation.
type ClientPoolParams struct {
	MaxSize           uint8         // Maximum number of clients.
	GetTimeout        time.Duration // Timeout for Get()
	GetMaxRetries     uint8         // Max retries for Get().
	CheckClientFreq   time.Duration // Frequency of check for old, idle or stopped clients.
	MaxClientRuntime  time.Duration // Runtime after which clients are considered old.
	MaxClientIdleTime time.Duration // Inactivity time after which clients are considered idle.
}

// The default argument values for client pool creation.
var DefaultClientPoolParams = ClientPoolParams{
	MaxSize:           10,
	GetTimeout:        time.Millisecond * 250,
	GetMaxRetries:     3,
	CheckClientFreq:   time.Second * 30,
	MaxClientRuntime:  time.Hour,
	MaxClientIdleTime: time.Minute * 10,
}

// NewClientPool creates a new pool that will hold up to params.MaxSize
// Clients. The Get() method will try to obtain a running Client on request for
// up to the specified params.construction before returning an error. The
// clientArgs arguments will be passed to the FindAndStart() method when
// creating each new Client.
func NewClientPool(params ClientPoolParams, clientArgs ...string) *ClientPool {

	processedArgs := []string{"--unbuffered", "--no-error"} // Always need this
	processedArgs = utilities.Uniq(append(processedArgs, clientArgs...))

	pool := ClientPool{
		clientArgs:        processedArgs,
		getTimeout:        params.GetTimeout,
		getMaxRetries:     params.GetMaxRetries,
		checkClientFreq:   params.CheckClientFreq,
		maxClientRuntime:  params.MaxClientRuntime,
		maxClientIdleTime: params.MaxClientIdleTime,
		isOpen:            true,
		maxSize:           params.MaxSize,
	}

	go pool.checkClients()

	return &pool
}

// IsOpen returns true if the pool is open.
func (pool *ClientPool) IsOpen() bool {
	pool.RLock()
	defer pool.RUnlock()

	return pool.isOpen
}

// Get returns a running Client from the pool, or creates a new one. It returns
// an error if the pool is closed, if the attempt to get a Client exceeds the
// pool's timeout, or if an error is encountered creating the Client.
func (pool *ClientPool) Get() (*Client, error) {
	return pool.getWithRetries()
}

// Tries up to getMaxRetries times to get a Client, each time with a timeout.
func (pool *ClientPool) getWithRetries() (*Client, error) {
	log := logs.GetLogger()

	for try := 0; try < int(pool.getMaxRetries); try++ {
		log.Debug().Int("try", try).Msg("getting a client")

		client, err := pool.getWithTimeout()
		if err != nil {
			log.Error().Err(err).Int("try", try).Msg("retrying")
			continue
		}
		if !client.IsRunning() {
			log.Error().Err(errDeadClient).Int("try", try).Msg("retrying")
			continue
		}

		return client, nil
	}

	return nil, errors.Errorf("failed to get a client from the pool "+
		"after %d tries", pool.getMaxRetries)
}

// Tries to get or create a Client, with a timeout.
func (pool *ClientPool) getWithTimeout() (*Client, error) {
	log := logs.GetLogger()

	timeout := time.NewTimer(pool.getTimeout)
	defer timeout.Stop()

	interval := time.Microsecond * 100

	for {
		select {
		case <-timeout.C:
			return nil, errGetTimeout
		default:
			pool.Lock()
			if !pool.isOpen {
				pool.Unlock()
				return nil, errPoolClosed
			}

			if !pool.isEmpty() {
				client, err := pool.pop()
				log.Debug().Msgf("got a client from the pool leaving %d",
					pool.size())

				pool.Unlock()
				return client, err
			}

			if pool.numClients < pool.maxSize {
				client, err := FindAndStart(pool.clientArgs...)
				if err == nil {
					pool.numClients++
					log.Debug().Msgf("added new client to the pool making %d",
						pool.numClients)
				}

				pool.Unlock()
				return client, err
			}
			pool.Unlock()

			time.Sleep(interval) // A client may become available later
		}
	}
}

// checkClients periodically, while to pool is open, examines all the unused
// clients in the pool to see whether any of them can be stopped and discarded.
// The reasons for discarding clients are: have been running for longer than
// the maxClientRuntime, have been idle longer than the maxClientIdleTime, or
// have stopped for another reason e.g. crashed or externally terminated.
//
// As the clients are unused and the pool is locked during this process, there
// is no danger of disconnecting an active client.
func (pool *ClientPool) checkClients() {
	checkTick := time.NewTicker(pool.checkClientFreq)
	defer checkTick.Stop()

	log := logs.GetLogger()

	for {
		select {
		case <-checkTick.C:
			pool.Lock()
			if !pool.isOpen {
				log.Debug().Msg("stopping client check")
				pool.Unlock()
				return
			}

			var keep []*Client
			numRemoved := uint8(0)
			for _, c := range pool.clients {
				rt := c.Runtime()

				if !c.IsRunning() {
					log.Debug().Dur("runtime", rt).
						Msg("removing one stopped client")
					numRemoved++
				} else if c.Runtime() > pool.maxClientRuntime {
					log.Debug().Int("pid", c.ClientPid()).
						Dur("runtime", rt).
						Dur("max_runtime", pool.maxClientRuntime).
						Msg("stopping long running client")
					stopAndLog(c, log)
					numRemoved++
				} else if c.IdleTime() > pool.maxClientIdleTime {
					log.Debug().Int("pid", c.ClientPid()).
						Dur("runtime", rt).
						Msg("stopping idle client")
					stopAndLog(c, log)
					numRemoved++
				} else {
					keep = append(keep, c)
				}
			}

			if uint8(len(keep)) != pool.size() {
				pool.clients = keep
				pool.numClients = pool.numClients - numRemoved
			}

			pool.Unlock()
		}
	}
}

// Return allows a client to be returned to the pool. If the client is running,
// it is returned to the pool. If the client has crashed or been stopped, this
// method will discard it and decrement the client count so that a new one can
// be created. If the pool has been closed, clients may still be returned,
// where they will be stopped and any errors from this ignored.
func (pool *ClientPool) Return(client *Client) error {
	log := logs.GetLogger()

	pool.Lock()
	defer pool.Unlock()

	if !pool.isOpen {
		log.Debug().Msg("discarding 1 client returned to a closed pool")
		client.StopIgnoreError()
		return nil
	}

	if !client.IsRunning() {
		log.Debug().Msg("discarding 1 client (stopped)")
		pool.numClients--
		return nil
	}

	if pool.size() < pool.maxSize {
		pool.push(client)
		log.Debug().Msgf("returned 1 client (running) to the pool making %d",
			pool.size())
		return nil
	}

	log.Debug().Msg("discarding 1 client (running), pool full")

	return client.Stop()
}

// Close closes the pool for further Get() operations. Clients may still be
// returned to a closed pool, see Return().
func (pool *ClientPool) Close() {
	pool.Lock()
	defer pool.Unlock()

	if !pool.isOpen {
		return
	}
	pool.isOpen = false

	log := logs.GetLogger()
	log.Debug().Msgf("stopping %d clients", pool.size())

	for pool.size() > 0 {
		c, err := pool.pop()
		if err != nil {
			log.Error().Err(err).Msg("internal error")
		}

		log.Debug().Int("pid", c.ClientPid()).Msg("stopping client")
		stopAndLog(c, log)
	}
}

// size returns the number of clients currently available in the pool.
func (pool *ClientPool) size() uint8 {
	return uint8(len(pool.clients))
}

func (pool *ClientPool) isEmpty() bool {
	return pool.size() == 0
}

// pop returns the top client in the pool.
func (pool *ClientPool) pop() (*Client, error) {
	if pool.isEmpty() {
		return nil, errPoolEmpty
	}

	n := pool.size()
	stack, top := pool.clients[:n-1], pool.clients[n-1]
	pool.clients = stack
	return top, nil
}

// push adds a client to the top of the client pool.
func (pool *ClientPool) push(client *Client) {
	pool.clients = append(pool.clients, client)
}

func stopAndLog(client *Client, log logs.Logger) {
	err := client.Stop()
	if err != nil {
		log.Error().Err(err).
			Int("pid", client.ClientPid()).
			Msg("client did not stop cleanly")
	}
}
