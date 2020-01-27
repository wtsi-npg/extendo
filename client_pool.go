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

	"github.com/kjsanger/extendo/utilities"
)

// ClientPool is a pool of iRODS Clients for use by applications. It provides
// a way for an application to obtain a isRunning Client without itself having to
// manage the number of connections or handle retries when a Client connection
// can't be obtained for some reason (e.g. the maximum number of connections is
// reached, a network error occurs, the iRODS server fails).
//
// Applications should use the NewClientPool to create a pool and then use
// the ClientPool's Get() and Return() methods to obtain and release Clients.
//
// Once a ClientPool has been created it may be closed. A closed pool will
// return an error on Get(), but will allow Return(). A closed pool may not
// be re-opened.
type ClientPool struct {
	clientArgs []string      // baton-do arguments
	timeout    time.Duration // Timeout for Get() and Return()
	maxRetries uint8         // Max retries for Get()
	sync.Mutex               // Lock for IsOpen(), Get(), Return() and Close()
	isOpen     bool          // True if the pool is open
	clients    []*Client     // Running clients in the pool
	numClients uint8
	maxClients uint8
}

var (
	errPoolClosed = errors.New("the client pool is closed")
	errPoolEmpty  = errors.New("the client pool is empty")
	errDeadClient = errors.New("dead client in the client pool")
	errGetTimeout = errors.New("timeout getting client from the pool")
)

// NewClientPool creates a new pool that will hold up to maxSize Clients. The
// Get() method will try to obtain a running Client on request for up to the
// specified timeout before returning an error. The clientArgs arguments will
// be passed to the FindAndStart() method when creating each new Client.
func NewClientPool(maxSize uint8, timeout time.Duration,
	clientArgs ...string) *ClientPool {

	processedArgs := []string{"--unbuffered", "--no-error"} // Always need this
	processedArgs = utilities.Uniq(append(processedArgs, clientArgs...))

	pool := ClientPool{
		clientArgs: processedArgs,
		timeout:    timeout,
		maxRetries: uint8(3),
		isOpen:     true,
		maxClients: maxSize,
	}

	return &pool
}

// IsOpen returns true if the pool is open.
func (pool *ClientPool) IsOpen() bool {
	pool.Lock()
	defer pool.Unlock()

	return pool.isOpen
}

// Get returns a isRunning Client from the pool, or creates a new one. It returns
// an error if the pool is closed, if the attempt to get a Client exceeds the
// pool's timeout, or if an error is encountered creating the Client.
func (pool *ClientPool) Get() (*Client, error) {
	return pool.getWithRetries()
}

// Tries up to maxRetries times to get a Client, each time with a timeout.
func (pool *ClientPool) getWithRetries() (*Client, error) {
	log := logs.GetLogger()

	for try := 0; try < int(pool.maxRetries); try++ {
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
		"after %d tries", pool.maxRetries)
}

// Tries to get ot create a Client, with a timeout.
func (pool *ClientPool) getWithTimeout() (*Client, error) {
	log := logs.GetLogger()

	timeout := time.NewTimer(pool.timeout)
	defer timeout.Stop()

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

			if pool.numClients < pool.maxClients {
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

			interval := time.Microsecond * 100
			log.Debug().Msgf("sleeping ... %s", interval)
			time.Sleep(interval)
		}
	}
}

// Return allows a client to be returned to the pool. If the client is isRunning,
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

	if uint8(pool.size()) < pool.maxClients {
		pool.push(client)
		log.Debug().Msgf("returned 1 client (isRunning) to the pool making %d",
			pool.size())
		return nil
	}

	log.Debug().Msg("discarding 1 client (isRunning), pool full")

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
		err = c.Stop()
		if err != nil {
			log.Error().Err(err).
				Int("pid", c.ClientPid()).
				Msg("client did not stop cleanly")
		}
	}
}

func (pool *ClientPool) size() int {
	return len(pool.clients)
}

func (pool *ClientPool) isEmpty() bool {
	return pool.size() == 0
}

func (pool *ClientPool) pop() (*Client, error) {
	if pool.isEmpty() {
		return nil, errPoolEmpty
	}

	n := pool.size()
	stack, top := pool.clients[:n-1], pool.clients[n-1]
	pool.clients = stack
	return top, nil
}

func (pool *ClientPool) push(client *Client) {
	pool.clients = append(pool.clients, client)
}
