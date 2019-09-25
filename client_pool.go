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
 * @file client_pool.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"time"

	logs "github.com/kjsanger/logshim"
	"github.com/pkg/errors"

	"github.com/kjsanger/extendo/utilities"
)

type token struct{}
type semaphore chan token

// ClientPool is a pool of iRODS Clients for use by applications. It provides
// a way for an application to obtain a running Client without itself having to
// manage the number of connections or handle retries when a Client connection
// can't be obtained for some reason (e.g. the maximum number of connections is
// reached, a network error occurs, the iRODS server fails).
//
// Applications should use the NewClientPool to create a pool and then use
// the ClientPool's Get() and Return() methods to obtain and release Clients.
type ClientPool struct {
	isOpen     bool          // True if the pool is open
	clientArgs []string      // baton-do arguments
	clients    chan *Client  // Running clients in the pool
	slots      semaphore     // Slots available to create new clients
	timeout    time.Duration // Timeout for Get calls
	maxRetries uint8         // Max retries for Get calls
}

var (
	errPoolClosed = errors.New("the client pool is closed")
	errDeadClient = errors.New("dead client in the client pool")
	errGetTimeout = errors.New("timeout getting client from the pool")
)

// NewClientPool creates a new pool that will hold up to maxSize Clients. The
// Get() method will try to obtain a running Client on request for up to the
// specified timeout before returning an error. The clientArgs arguments will
// be passed to the FindAndStart() method when creating each new Client.
func NewClientPool(maxSize uint8, timeout time.Duration,
	clientArgs ...string) *ClientPool {

	processedArgs := []string{"--unbuffered"} // Always need this
	processedArgs = utilities.Uniq(append(processedArgs, clientArgs...))

	pool := ClientPool{
		isOpen:     true,
		clientArgs: processedArgs,
		timeout:    timeout,
		maxRetries: uint8(3),
		slots:      make(semaphore, maxSize),
		clients:    make(chan *Client, maxSize),
	}

	return &pool
}

// IsOpen returns true if the pool is open.
func (pool *ClientPool) IsOpen() bool {
	return pool.isOpen
}

// Get returns a running Client from the pool, or creates a new one. It returns
// an error if the pool is closed, if the attempt to get a Client exceeds the
// pool's timeout, or if an error is encountered creating the Client.
func (pool *ClientPool) Get() (*Client, error) {
	if !pool.IsOpen() {
		return nil, errPoolClosed
	}

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

	select {
	case client := <-pool.clients:
		log.Debug().Msgf("got a client from the pool leaving %d",
			len(pool.clients))
		return client, nil

	case <-timeout.C:
		timeout.Reset(pool.timeout)

		log.Debug().Msg("timed out getting a client from the pool")

		select {
		case client := <-pool.clients:
			log.Debug().Msg("got client from the pool while waiting " +
				"to add a new one")
			return client, nil

		case pool.slots <- token{}:
			log.Debug().Msgf("added new client to the pool making %d",
				len(pool.clients))
			client, err := FindAndStart(pool.clientArgs...)
			if err != nil {
				<-pool.slots // Free the slot on error
			}
			return client, err

		case <-timeout.C:
			return nil, errGetTimeout
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
	if !pool.IsOpen() {
		log.Debug().Msg("returned 1 client to a closed pool")
		client.StopIgnoreError()
		return nil
	}

	if !client.IsRunning() {
		log.Debug().Msg("discarded 1 client (stopped)")
		<-pool.slots
		return nil
	}

	select {
	case pool.clients <- client:
		log.Debug().Msgf("returned 1 client (running) to the pool making %d",
			len(pool.clients))
		return nil

	default:
		log.Debug().Msg("discarded 1 client (running), pool full")
		<-pool.slots
		return client.Stop()
	}
}

// Close closes the pool for further Get() operations. Clients may still be
// returned to a closed pool, see Return.
func (pool *ClientPool) Close() {
	if !pool.IsOpen() {
		return
	}

	pool.isOpen = false

	log := logs.GetLogger()
	log.Debug().Msg("closing client channel")
	close(pool.clients)

	for c := range pool.clients {
		log.Debug().Msg("stopping client")
		err := c.Stop()
		if err != nil {
			log.Error().Err(err).Msg("client did not stop cleanly")
		}
	}
}
