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
 * @file client.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	logs "github.com/kjsanger/logshim"
	"github.com/pkg/errors"
)

const (
	CHMOD     = "chmod"     // chmod baton operation
	CHECKSUM  = "checksum"  // checksum baton operation
	GET       = "get"       // get baton operation
	LIST      = "list"      // list baton operation
	METAMOD   = "metamod"   // metamod baton operation
	METAADD   = "add"       // metamod add baton operation
	METAREM   = "rem"       // metamod rem baton operation
	METAQUERY = "metaquery" // metaquery baton operation
	MKDIR     = "mkdir"     // mkdir baton operation
	PUT       = "put"       // put baton operation
	REMOVE    = "remove"    // rm baton operation
	RMDIR     = "rmdir"     // rmdir baton operation
)

const (
	RodsUserFileDoesNotExist  = int32(-310000) // iRODS: user file does not exist
	RodsCatCollectionNotEmpty = int32(-821000) // iRODS: collection not empty
)

// Client is a launcher for a baton sub-process which holds its system I/O
// streams and its channels.
type Client struct {
	path        string             // Path of the baton executable.
	cmd         *exec.Cmd          // Cmd of the sub-process, once started.
	stdin       io.WriteCloser     // stdin of the sub-process, once started.
	stdout      io.ReadCloser      // stdout of the sub-process, once started.
	stderr      io.ReadCloser      // stderr of the sub-process, once started.
	in          chan []byte        // For sending to the sub-process.
	out         chan []byte        // For receiving from the sub-process.
	cancel      context.CancelFunc // For stopping the I/O goroutines.
	ioWaitGroup *sync.WaitGroup    // WaitGroup for I/O goroutines.
}

// Envelope is the JSON document accepted by baton-do, describing an operation
// to perform on a target. It is also the document returned by baton-do
// afterwards, describing the outcome of the operation, including return value
// and errors.
type Envelope struct {
	// Operation for baton-do.
	Operation string `json:"operation"`
	// Arguments for operation.
	Arguments Args `json:"arguments"`
	// Target of the operation.
	Target RodsItem `json:"target"`
	// Result of the operation.
	Result *ResultWrapper `json:"result,omitempty"`
	// ErrorMsg from the operation.
	ErrorMsg *ErrorMsg `json:"error,omitempty"`
}

// Args contains the arguments for the various baton-do operation parameters.
type Args struct {
	// Request an operation.
	Operation string `json:"operation,omitempty"`
	// Request ACLs.
	ACL bool `json:"acl,omitempty"`
	// Request metadata AVUs.
	AVU bool `json:"avu,omitempty"`
	// Request checksums.
	Checksum bool `json:"checksum,omitempty"`
	// Restrict to collections.
	Collection bool `json:"collection,omitempty"`
	// Request collection contents.
	Contents bool `json:"contents,omitempty"`
	// Force an operation.
	Force bool `json:"force,omitempty"`
	// Restrict to data objects.
	Object bool `json:"object,omitempty"`
	// Request a recursive operation.
	Recurse bool `json:"recurse,omitempty"`
	// Request replicate information.
	Replicate bool `json:"replicate,omitempty"`
	// Request data object size.
	Size bool `json:"size,omitempty"`
	// Request timestamps.
	Timestamp bool `json:"timestamp,omitempty"`
}

type ResultWrapper struct {
	Item *RodsItem   `json:"single,omitempty"`
	List *[]RodsItem `json:"multiple,omitempty"`
}

type ErrorMsg struct {
	Message string `json:"message"`
	Code    int32  `json:"code"`
}

// RodsError is an error raised on the iRODS server and reported to the client.
type RodsError struct {
	err  error
	code int32
}

// Error implements the error interface for RodsErrors.
func (e *RodsError) Error() string {
	return fmt.Sprintf("%s code: %d", e.err, e.code)
}

// Code returns the iRODS error code for an error.
func (e *RodsError) Code() int32 {
	return e.code
}

// IsRodsError returns true if the Cause of the error is a RodsError.
func IsRodsError(err error) bool {
	switch errors.Cause(err).(type) {
	case *RodsError:
		return true
	default:
		return false
	}
}

// RodsErrorCode returns the iRODS error code of the Cause error. If the cause
// is not a RodsError, an error is returned.
func RodsErrorCode(err error) (int32, error) {
	switch e := errors.Cause(err).(type) {
	case *RodsError:
		return e.Code(), nil
	default:
		return int32(0),
			errors.Errorf("cannot get an iRODS error code from %v", err)
	}
}

// FindBaton returns the cleaned path to the first occurrence of the baton-do
// executable in the environment's PATH. If the executable is not found, an
// error is raised.
func FindBaton() (string, error) {
	var baton string
	var err error

	envPath := os.Getenv("PATH")
	dirs := strings.Split(envPath, ":")

	for _, dir := range dirs {
		paths, err := filepath.Glob(filepath.Join(dir, "baton-do"))
		if err != nil {
			break
		}

		for _, path := range paths {
			if filepath.Base(path) == "baton-do" {
				baton = path
				break
			}
		}
	}
	if baton == "" {
		return baton, errors.Errorf("baton-do not present in PATH '%s'",
			envPath)
	}

	return filepath.Clean(baton), err
}

// FindAndStart locates the baton-do executable using FindBaton, creates a
// Client using NewClient and finally calls Start on the newly created Client,
// passing the argument strings of this function to the Start method. The
// running Client is returned.
func FindAndStart(arg ...string) (*Client, error) {
	baton, err := FindBaton()
	if err != nil {
		return nil, err
	}

	client, err := NewClient(baton)
	if err != nil {
		return nil, err
	}

	return client.Start(arg...)
}

// NewClient returns a new instance with the executable path set. The path
// argument is passed to exec.LookPath.
func NewClient(path string) (*Client, error) {
	executable, err := exec.LookPath(path)
	if err != nil {
		return nil, err
	}

	return &Client{path: executable}, err
}

// Start runs the client's external baton program, creating new channels for
// communication with it. The arguments to Start are passed as command line
// arguments to the baton program. If the program is already running and Start
// is called, an error is raised.
func (client *Client) Start(arg ...string) (*Client, error) {
	log := logs.GetLogger()

	if client.IsRunning() {
		return client, errors.New("client is already running")
	}

	cmd := exec.Command(client.path, arg...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	if err = cmd.Start(); err != nil {
		return nil, err
	}

	// Sub-process input and response synchronisation
	in := make(chan []byte)
	out := make(chan []byte)

	// I/O goroutine cancelling and cleanup
	cancelCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(3) // The stdin, stdout and stderr goroutines

	// Send messages to the baton sub-process
	go func(ctx context.Context) {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				// Close stdin to unblock the reader
				if ce := stdin.Close(); ce != nil {
					log.Error().Err(ce).Str("executable", client.path).
						Msg("failed to close stdin")
				}
				return
			case buf := <-in:
				n, werr := stdin.Write(append(buf, '\n'))

				if werr != nil {
					log.Error().Err(werr).
						Str("executable", client.path).
						Str("value", string(buf)).
						Int("num_written", n).
						Msg("error writing to stdin")
				}
			}
		}
	}(cancelCtx)

	// Receive messages from the baton sub-process
	go func(ctx context.Context) {
		defer wg.Done()

		rd := bufio.NewReader(stdout)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// On cancelling, this is unblocked by the send goroutine
				// closing stdin
				bout, re := rd.ReadBytes('\n')
				if re == nil {
					out <- bytes.TrimRight(bout, "\r\n")
				} else if re == io.EOF {
					log.Debug().Str("executable", client.path).
						Msg("reached EOF on stdout")
					return
				} else {
					log.Error().Err(re).Str("executable", client.path).
						Msg("read error on stdout")
					return
				}
			}
		}
	}(cancelCtx)

	go func(ctx context.Context) {
		defer wg.Done()

		rd := bufio.NewReader(stderr)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				bout, re := rd.ReadBytes('\n')
				if re == nil {
					out := bytes.TrimRight(bout, "\r\n")
					log.Debug().Str("stderr", client.path).
						Msg(string(out))
				} else if re == io.EOF {
					log.Debug().Str("executable", client.path).
						Msg("reached EOF on stderr")
					return
				} else {
					log.Error().Err(re).Str("executable", client.path).
						Msg("read error on stderr")
					return
				}
			}
		}
	}(cancelCtx)

	client.cmd = cmd
	client.stdin = stdin
	client.stdout = stdout
	client.stderr = stderr
	client.in = in
	client.out = out
	client.cancel = cancel
	client.ioWaitGroup = &wg

	return client, err
}

// Stop stops the baton sub-process, if it is running. Returns any error
// from the sub-process.
func (client *Client) Stop() error {
	if !client.IsRunning() {
		return nil
	}

	client.cancel()
	client.ioWaitGroup.Wait()
	return client.cmd.Wait()
}

// Stop stops the baton sub-process, if it is running. Ignores any error
// from the sub-process.
func (client *Client) StopIgnoreError() {
	if err := client.Stop(); err != nil {
		logs.GetLogger().Error().Err(err).Msg("stopped client")
	}
}

// ClientPid returns the process ID of the baton sub-process if it has started,
// or -1 otherwise.
func (client *Client) ClientPid() int {
	if client.IsRunning() {
		return client.cmd.Process.Pid
	}
	return -1
}

// IsRunning returns true if the baton sub-process is running, or false
// otherwise.
func (client *Client) IsRunning() bool {
	return client.cmd != nil && client.cmd.Process != nil &&
		!(client.cmd.ProcessState != nil && client.cmd.ProcessState.Exited())
}

// Chmod sets permissions on a collection or data object in iRODS. By setting
// Args.Recurse=true, the operation may be made recursive.
func (client *Client) Chmod(args Args, item RodsItem) (RodsItem, error) {
	items, err := client.execute(CHMOD, args, item)
	if err != nil {
		return item, err
	}
	return items[0], err
}

// Checksum calculates a checksum for a data object in iRODS. iRODS makes this
// a no-op if a checksum is already recorded. However, this can be overridden
// by setting Force=true in Args. When called, this sets or updates the checksum
// on all replicates. If Args.Checksum=true is set, the new checksum will
// be reported in the return value.
func (client *Client) Checksum(args Args, item RodsItem) (RodsItem, error) {
	items, err := client.execute(CHECKSUM, args, item)
	if err != nil {
		return item, err
	}

	return items[0], err
}

func (client *Client) Get(args Args, item RodsItem) (RodsItem, error) {
	items, err := client.execute(GET, args, item)
	if err != nil {
		return item, err
	}
	return items[0], err
}

// List retrieves information about collections and/or data objects in iRODS.
// The items returned are sorted as a RodsItemArr (collections first, then by
// path and finally by name). The detailed composition of the items is
// influenced by the supplied Args:
//
// Args.ACL = true        Include ACLs
// Args.AVU = true        Include AVUs
// Args.Contents = true   Include collection direct contents
// Args.Recurse = true    Recurse into collections
// Args.Replicates = true Include replicates for data objects
// Args.Size = true       Include size for data objects
// Args.Timestamp = true  Include timestamps for data objects
//
func (client *Client) List(args Args, item RodsItem) ([]RodsItem, error) {
	if args.Recurse {
		return client.listRecurse(args, item)
	}

	return client.execute(LIST, args, item)
}

// ListItem retrieves information about an individual collection or data
// object in iRODS. The effects of Args are the same as for List, except that
// Recurse is not permitted. If the listed item does not exist, an error is
// returned. If the operation would return more than one collection or data
// object, an error is returned.
func (client *Client) ListItem(args Args, item RodsItem) (RodsItem, error) {
	if args.Recurse {
		return item, errors.New("invalid argument: Recurse=true")
	}

	items, err := client.execute(LIST, args, item)
	if err != nil {
		return item, err
	}

	switch len(items) {
	case 0:
		return item, errors.Errorf("no such item: %+v", item)
	case 1:
		return items[0], err
	default:
		return item, errors.Errorf("attempt to ListItem multiple "+
			"items: %+v", items)
	}
}

func (client *Client) ListChecksum(item RodsItem) (string, error) {
	var checksum string

	if !item.IsDataObject() {
		return checksum, errors.Errorf("invalid argument: can only get "+
			"the checksum of a file or data object, but was passed %+v", item)
	}

	obj, err := client.ListItem(Args{Checksum: true}, item)
	if err != nil {
		return checksum, err
	}
	checksum = obj.IChecksum

	return checksum, err
}

func (client *Client) metaMod(args Args, item RodsItem) (RodsItem, error) {
	items, err := client.execute(METAMOD, args, item)
	if err != nil {
		return item, err
	}
	return items[0], err
}

// MetaAdd adds the AVUs of the item to a collection or data object in iRODS
// and returns the item.
func (client *Client) MetaAdd(args Args, item RodsItem) (RodsItem, error) {
	args.Operation = METAADD
	return client.metaMod(args, item)
}

// MetaRem removes the AVUs of the item from a collection or data object in
// iRODS and returns the item.
func (client *Client) MetaRem(args Args, item RodsItem) (RodsItem, error) {
	args.Operation = METAREM
	return client.metaMod(args, item)
}

func (client *Client) MetaQuery(args Args, item RodsItem) ([]RodsItem, error) {
	if !(args.Object || args.Collection) {
		return nil, errors.Errorf("metaquery arguments must specify " +
			"Object and/or Collection targets; neither were specified")
	}

	return client.execute(METAQUERY, args, item)
}

// MkDir creates a new collection in iRODS and returns the item.
func (client *Client) MkDir(args Args, item RodsItem) (RodsItem, error) {
	items, err := client.execute(MKDIR, args, item)
	if err != nil {
		return item, err
	}
	return items[0], err
}

// Puts a collection or data object into iRODS and returns the item. By
// setting Args.Recurse=true, the operation may be made recursive on a
// collection.
func (client *Client) Put(args Args, item RodsItem) ([]RodsItem, error) {
	if args.Recurse {
		return client.putRecurse(args, item)
	}

	return client.execute(PUT, args, item)
}

// RemObj removes a data object from iRODS and returns the item.
func (client *Client) RemObj(args Args, item RodsItem) ([]RodsItem, error) {
	return client.execute(REMOVE, args, item)
}

// RemDir removes a collection from iRODS and returns the item.
func (client *Client) RemDir(args Args, item RodsItem) ([]RodsItem, error) {
	return client.execute(RMDIR, args, item)
}

func (client *Client) listRecurse(args Args, item RodsItem) ([]RodsItem, error) {
	var items RodsItemArr

	if item.IsDataObject() {
		return []RodsItem{item}, nil
	}

	items = append(items, item)

	args.Contents = true
	populated, err := client.execute(LIST, args, item)
	if err == nil {
		for _, elt := range populated[0].IContents {
			if elt.IsCollection() {
				content, err := client.listRecurse(args, elt)
				if err != nil {
					break
				}

				items = append(items, content...)
			} else {
				items = append(items, elt)
			}
		}
	}
	sort.Sort(items)

	return items, err
}

func (client *Client) putRecurse(args Args, item RodsItem) ([]RodsItem, error) {
	var newItems []RodsItem

	// It is just a simple data object
	if item.IsLocalFile() && (item.IsDataObject() || item.IsCollection()) {
		return client.execute(PUT, args, item)
	}

	if !item.IsLocalDir() {
		return newItems, errors.Errorf("cannot recursively put %s "+
			"into %s because the former is not a directory",
			item.LocalPath(), item.RodsPath())
	}
	if !item.IsCollection() {
		return newItems, errors.Errorf("cannot recursively put %s "+
			"into %s because the latter is not a collection",
			item.LocalPath(), item.RodsPath())
	}

	log := logs.GetLogger()
	rodsRoot := item.RodsPath()

	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				log.Warn().Err(err).Str("path", path).
					Msg("file was deleted")
				return nil
			}

			return err
		}

		if !info.IsDir() {
			dir := filepath.Dir(path)
			obj := RodsItem{
				IDirectory: dir,
				IFile:      info.Name(),
				IPath:      filepath.Clean(filepath.Join(rodsRoot, dir)),
				IName:      info.Name()}
			newItems = append(newItems, obj)
		}

		return err
	}

	werr := filepath.Walk(item.LocalPath(), walkFn)
	if werr != nil {
		return newItems, werr
	}

	for i, elt := range newItems {
		// Create the leading collections, if they are not there
		coll := RodsItem{IPath: elt.IPath}
		_, cerr := client.execute(MKDIR, Args{Recurse: true}, coll)
		if cerr != nil {
			return newItems, cerr
		}

		// Put the data object
		objs, oerr := client.execute(PUT, args, elt)
		if oerr != nil {
			return newItems, oerr
		}

		// Update newItems with a populated item
		newItems[i] = objs[0]
	}

	return newItems, nil
}

func (client *Client) execute(op string, args Args, item RodsItem) ([]RodsItem,
	error) {
	response, err := client.send(wrap(op, args, item))
	if err != nil {
		return nil, err
	}

	return unwrap(response)
}

func (client *Client) send(envelope *Envelope) (*Envelope, error) {
	log := logs.GetLogger()

	jsonMessage, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}

	log.Debug().Msgf("Sending %s", jsonMessage)
	client.in <- jsonMessage
	jsonResponse := <-client.out
	log.Debug().Msgf("Received %s", jsonResponse)

	response := &Envelope{}
	if err = json.Unmarshal(jsonResponse, response); err != nil {
		return nil, err
	}

	return response, err
}

func wrap(operation string, args Args, target RodsItem) *Envelope {
	return &Envelope{Operation: operation, Arguments: args, Target: target}
}

func unwrap(envelope *Envelope) ([]RodsItem, error) {
	var items RodsItemArr
	if envelope.ErrorMsg != nil {
		re := RodsError{errors.New(envelope.ErrorMsg.Message),
			envelope.ErrorMsg.Code}

		return items, errors.Wrapf(&re, "%s operation failed",
			envelope.Operation)
	}

	if envelope.Result == nil {
		return items, errors.Errorf("invalid %s operation "+
			"envelope (no result)", envelope.Operation)
	}

	switch {
	case envelope.Result.List != nil:
		items = *envelope.Result.List
	case envelope.Result.Item != nil:
		items = []RodsItem{*envelope.Result.Item}
	default:
		return items, errors.Errorf("invalid %s operation "+
			"result (no content)", envelope.Operation)
	}

	sort.Sort(items)

	for _, item := range items {
		var avus AVUArr = item.IAVUs
		sort.Sort(avus)
		item.IAVUs = avus

		var acls ACLArr = item.IACLs
		sort.Sort(acls)
		item.IACLs = acls

		var timestamps TimestampArr = item.ITimestamps
		sort.Sort(timestamps)
		item.ITimestamps = timestamps
	}

	return items, nil
}
