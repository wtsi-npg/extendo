/*
 * Copyright (C) 2019, 2020 Genome Research Ltd. All rights reserved.
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
 * @file extendo_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"os"
	"testing"
	"time"

	logs "github.com/kjsanger/logshim"
	"github.com/kjsanger/logshim-zerolog/zlog"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// These are whitebox Unit tests with access to extendo package internals.
func TestMain(m *testing.M) {
	loggerImpl := zlog.New(os.Stderr, logs.ErrorLevel)

	writer := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	consoleLogger := loggerImpl.Logger.Output(zerolog.SyncWriter(writer))
	loggerImpl.Logger = &consoleLogger
	logs.InstallLogger(loggerImpl)

	os.Exit(m.Run())
}

func TestFindBaton(t *testing.T) {
	path, err := FindBaton()
	if assert.NoError(t, err) {
		info, err := os.Stat(path)
		if assert.NoError(t, err) {
			assert.False(t, info.IsDir())
			assert.Equal(t, info.Name(), "baton-do")
		}
	}
}

func TestStartClient(t *testing.T) {
	bc, err := FindAndStart()
	if assert.NoError(t, err, "Failed to start baton-do") {
		assert.True(t, bc.IsRunning(), "baton-do is not running")
		_ = bc.Stop()
	}
}

func TestStopClient(t *testing.T) {
	bc, err := FindAndStart()
	if assert.NoError(t, err, "Failed to start baton-do") {
		assert.True(t, bc.IsRunning(), "baton-do is not running")

		if assert.NoError(t, bc.Stop(), "Failed to stop baton-do") {
			assert.True(t, bc.cmd.ProcessState.Success(),
				"baton-do did not run successfully")
		}
	}
}

func TestIsRunning(t *testing.T) {
	bc, err := FindAndStart()
	if assert.NoError(t, err, "Failed to start baton-do") {
		assert.True(t, bc.IsRunning(), "baton-do is not running")
		_ = bc.Stop()
		assert.False(t, bc.IsRunning(), "baton-do is still running")
	}
}

func TestClientPid(t *testing.T) {
	bc, err := FindAndStart()
	if assert.NoError(t, err, "Failed to start baton-do") {
		pid := bc.ClientPid()
		assert.NotNil(t, pid, "baton-do PID is %d", pid)
		_ = bc.Stop()
	}
}

func TestIsLocalDir(t *testing.T) {
	root := &RodsItem{IDirectory: "/"}
	assert.True(t, root.IsLocalDir())

	file1 := RodsItem{IDirectory: "/", IFile: "x"}
	assert.False(t, file1.IsLocalDir())
}

func TestIsLocalFile(t *testing.T) {
	root := &RodsItem{IDirectory: "/"}
	assert.False(t, root.IsLocalFile())

	file1 := RodsItem{IDirectory: "/", IFile: "x"}
	assert.True(t, file1.IsLocalFile())
}

func TestIsCollection(t *testing.T) {
	root := &RodsItem{IPath: "/testZone"}
	assert.True(t, root.IsCollection())

	file1 := RodsItem{IPath: "/testZone", IName: "x"}
	assert.False(t, file1.IsCollection())
}

func TestIsDataObject(t *testing.T) {
	root := &RodsItem{IPath: "/testZone"}
	assert.False(t, root.IsDataObject())

	file1 := RodsItem{IPath: "/testZone", IName: "x"}
	assert.True(t, file1.IsDataObject())
}

func TestSearchAVU(t *testing.T) {
	avu0 := AVU{Attr: "x", Value: "y", Units: "z"}
	avu1 := AVU{Attr: "a", Value: "b", Units: "z"}
	avu2 := AVU{Attr: "w", Value: "x", Units: "z"}

	// Unsorted array (will fail test unless sorted before the binary search)
	avus := []AVU{avu0, avu1, avu2}

	// SearchAVU sorts for us
	assert.True(t, SearchAVU(avu0, avus),
		"%v was not found in", avu0, avus)
	assert.True(t, SearchAVU(avu1, avus),
		"%v was not found in", avu1, avus)
	assert.True(t, SearchAVU(avu2, avus),
		"%v was not found in", avu2, avus)

	assert.False(t, SearchAVU(AVU{Attr: "f", Value: "g"}, avus))
}

func TestSetIntersectAVUs(t *testing.T) {
	avu0 := AVU{Attr: "x", Value: "y", Units: "z"}
	avu1 := AVU{Attr: "a", Value: "b", Units: "z"}
	avu2 := AVU{Attr: "w", Value: "x", Units: "z"}
	avu3 := AVU{Attr: "1", Value: "2", Units: "3"}
	avu4 := AVU{Attr: "4", Value: "5", Units: "6"}

	avusX := []AVU{avu0, avu1, avu2, avu3}
	avusY := []AVU{avu4, avu3, avu2}

	intersection := SetIntersectAVUs(avusX, avusY)
	assert.Equal(t, 2, len(intersection))

	for _, avu := range []AVU{avu3, avu2} {
		assert.Contains(t, intersection, avu)
	}
}

func TestSetUnionAVUs(t *testing.T) {
	avu0 := AVU{Attr: "x", Value: "y", Units: "z"}
	avu1 := AVU{Attr: "a", Value: "b", Units: "z"}
	avu2 := AVU{Attr: "w", Value: "x", Units: "z"}
	avu3 := AVU{Attr: "1", Value: "2", Units: "3"}
	avu4 := AVU{Attr: "4", Value: "5", Units: "6"}

	avusX := []AVU{avu0, avu1, avu2, avu3}
	avusY := []AVU{avu4, avu3, avu2}

	union := SetUnionAVUs(avusX, avusY)
	assert.Equal(t, 5, len(union))

	for _, avu := range []AVU{avu0, avu1, avu2, avu3, avu4} {
		assert.Contains(t, union, avu)
	}
}

func TestUniqAVUs(t *testing.T) {
	avu0 := AVU{Attr: "x", Value: "y", Units: "z"}
	avu1 := AVU{Attr: "a", Value: "b", Units: "z"}
	avu2 := AVU{Attr: "w", Value: "x", Units: "z"}

	avus := []AVU{avu1, avu2, avu0, avu0, avu1, avu0, avu1}

	expected := []AVU{avu0, avu1, avu2}
	SortAVUs(expected)

	assert.Equal(t, UniqAVUs(avus), expected)
}

func TestAVU_HasNamespace(t *testing.T) {
	assert.False(t, AVU{Attr:"x",Value: "y"}.HasNamespace())
	assert.False(t, AVU{Attr:":x", Value:"y"}.HasNamespace())
	assert.True(t, AVU{Attr:"a:x", Value: "y"}.HasNamespace())
	assert.True(t, AVU{Attr:"aa:x",Value: "y"}.HasNamespace())
}

func TestAVU_SetNamespace(t *testing.T) {
	ns := "a"

	avu := AVU{Attr: "x", Value: "y", Units: "z"}.WithNamespace(ns)
	assert.True(t, avu.HasNamespace())
	assert.Equal(t, ns, avu.Namespace())
}

func TestAVU_Namespace(t *testing.T) {
	avu0 := AVU{Attr:"a:x", Value: "y", Units: "z"}
	assert.Equal(t, avu0.Namespace(), "a")

	avu1 := AVU{Attr: "x", Value: "y", Units: "z"}
	assert.Equal(t, "", avu1.Namespace())
}

func TestAVU_NamespacedAttr(t *testing.T) {
	avu0 := AVU{Attr: "a:x", Value: "y", Units: "z"}
	assert.True(t, avu0.HasNamespace())
	assert.Equal(t, "x", avu0.WithoutNamespace())

	avu1 := AVU{Attr: "x", Value: "y", Units: "z"}
	assert.Equal(t, "x", avu1.WithoutNamespace())
}
