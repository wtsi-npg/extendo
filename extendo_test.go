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

func TestMain(m *testing.M) {
	loggerImpl := zlog.New(os.Stderr, logs.WarnLevel)

	writer := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	consoleLogger := loggerImpl.Logger.Output(zerolog.SyncWriter(writer))
	loggerImpl.Logger = &consoleLogger
	logs.InstallLogger(loggerImpl)

	os.Exit(m.Run())
}

func TestFindBaton(t *testing.T) {
	path, err := FindBaton()
	assert.NoError(t, err)
	info, err := os.Stat(path)
	if assert.NoError(t, err) {
		assert.False(t, info.IsDir())
		assert.Equal(t, info.Name(), "baton-do")
	}
}

func TestStartClient(t *testing.T) {
	bc, err := FindAndStart()
	assert.NoError(t, err, "Failed to start baton-do")
	assert.True(t, bc.IsRunning(), "baton-do is not running")

	_ = bc.Stop()
}

func TestStopClient(t *testing.T) {
	bc, err := FindAndStart()
	assert.NoError(t, err, "Failed to start baton-do")

	assert.True(t, bc.IsRunning(), "baton-do is not running")

	assert.NoError(t, bc.Stop(), "Failed to stop baton-do")
	assert.True(t, bc.cmd.ProcessState.Success(),
		"baton-do did not run successfully")
}

func TestIsRunning(t *testing.T) {
	bc, err := FindAndStart()
	assert.NoError(t, err, "Failed to start baton-do")
	assert.True(t, bc.IsRunning(), "baton-do is not running")
	_ = bc.Stop()
	assert.False(t, bc.IsRunning(), "baton-do is still running")
}

func TestClientPid(t *testing.T) {
	bc, _ := FindAndStart()
	pid := bc.ClientPid()
	assert.NotNil(t, pid, "baton-do PID is %d", pid)
	_ = bc.Stop()
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
	avu0 := MakeAVU("x", "y", "z")
	avu1 := MakeAVU("a", "b", "z")
	avu2 := MakeAVU("w", "x", "z")

	// Unsorted array (will fail test unless sorted before the binary search)
	avus := []AVU{avu0, avu1, avu2}

	// SearchAVU sorts for us
	assert.True(t, SearchAVU(avu0, avus),
		"%v was not found in", avu0, avus)
	assert.True(t, SearchAVU(avu1, avus),
		"%v was not found in", avu1, avus)
	assert.True(t, SearchAVU(avu2, avus),
		"%v was not found in", avu2, avus)

	assert.False(t, SearchAVU(MakeAVU("f", "g"), avus))
}

func TestIntersectionAVUs(t *testing.T) {
	avu0 := MakeAVU("x", "y", "z")
	avu1 := MakeAVU("a", "b", "z")
	avu2 := MakeAVU("w", "x", "z")
	avu3 := MakeAVU("1", "2", "3")
	avu4 := MakeAVU("4", "5", "6")

	avusX := []AVU{avu0, avu1, avu2, avu3}
	avusY := []AVU{avu4, avu3, avu2}

	assert.Equal(t, IntersectionAVUs(avusX, avusY), []AVU{avu3, avu2})
}

func TestUniqAVUs(t *testing.T) {
	avu0 := MakeAVU("x", "y", "z")
	avu1 := MakeAVU("a", "b", "z")
	avu2 := MakeAVU("w", "x", "z")

	avus := []AVU{avu1, avu2, avu0, avu0, avu1, avu0, avu1}
	assert.Equal(t, UniqAVUs(avus), SortAVUs([]AVU{avu0, avu1, avu2}))
}
