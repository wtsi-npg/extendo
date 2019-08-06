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

const batonExecutable = "baton-do"

func TestMain(m *testing.M) {
	loggerImpl := zlog.New(os.Stderr, logs.WarnLevel)

	writer := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	consoleLogger := loggerImpl.Logger.Output(zerolog.SyncWriter(writer))
	loggerImpl.Logger = &consoleLogger
	logs.InstallLogger(loggerImpl)

	os.Exit(m.Run())
}

func TestStartClient(t *testing.T) {
	bc, err := Start(batonExecutable)
	assert.NoError(t, err, "Failed to start %s", batonExecutable)
	assert.True(t, bc.IsRunning(),
		"%s is not running", batonExecutable)

	_ = bc.Stop()
}

func TestStopClient(t *testing.T) {
	bc, err := Start(batonExecutable)
	assert.NoError(t, err, "Failed to start %s", batonExecutable)

	assert.True(t, bc.IsRunning(),
		"%s is not running", batonExecutable)

	assert.NoError(t, bc.Stop(),
		"Failed to stop %s", batonExecutable)
	assert.True(t, bc.cmd.ProcessState.Success(),
		"%s did not run successfully", batonExecutable)
}

func TestIsRunning(t *testing.T) {
	bc, err := Start(batonExecutable)
	assert.NoError(t, err, "Failed to start %s", batonExecutable)
	assert.True(t, bc.IsRunning(),
		"%s is not running", batonExecutable)
	_ = bc.Stop()
	assert.False(t,
		bc.IsRunning(), "%s is still running", batonExecutable)
}

func TestClientPid(t *testing.T) {
	bc, _ := Start(batonExecutable)
	pid := bc.ClientPid()
	assert.NotNil(t, pid, "%s PID is %d", batonExecutable, pid)
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
