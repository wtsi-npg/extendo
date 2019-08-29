package extendo_test

import (
	"testing"

	logs "github.com/kjsanger/logshim"
	"github.com/kjsanger/logshim/dlog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Define the Extendo test suite. The tests themselves are defined in separate
// files. These are BDD-style blackbox tests conducted from outside the extendo
// package.
func TestExtendo(t *testing.T) {
	log := dlog.New(GinkgoWriter, logs.ErrorLevel)
	logs.InstallLogger(log)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Extendo Suite")
}
