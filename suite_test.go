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
 * @file suite_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

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
