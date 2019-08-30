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
 * @file helpers_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"

	ex "extendo"
)

var batonArgs = []string{"--unbuffered"}

var dirPaths = []string{
	"testdata",
	"testdata/1",
	"testdata/1/reads",
	"testdata/1/reads/fast5",
	"testdata/1/reads/fastq",
	"testdata/testdir"}

var filePaths = []string{
	"testdata/1/reads/fast5/reads1.fast5",
	"testdata/1/reads/fast5/reads1.fast5.md5",
	"testdata/1/reads/fast5/reads2.fast5",
	"testdata/1/reads/fast5/reads3.fast5",
	"testdata/1/reads/fastq/reads1.fastq",
	"testdata/1/reads/fastq/reads1.fastq.md5",
	"testdata/1/reads/fastq/reads2.fastq",
	"testdata/1/reads/fastq/reads3.fastq",
	"testdata/testdir/.gitignore",
}

// Copy test data from local directory src into iRODS collection dst
func putTestData(src string, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	client, err := ex.FindAndStart("--unbuffered")
	if err != nil {
		return err
	}

	_, err = client.MkDir(ex.Args{Recurse: true}, ex.RodsItem{IPath: dst})
	if err != nil {
		return err
	}

	_, err = client.Put(ex.Args{Checksum: true, Recurse: true},
		ex.RodsItem{IDirectory: src, IPath: dst})
	if err != nil {
		return err
	}

	return err
}

// Remove test data recursively from under path dst from iRODS
func removeTestData(dst string) error {
	client, err := ex.FindAndStart("--unbuffered")
	if err != nil {
		return err
	}
	_, err = client.RemDir(ex.Args{Force: true, Recurse: true},
		ex.RodsItem{IPath: dst})
	if err != nil {
		return err
	}

	return client.Stop()
}

// Return a new pseudo randomised path in iRODS
func tmpRodsPath(root string, prefix string) string {
	s := rand.NewSource(GinkgoRandomSeed())
	r := rand.New(s)
	d := fmt.Sprintf("%s.%d.%010d", prefix, os.Getpid(), r.Uint32())
	return filepath.Join(root, d)
}
