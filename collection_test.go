/*
 * Copyright (C) 2019, 2020, 2021, 2022. Genome Research Ltd. All rights
 * reserved.
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
 * @file collection_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo_test

import (
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ex "github.com/wtsi-npg/extendo/v2"
)

var _ = Describe("Make an existing Collection instance from iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoNewCollection")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a collection does not exist in iRODS", func() {
		It("should be possible to make a Collection instance", func() {
			path := "/no/such/collection"
			coll := ex.NewCollection(client, path)
			Expect(coll.Exists()).To(BeFalse())
			Expect(coll.RodsPath()).To(Equal(path))
			Expect(coll.LocalPath()).To(Equal(""))
		})
	})

	When("a collection exists in iRODS", func() {
		It("should be possible to make a Collection instance", func() {
			coll := ex.NewCollection(client, workColl)
			Expect(coll.Exists()).To(BeTrue())
			Expect(coll.RodsPath()).To(Equal(workColl))
			Expect(coll.LocalPath()).To(Equal(""))
		})
	})
})

var _ = Describe("Report that a Collection exists", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string

		coll *ex.Collection
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoCollectionExists")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a collection exists", func() {
		BeforeEach(func() {
			coll, err = ex.MakeCollection(client, filepath.Join(workColl, "testdata"))
			Expect(err).NotTo(HaveOccurred())
		})

		When("Exists() is called", func() {
			It("should return true", func() {
				Expect(coll.Exists()).To(BeTrue())
			})
		})

		When("the collection has gone and Exists() is called", func() {
			It("should return false", func() {
				err = coll.RemoveRecurse()
				Expect(err).NotTo(HaveOccurred())
				Expect(coll.Exists()).To(BeFalse())
			})
		})
	})
})

var _ = Describe("Make a new Collection in iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoMakeCollection")
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a new collection is made in iRODS", func() {
		When("its parent collections already exist", func() {
			It("should be present afterwards", func() {
				remotePath := filepath.Join(workColl, "testdata")

				coll, err := ex.MakeCollection(client, remotePath)
				Expect(err).ToNot(HaveOccurred())
				Expect(coll.Exists()).To(BeTrue())
				Expect(coll.RodsPath()).To(Equal(remotePath))
			})
		})

		When("its parent collections do not exist", func() {
			It("should be present afterwards", func() {
				remotePath := filepath.Join(workColl, "testdata", "1", "2", "3")

				coll, err := ex.MakeCollection(client, remotePath)
				Expect(err).ToNot(HaveOccurred())
				Expect(coll.Exists()).To(BeTrue())
				Expect(coll.RodsPath()).To(Equal(remotePath))
			})
		})
	})
})

var _ = Describe("Put a Collection into iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoPutCollection")
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a new collection is put into iRODS", func() {
		It("should be present afterwards", func() {
			localPath := "testdata"
			remotePath := filepath.Join(workColl, "testdata")

			coll, err := ex.PutCollection(client, localPath, remotePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(coll.RodsPath()).To(Equal(remotePath))
		})
	})
})

var _ = Describe("Get the parent of a collection", func() {
	var (
		client *ex.Client
		err    error
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		client.StopIgnoreError()
	})

	When("a collection is not the root", func() {
		It("should have an appropriate parent", func() {
			coll := ex.NewCollection(client, "/testZone/home/irods")
			Expect(coll.Parent().RodsPath()).To(Equal("/testZone/home"))
			Expect(coll.Parent().Parent().RodsPath()).To(Equal("/testZone"))
			Expect(coll.Parent().Parent().Parent().RodsPath()).To(Equal("/"))
		})
	})

	When("a collection is the root", func() {
		It("should return itself as parent", func() {
			coll := ex.NewCollection(client, "/")
			Expect(coll.Parent().RodsPath()).To(Equal("/"))
		})
	})
})

var _ = Describe("Ensure that a Collection exists", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoEnsureCollection")
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a leaf collection does not exist", func() {
		It("should be created", func() {
			path := filepath.Join(workColl, "my_new_collection")
			coll := ex.NewCollection(client, path)
			err = coll.Ensure()
			Expect(err).NotTo(HaveOccurred())
			Expect(coll.Exists()).To(BeTrue())
			Expect(coll.RodsPath()).To(Equal(path))
		})
	})

	When("a branch collection does not exist", func() {
		It("should be created", func() {
			path := filepath.Join(workColl, "my_new_collection/and_another/and_finally")
			coll := ex.NewCollection(client, path)
			err = coll.Ensure()
			Expect(err).NotTo(HaveOccurred())
			Expect(coll.Exists()).To(BeTrue())
			Expect(coll.RodsPath()).To(Equal(path))
		})
	})

})

var _ = Describe("List a Collection contents", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string

		getRodsPaths itemPathTransform
		getCollPaths collPathTransform
		getObjPaths  objPathTransform
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoNewCollection")

		getRodsPaths = makeRodsItemTransform(workColl)
		getCollPaths = makeCollTransform(workColl)
		getObjPaths = makeObjTransform(workColl)

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a collection contents are not fetched", func() {
		It("should be empty", func() {
			coll := ex.NewCollection(client, filepath.Join(workColl, "testdata"))
			Expect(coll.Contents()).To(BeEmpty())
		})
	})

	When("a collection contents are fetched without recursion", func() {
		It("should return the shallow contents", func() {
			coll := ex.NewCollection(client, filepath.Join(workColl, "testdata"))
			items, err := coll.FetchContents()
			Expect(err).NotTo(HaveOccurred())

			expected := []string{"testdata/1", "testdata/testdir"}
			Expect(items).To(WithTransform(getRodsPaths, ConsistOf(expected)))
			Expect(coll.Collections()).To(WithTransform(getCollPaths, ConsistOf(expected)))
			Expect(coll.DataObjects()).To(BeEmpty())
		})
	})

	When("a collection contents are fetched with recursion", func() {
		It("should return the deep contents", func() {
			coll := ex.NewCollection(client, filepath.Join(workColl, "testdata"))
			items, err := coll.FetchContentsRecurse()
			Expect(err).NotTo(HaveOccurred())

			expectedItems := []string{
				"testdata",
				"testdata/1",
				"testdata/1/reads",
				"testdata/1/reads/fast5",
				"testdata/1/reads/fastq",
				"testdata/testdir",
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
			Expect(items).To(WithTransform(getRodsPaths,
				ConsistOf(expectedItems)))

			expectedColls := []string{
				"testdata",
				"testdata/1",
				"testdata/1/reads",
				"testdata/1/reads/fast5",
				"testdata/1/reads/fastq",
				"testdata/testdir",
			}
			Expect(coll.Collections()).To(WithTransform(getCollPaths,
				ConsistOf(expectedColls)))

			expectedObjs := []string{
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
			Expect(coll.DataObjects()).To(WithTransform(getObjPaths,
				ConsistOf(expectedObjs)))
		})
	})
})
