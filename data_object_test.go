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
 * @file data_object_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo_test

import (
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	ex "extendo"
)

var _ = Describe("Make an existing DataObject instance from iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoNewDataObject")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a data object exists in iRODS", func() {
		It("should be possible to make a DataObject instance", func() {
			path := filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
			obj, err := ex.NewDataObject(client, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj.Exists()).To(BeTrue())
			Expect(obj.RodsPath()).To(Equal(path))
			Expect(obj.LocalPath()).To(Equal(""))
		})
	})
})

var _ = Describe("Report that a DataObject exists", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		obj *ex.DataObject
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoDataObjectExists")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a collection exists", func() {
		BeforeEach(func() {
			path := filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
			obj, err = ex.NewDataObject(client, path)
			Expect(err).NotTo(HaveOccurred())
		})

		When("Exists() is called", func() {
			It("should return true", func() {
				Expect(obj.Exists()).To(BeTrue())
			})
		})

		When("the Data object has gone and Exists() is called", func() {
			It("should return false", func() {
				err = obj.Remove()
				Expect(err).NotTo(HaveOccurred())
				Expect(obj.Exists()).To(BeFalse())
			})
		})
	})
})

var _ = Describe("Put a DataObject into iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoPutDataObject")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a new data object is put into iRODS", func() {
		It("should be present afterwards", func() {
			localPath := "testdata/1/reads/fast5/reads1.fast5"
			remotePath := filepath.Join(workColl, "testdata/testdir/reads1.fast5")

			obj, err := ex.PutDataObject(client, localPath, remotePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(obj.Exists()).To(BeTrue())
			Expect(obj.RodsPath()).To(Equal(remotePath))
			Expect(obj.Checksum()).To(Equal("1181c1834012245d785120e3505ed169"))
		})
	})
})

var _ = Describe("Archive a DataObject into iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		localPath  string
		remotePath string
		checksum   = "1181c1834012245d785120e3505ed169"

		newLocalPath string
		newChecksum  = "348bd3ce10ec00ecc29d31ec97cd5839"
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoDataObjectArchive")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		localPath = "testdata/1/reads/fast5/reads1.fast5"
		remotePath = filepath.Join(workColl, "testdata/testdir/reads99.fast5")

		newLocalPath = "testdata/1/reads/fast5/reads2.fast5"
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("archiving a data object", func() {
		When("it is a new data object", func() {
			When("the expected checksum is matched", func() {
				It("should be present in iRODS afterwards", func() {
					obj, err := ex.ArchiveDataObject(client, localPath,
						remotePath, checksum)
					Expect(err).NotTo(HaveOccurred())

					Expect(obj.Exists()).To(BeTrue())
					Expect(obj.RodsPath()).To(Equal(remotePath))
				})

				It("should have the expected checksum", func() {
					obj, err := ex.ArchiveDataObject(client, localPath,
						remotePath, checksum)
					Expect(err).NotTo(HaveOccurred())

					Expect(obj.Checksum()).To(Equal(checksum))
				})
			})

			When("the checksum is mismatched", func() {
				It("archiving should fail", func() {
					dummyChecksum := "no_such_checksum"
					_, err := ex.ArchiveDataObject(client, localPath,
						remotePath, dummyChecksum)
					Expect(err).To(HaveOccurred())

					pattern := `failed to archive.*did not match remote checksum`
					Expect(err).To(MatchError(MatchRegexp(pattern)))
				})
			})
		})

		When("overwriting a data object with a different file", func() {
			When("the new expected checksum is matched", func() {
				It("should be present in iRODS afterwards", func() {
					obj, err := ex.ArchiveDataObject(client, newLocalPath,
						remotePath, newChecksum)
					Expect(err).NotTo(HaveOccurred())

					Expect(obj.Exists()).To(BeTrue())
				})

				It("should have the new expected checksum", func() {
					obj, err := ex.ArchiveDataObject(client, newLocalPath,
						remotePath, newChecksum)
					Expect(err).NotTo(HaveOccurred())

					Expect(obj.Checksum()).To(Equal(newChecksum))
				})
			})
		})

		When("metadata are supplied", func() {
			It("should be present afterwards", func() {
				creationMeta := ex.MakeCreationMetadata()
				extraMeta := []ex.AVU{
					ex.MakeAVU("x", "y"),
					ex.MakeAVU("a", "b")}

				obj, err := ex.ArchiveDataObject(client, newLocalPath,
					remotePath, newChecksum, creationMeta, extraMeta)
				Expect(err).NotTo(HaveOccurred())

				avus, err := obj.FetchMetadata()
				Expect(err).NotTo(HaveOccurred())

				Expect(avus).To(ConsistOf(ex.SetUnionAVUs(creationMeta, extraMeta)))
			})
		})
	})

	Context("archiving a collection", func() {
		It("should fail to archive", func() {
			dir := "testdata/1/reads/fast5/"
			_, err := ex.ArchiveDataObject(client, dir, remotePath, "")
			Expect(err).To(HaveOccurred())

			pattern := `put operation failed`
			Expect(err).To(MatchError(MatchRegexp(pattern)))

			code, e := ex.RodsErrorCode(err)
			Expect(e).NotTo(HaveOccurred())

			Expect(code).To(Equal(ex.RodsUserFileDoesNotExist))
		})
	})
})

var _ = Describe("Replace metadata on a DataObject", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		remotePath string
		obj        *ex.DataObject

		avuA0, avuA1, avuA2, avuB0, avuB1 ex.AVU
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoReplace")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		remotePath = filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("replacing metadata on data objects", func() {
		BeforeEach(func() {
			obj, err = ex.NewDataObject(client, remotePath)
			Expect(err).NotTo(HaveOccurred())

			avuA0 = ex.MakeAVU("a", "0", "z")
			avuA1 = ex.MakeAVU("a", "1")
			avuA2 = ex.MakeAVU("a", "2", "z")

			avuB0 = ex.MakeAVU("b", "0", "z")
			avuB1 = ex.MakeAVU("b", "1", "z")

			err = obj.AddMetadata([]ex.AVU{avuA0, avuA1, avuA2, avuB0, avuB1})
			Expect(err).NotTo(HaveOccurred())
		})

		When("it shares attributes with existing metadata", func() {
			It("should be added", func() {
				newAVU := ex.MakeAVU("a", "nvalue", "nunits")

				err := obj.ReplaceMetadata([]ex.AVU{newAVU})
				Expect(err).NotTo(HaveOccurred())

				Expect(obj.Metadata()).To(Equal([]ex.AVU{newAVU, avuB0, avuB1}))
			})
		})
	})
})
