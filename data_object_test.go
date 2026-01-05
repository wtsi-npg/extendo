/*
 * Copyright (C) 2019, 2020, 2021, 2022, 2026. Genome Research Ltd. All
 * rights reserved.
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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ex "github.com/wtsi-npg/extendo/v3"
)

var _ = Describe("Make an existing DataObject instance from iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
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
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a data object does not exist in iRODS", func() {
		It("should be possible to make a DataObject instance", func() {
			path := "/no/such/object.txt"
			obj := ex.NewDataObject(client, path)
			Expect(obj.Exists()).To(BeFalse())
			Expect(obj.RodsPath()).To(Equal(path))
			Expect(obj.LocalPath()).To(Equal(""))
		})
	})

	When("a data object exists in iRODS", func() {
		It("should be possible to make a DataObject instance", func() {
			path := filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
			obj := ex.NewDataObject(client, path)
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

		rootColl, workColl string

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
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a data object exists", func() {
		BeforeEach(func() {
			path := filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
			obj = ex.NewDataObject(client, path)
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

var _ = Describe("Get the parent of a DataObject", func() {
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

	When("a data object is present", func() {
		It("should have its collection as its parent", func() {
			obj := ex.NewDataObject(client, "/testZone/home/irods/dummy.txt")
			Expect(obj.Parent().RodsPath()).To(Equal("/testZone/home/irods"))
		})
	})
})

var _ = Describe("Put a DataObject into iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
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
		err = removeTmpCollection(workColl)
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

		rootColl, workColl                  string
		localPath, remotePath, newLocalPath string

		checksum    = "1181c1834012245d785120e3505ed169"
		newChecksum = "348bd3ce10ec00ecc29d31ec97cd5839"
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
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("archiving a data object", func() {
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
				creationMeta := ex.MakeCreationMetadata(newChecksum)
				extraMeta := []ex.AVU{
					{Attr: "x", Value: "y"},
					{Attr: "a", Value: "b"},
				}

				obj, err := ex.ArchiveDataObject(client, newLocalPath,
					remotePath, newChecksum, creationMeta, extraMeta)
				Expect(err).NotTo(HaveOccurred())

				avus, err := obj.FetchMetadata()
				Expect(err).NotTo(HaveOccurred())

				expected := ex.SetUnionAVUs(creationMeta, extraMeta)
				Expect(avus).To(ConsistOf(expected))
			})
		})
	})

	When("archiving a collection", func() {
		It("should fail to archive", func() {
			dir := "testdata/1/reads/fast5/"
			_, err := ex.ArchiveDataObject(client, dir, remotePath, "")
			Expect(err).To(HaveOccurred())

			pattern := `put operation failed`
			Expect(err).To(MatchError(MatchRegexp(pattern)))

			code, e := ex.RodsErrorCode(err)
			Expect(e).NotTo(HaveOccurred())

			// With checksum verification enabled, iRODS tries to read the local file
			// first, which in this test is intentionally a directory.
			Expect(code).To(Equal(ex.RodsUserFileDoesNotExist))
		})
	})
})

var _ = Describe("Inspect metadata on an DataObject", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string

		obj  *ex.DataObject
		avus []ex.AVU
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoDataObjectExists")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		remotePath := filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
		obj = ex.NewDataObject(client, remotePath)
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Describe("Checking for a specific AVU (HasMetadatum)", func() {
		When("the object does not have that AVU", func() {
			It("should be false", func() {
				Expect(obj.HasMetadatum(ex.AVU{Attr: "x", Value: "y"})).To(BeFalse())
			})
		})

		When("the object has only that AVU", func() {
			BeforeEach(func() {
				avus = []ex.AVU{{Attr: "x", Value: "y"}}
				err := obj.AddMetadata(avus)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should be true", func() {
				Expect(obj.HasMetadatum(ex.AVU{Attr: "x", Value: "y"})).To(BeTrue())
			})
		})

		When("the object has that, amongst other AVUs", func() {
			BeforeEach(func() {
				avus = []ex.AVU{
					{Attr: "x", Value: "y"},
					{Attr: "x", Value: "a"},
					{Attr: "x", Value: "b"},
					{Attr: "a", Value: "a"},
					{Attr: "b", Value: "b"},
				}
				err := obj.AddMetadata(avus)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should be true", func() {
				Expect(obj.HasMetadatum(ex.AVU{Attr: "x", Value: "y"})).To(BeTrue())
			})
		})
	})

	Describe("Checking for a subset of AVUs (HasSomeMetadata)", func() {
		BeforeEach(func() {
			avus = []ex.AVU{
				{Attr: "x", Value: "y"},
				{Attr: "x", Value: "a"},
				{Attr: "a", Value: "a"},
			}
			err := obj.AddMetadata(avus)
			Expect(err).NotTo(HaveOccurred())
		})

		When("the object does not have any of the AVUs", func() {
			It("should be false", func() {
				Expect(obj.HasSomeMetadata([]ex.AVU{
					{Attr: "z", Value: "y"},
					{Attr: "z", Value: "a"},
					{Attr: "w", Value: "a"},
				})).To(BeFalse())
			})
		})

		When("the object has some of the AVUs", func() {
			It("should be true when the single query AVU is present", func() {
				Expect(obj.HasSomeMetadata([]ex.AVU{
					{Attr: "x", Value: "a"},
				})).To(BeTrue())
			})

			It("should be true when all the query AVUs are present", func() {
				Expect(obj.HasSomeMetadata([]ex.AVU{
					{Attr: "x", Value: "a"},
					{Attr: "a", Value: "a"},
				})).To(BeTrue())
			})

			It("should be true when a subset of the query AVUs are present", func() {
				Expect(obj.HasSomeMetadata([]ex.AVU{
					{Attr: "x", Value: "a"},
					{Attr: "g", Value: "h"},
					{Attr: "i", Value: "j"},
				})).To(BeTrue())
			})
		})

		When("the object has none of the AVUs", func() {
			It("should be false when a none of the query AVUs are present", func() {
				Expect(obj.HasSomeMetadata([]ex.AVU{
					{Attr: "r", Value: "s"},
					{Attr: "t", Value: "u"},
					{Attr: "v", Value: "w"},
				})).To(BeFalse())
			})
		})
	})

	Describe("Checking for a set of AVUs (HasAllMetadata)", func() {
		BeforeEach(func() {
			avus = []ex.AVU{
				{Attr: "x", Value: "y"},
				{Attr: "x", Value: "a"},
				{Attr: "a", Value: "a"},
			}
			err := obj.AddMetadata(avus)
			Expect(err).NotTo(HaveOccurred())
		})

		When("the object does not have any of the AVUs", func() {
			It("should be false", func() {
				Expect(obj.HasAllMetadata([]ex.AVU{
					{Attr: "z", Value: "y"},
					{Attr: "z", Value: "a"},
					{Attr: "w", Value: "a"},
				})).To(BeFalse())
			})
		})

		When("the object has all the AVUs", func() {
			It("should be true when the single query AVU is present", func() {
				Expect(obj.HasAllMetadata([]ex.AVU{
					{Attr: "x", Value: "a"},
				})).To(BeTrue())
			})

			It("should be true when all the query AVUs are present", func() {
				Expect(obj.HasAllMetadata([]ex.AVU{
					{Attr: "x", Value: "a"},
					{Attr: "a", Value: "a"},
				})).To(BeTrue())
			})
		})

		When("the object has some of the AVUs", func() {
			It("should be false when a subset of the query AVUs are present", func() {
				Expect(obj.HasAllMetadata([]ex.AVU{
					{Attr: "x", Value: "a"},
					{Attr: "g", Value: "h"},
					{Attr: "i", Value: "j"},
				})).To(BeFalse())
			})
		})

		When("the object has none ofthe AVUs", func() {
			It("should be false when a none of the query AVUs are present", func() {
				Expect(obj.HasAllMetadata([]ex.AVU{
					{Attr: "r", Value: "s"},
					{Attr: "t", Value: "u"},
					{Attr: "v", Value: "w"},
				})).To(BeFalse())
			})
		})
	})
})

var _ = Describe("Replace metadata on a DataObject", func() {
	var (
		client *ex.Client
		err    error

		rootColl, workColl string
		remotePath         string

		obj *ex.DataObject

		avuA0, avuA1, avuA2, avuB0, avuB1 ex.AVU
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoMetadataReplace")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		remotePath = filepath.Join(workColl, "testdata/1/reads/fast5/reads1.fast5")
	})

	AfterEach(func() {
		err = removeTmpCollection(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("replacing metadata on data objects", func() {
		BeforeEach(func() {
			obj = ex.NewDataObject(client, remotePath)

			avuA0 = ex.AVU{Attr: "a", Value: "0", Units: "z"}
			avuA1 = ex.AVU{Attr: "a", Value: "1"}
			avuA2 = ex.AVU{Attr: "a", Value: "2", Units: "z"}

			avuB0 = ex.AVU{Attr: "b", Value: "0", Units: "z"}
			avuB1 = ex.AVU{Attr: "b", Value: "1", Units: "z"}

			err = obj.AddMetadata([]ex.AVU{avuA0, avuA1, avuA2, avuB0, avuB1})
			Expect(err).NotTo(HaveOccurred())
		})

		When("it shares attributes with existing metadata", func() {
			It("should be added", func() {
				newAVU := ex.AVU{Attr: "a", Value: "nvalue", Units: "nunits"}

				err := obj.ReplaceMetadata([]ex.AVU{newAVU})
				Expect(err).NotTo(HaveOccurred())

				Expect(obj.Metadata()).To(Equal([]ex.AVU{newAVU, avuB0, avuB1}))
			})
		})
	})
})
