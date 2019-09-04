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
 * @file collection_test.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo_test

import (
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	ex "github.com/kjsanger/extendo"
)

var _ = Describe("Make an existing Collection instance from iRODS", func() {
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
		workColl = tmpRodsPath(rootColl, "ExtendoNewCollection")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a collection exists in iRODS", func() {
		It("should be possible to make a Collection instance", func() {
			coll, err := ex.NewCollection(client, workColl)
			Expect(err).NotTo(HaveOccurred())
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

		rootColl string
		workColl string

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
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a collection exists", func() {
		BeforeEach(func() {
			coll, err = ex.NewCollection(client, filepath.Join(workColl, "testdata"))
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

		rootColl string
		workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoMakeCollection")
	})

	AfterEach(func() {
		err = removeTestData(workColl)
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

		rootColl string
		workColl string
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoPutCollection")
	})

	AfterEach(func() {
		err = removeTestData(workColl)
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
