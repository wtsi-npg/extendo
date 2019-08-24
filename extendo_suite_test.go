package extendo_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	ex "extendo"
	logs "github.com/kjsanger/logshim"
	"github.com/kjsanger/logshim/dlog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
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

func TestExtendo(t *testing.T) {
	log := dlog.New(GinkgoWriter, logs.DebugLevel)
	logs.InstallLogger(log)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Item Suite")
}

var _ = Describe("Start and stop the Item client", func() {
	var (
		client *ex.Client
		err    error
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
	})

	Describe("Stop and start", func() {
		Context("When the client is not running", func() {
			It("should start without error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should be running", func() {
				Expect(client.IsRunning()).To(BeTrue())
			})

			When("Start is attempted", func() {
				It("should return an error", func() {
					_, err := client.Start()
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError("client is already running"))
				})
			})
		})

		Context("When the client is running", func() {
			BeforeEach(func() {
				err = client.Stop()
			})

			It("should stop without error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should not be running", func() {
				Expect(client.IsRunning()).To(BeFalse())
			})
		})

	})
})

var _ = Describe("List an iRODS path", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		testColl     ex.RodsItem
		testObj      ex.RodsItem
		testChecksum = "1181c1834012245d785120e3505ed169"
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoList")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("the path does not exist", func() {
		It("should return an iRODS -310000 error", func() {
			path := filepath.Join(rootColl, "does_not_exist")
			item := ex.RodsItem{IPath: path}

			_, err := client.ListItem(ex.Args{}, item)
			Expect(err).To(HaveOccurred())

			var expected int32 = -310000
			var code int32

			switch err := errors.Cause(err).(type) {
			case *ex.RodsError:
				code = err.Code()
			}

			Expect(code).To(Equal(expected))
		})
	})

	When("the item is a collection", func() {
		BeforeEach(func() {
			testColl = ex.RodsItem{IPath: filepath.Join(workColl, "testdata")}
		})

		Context("multiple items are requested", func() {
			It("should return a RodsItem with that path", func() {
				items, err := client.List(ex.Args{}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(items)).To(Equal(1))
				Expect(items[0].IPath).To(Equal(testColl.IPath))
			})

			When("contents are requested", func() {
				It("should return contents", func() {
					items, err := client.List(ex.Args{Contents: true}, testColl)
					Expect(err).NotTo(HaveOccurred())

					Expect(len(items)).To(Equal(1))
					contents := items[0].IContents
					Expect(contents).To(Equal([]ex.RodsItem{
						{IPath: filepath.Join(testColl.IPath, "1")},
						{IPath: filepath.Join(testColl.IPath, "testdir")},
					}))
				})
			})

			When("metadata are requested", func() {
				BeforeEach(func() {
					testColl.IAVUs = []ex.AVU{{Attr: "test_attr_x", Value: "y"}}
					_, err = client.MetaAdd(ex.Args{}, testColl)
					Expect(err).NotTo(HaveOccurred())
				})

				It("should have metadata", func() {
					items, err := client.List(ex.Args{AVU: true}, testColl)
					Expect(err).NotTo(HaveOccurred())

					Expect(len(items)).To(Equal(1))
					metadata := items[0].IAVUs
					Expect(metadata).To(Equal([]ex.AVU{{Attr: "test_attr_x", Value: "y"}}))
				})
			})

			When("ACLs are requested", func() {
				It("should have ACLs if requested", func() {
					items, err := client.List(ex.Args{ACL: true}, testColl)
					Expect(err).NotTo(HaveOccurred())

					Expect(len(items)).To(Equal(1))
					acls := items[0].IACLs
					Expect(acls).To(Equal([]ex.ACL{{Owner: "irods",
						Level: "own", Zone: "testZone"}}))
				})
			})

			When("contents are recursed", func() {
				It("should return a recursive slice of contents", func() {
					items, err := client.List(ex.Args{Recurse: true}, testColl)
					Expect(err).NotTo(HaveOccurred())

					var expected []ex.RodsItem
					for _, dirPath := range dirPaths {
						expected = append(expected,
							ex.RodsItem{
								IPath: filepath.Join(workColl, dirPath)})
					}

					for _, filePath := range filePaths {
						objPath := filepath.Join(workColl, filePath)
						expected = append(expected,
							ex.RodsItem{
								IPath: filepath.Dir(objPath),
								IName: filepath.Base(objPath)},
						)
					}

					Expect(items).To(Equal(expected))
				})
			})
		})

		Context("a single item is requested", func() {
			It("should return a RodsItem with that path", func() {
				item, err := client.ListItem(ex.Args{}, testColl)
				Expect(err).NotTo(HaveOccurred())
				Expect(item.IPath).To(Equal(testColl.IPath))
			})

			When("a recursive list are requested", func() {
				It("should raise an error", func() {
					_, err := client.ListItem(ex.Args{Recurse: true}, testColl)
					Expect(err).To(HaveOccurred())

					Expect(err).To(MatchError("invalid argument: " +
						"Recurse=true"))
				})
			})
		})
	})

	When("the item is a data object", func() {
		BeforeEach(func() {
			testObj = ex.RodsItem{
				IPath: filepath.Join(workColl, "testdata/1/reads/fast5"),
				IName: "reads1.fast5"}
		})

		Context("multiple items are requested", func() {
			It("should return a RodsItem with that path and name", func() {
				items, err := client.List(ex.Args{}, testObj)
				Expect(err).NotTo(HaveOccurred())
				Expect(items[0]).To(Equal(testObj))
			})

			It("should have a checksum if requested", func() {
				items, err := client.List(ex.Args{Checksum: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(items)).To(Equal(1))
				Expect(items[0].IChecksum).To(Equal(testChecksum))
			})

			It("should have a size if requested", func() {
				items, err := client.List(ex.Args{Size: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				var expected uint64 = 4
				Expect(len(items)).To(Equal(1))
				Expect(items[0].ISize).To(Equal(expected))
			})

			It("should have replicate information if requested", func() {
				items, err := client.List(ex.Args{Replicate: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(items)).To(Equal(1))
				reps := items[0].IReplicates
				Expect(len(reps)).To(Equal(1))

				Expect(reps[0]).To(Equal(ex.Replicate{
					Resource: "demoResc",
					Location: "localhost",
					Checksum: testChecksum,
					Valid:    true}))
			})

			It("should have timestamp information if requested", func() {
				items, err := client.List(ex.Args{Timestamp: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(items)).To(Equal(1))
				stamps := items[0].ITimestamps
				Expect(len(stamps)).To(Equal(2))

				Expect(stamps[0].Created).
					To(BeTemporally("~", time.Now(), time.Minute))
				Expect(stamps[1].Modified).
					To(BeTemporally("~", time.Now(), time.Minute))
			})

			When("metadata are requested", func() {
				BeforeEach(func() {
					testObj.IAVUs = []ex.AVU{
						{Attr: "test_attr_a", Value: "1"},
						{Attr: "test_attr_b", Value: "2"},
						{Attr: "test_attr_c", Value: "3"}}
					_, err = client.MetaAdd(ex.Args{}, testObj)
					Expect(err).NotTo(HaveOccurred())
				})

				It("should have metadata", func() {
					items, err := client.List(ex.Args{AVU: true}, testObj)
					Expect(err).NotTo(HaveOccurred())

					Expect(len(items)).To(Equal(1))
					metadata := items[0].IAVUs

					Expect(metadata).To(Equal([]ex.AVU{
						{Attr: "test_attr_a", Value: "1"},
						{Attr: "test_attr_b", Value: "2"},
						{Attr: "test_attr_c", Value: "3"}}))
				})
			})

			It("should have ACLs if requested", func() {
				items, err := client.List(ex.Args{ACL: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(items)).To(Equal(1))
				acls := items[0].IACLs
				Expect(acls).To(Equal([]ex.ACL{{Owner: "irods",
					Level: "own", Zone: "testZone"}}))
			})
		})

		Context("a single item is requested", func() {
			It("should return a RodsItem with that path and name", func() {
				item, err := client.ListItem(ex.Args{}, testObj)
				Expect(err).NotTo(HaveOccurred())
				Expect(item).To(Equal(testObj))
			})
		})
	})

})

var _ = Describe("Put a file into iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		existingObject ex.RodsItem
		testObj        ex.RodsItem
		testChecksum   = "1181c1834012245d785120e3505ed169"
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoPut")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		objDir, err := filepath.Abs("testdata/1/reads/fast5")
		Expect(err).NotTo(HaveOccurred())

		coll := filepath.Join(workColl, "testdata/1/reads/fast5")
		existingObject = ex.RodsItem{IDirectory: objDir,
			IFile: "reads1.fast5",
			IPath: coll, IName: "reads1.fast5"}

		testObj = ex.RodsItem{IDirectory: objDir,
			IFile: "reads1.fast5",
			IPath: workColl, IName: "reads99.fast5"}
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a new data object is put into iRODS", func() {
		It("should be present in iRODS afterwards", func() {
			items, err := client.Put(ex.Args{}, testObj)
			Expect(err).NotTo(HaveOccurred())

			item, err := client.ListItem(ex.Args{}, items[0])
			Expect(err).NotTo(HaveOccurred())

			Expect(item.IPath).To(Equal(testObj.IPath))
			Expect(item.IName).To(Equal(testObj.IName))
		})

		It("should not have a checksum by default", func() {
			items, err := client.Put(ex.Args{}, testObj)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(items)).To(Equal(1))

			checksum, err := client.ListChecksum(items[0])
			Expect(err).NotTo(HaveOccurred())
			Expect(checksum).To(Equal(""))
		})

		When("a checksum is requested", func() {
			It("should have a checksum", func() {
				_, err := client.Put(ex.Args{Checksum: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				checksum, err := client.ListChecksum(testObj)
				Expect(err).NotTo(HaveOccurred())
				Expect(checksum).To(Equal(testChecksum))
			})
		})
	})

	When("overwriting a data object", func() {
		It("should not raise an error", func() {
			items, err := client.Put(ex.Args{}, existingObject)
			Expect(err).NotTo(HaveOccurred())
			Expect(items[0].IPath).To(Equal(existingObject.IPath))
			Expect(items[0].IName).To(Equal(existingObject.IName))
		})
	})
})

var _ = Describe("Put a directory into iRODS", func() {
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
		workColl = tmpRodsPath(rootColl, "ExtendoPut")
	})

	When("a local directory is put into iRODS, without recursion", func() {
		It("should fail to create that collection", func() {
			testItem :=
				ex.RodsItem{IDirectory: "testdata", IPath: workColl}
			Expect(testItem.IsLocalDir()).To(BeTrue())
			Expect(testItem.IsCollection()).To(BeTrue())
			Expect(testItem.IsLocalFile()).To(BeFalse())
			Expect(testItem.IsDataObject()).To(BeFalse())

			// iRODS considers a non-recursive put to be a transfer of
			// a file to a data object. A local file does not exist
			// (just a local directory), so iRODS' client code raises
			// an error.
			_, err := client.Put(ex.Args{Recurse: false}, testItem)
			Expect(err).To(HaveOccurred())

			var expected int32 = -310000
			var code int32

			switch err := errors.Cause(err).(type) {
			case *ex.RodsError:
				code = err.Code()
			}

			Expect(code).To(Equal(expected))
		})
	})

	When("a local directory is put into iRODS, with recursion", func() {
		AfterEach(func() {
			err = removeTestData(workColl)
			Expect(err).NotTo(HaveOccurred())

			client.StopIgnoreError()
		})

		It("should create a tree, including data objects", func() {
			testItem := ex.RodsItem{IDirectory: "testdata", IPath: workColl}
			Expect(testItem.IsLocalDir()).To(BeTrue())
			Expect(testItem.IsCollection()).To(BeTrue())
			Expect(testItem.IsLocalFile()).To(BeFalse())
			Expect(testItem.IsDataObject()).To(BeFalse())

			items, err := client.Put(ex.Args{Recurse: true}, testItem)
			Expect(err).NotTo(HaveOccurred())

			var expected []ex.RodsItem
			for _, filePath := range filePaths {
				objPath := filepath.Join(workColl, filePath)
				expected = append(expected,
					ex.RodsItem{
						IDirectory: filepath.Dir(filePath),
						IFile:      filepath.Base(filePath),
						IPath:      filepath.Dir(objPath),
						IName:      filepath.Base(objPath),
					},
				)
			}

			Expect(items).To(Equal(expected))
		})
	})
})

var _ = Describe("Calculate a data object checksum", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		testObj      ex.RodsItem
		testChecksum = "1181c1834012245d785120e3505ed169"
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoPut")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		objDir, err := filepath.Abs("testdata/1/reads/fast5")
		Expect(err).NotTo(HaveOccurred())

		testObj = ex.RodsItem{IDirectory: objDir,
			IFile: "reads1.fast5",
			IPath: workColl, IName: "reads99.fast5"}
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("a data object has no checksum", func() {
		It("should have a checksum afterwards", func() {
			_, err := client.Put(ex.Args{}, testObj)
			Expect(err).NotTo(HaveOccurred())

			checksum, err := client.ListChecksum(testObj)
			Expect(err).NotTo(HaveOccurred())
			Expect(checksum).To(Equal(""))

			_, err = client.Checksum(ex.Args{}, testObj)
			Expect(err).NotTo(HaveOccurred())

			checksum, err = client.ListChecksum(testObj)
			Expect(err).NotTo(HaveOccurred())
			Expect(checksum).To(Equal(testChecksum))
		})
	})
})

var _ = Describe("Add access permissions", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		testColl ex.RodsItem
		testObj  ex.RodsItem

		publicRead = ex.ACL{Owner: "public", Level: "read", Zone: "testZone"}
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoChmod")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		testColl = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata")}
		testObj = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata/1/reads/fast5"),
			IName: "reads1.fast5"}
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("setting permissions on collections", func() {
		When("adding access for a group", func() {
			It("should add an ACL", func() {
				item, err := client.ListItem(ex.Args{ACL: true}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).NotTo(ContainElement(publicRead))

				testColl.IACLs = []ex.ACL{publicRead}
				_, err = client.Chmod(ex.Args{}, testColl)
				Expect(err).NotTo(HaveOccurred())

				item, err = client.ListItem(ex.Args{ACL: true}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).To(ContainElement(publicRead))
			})
		})
	})

	Context("setting permissions on data objects", func() {
		When("adding access for a group", func() {
			It("should add an ACL", func() {
				item, err := client.ListItem(ex.Args{ACL: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).NotTo(ContainElement(publicRead))

				testObj.IACLs = []ex.ACL{publicRead}
				_, err = client.Chmod(ex.Args{}, testObj)
				Expect(err).NotTo(HaveOccurred())

				item, err = client.ListItem(ex.Args{ACL: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).To(ContainElement(publicRead))
			})
		})
	})
})

var _ = Describe("Remove access permissions", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		testColl ex.RodsItem
		testObj  ex.RodsItem

		publicRead = ex.ACL{Owner: "public", Level: "read", Zone: "testZone"}
		publicNull = ex.ACL{Owner: "public", Level: "null", Zone: "testZone"}
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoChmod")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		testColl = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata")}
		testColl.IACLs = []ex.ACL{publicRead}
		_, err = client.Chmod(ex.Args{}, testColl)
		Expect(err).NotTo(HaveOccurred())

		testObj = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata/1/reads/fast5"),
			IName: "reads1.fast5"}
		testObj.IACLs = []ex.ACL{publicRead}
		_, err = client.Chmod(ex.Args{}, testObj)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("setting permissions on collections", func() {
		When("removing access for a group", func() {
			It("should remove an ACL", func() {
				item, err := client.ListItem(ex.Args{ACL: true}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).To(ContainElement(publicRead))

				testColl.IACLs = []ex.ACL{publicNull}
				_, err = client.Chmod(ex.Args{}, testColl)
				Expect(err).NotTo(HaveOccurred())

				item, err = client.ListItem(ex.Args{ACL: true}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).NotTo(ContainElement(publicRead))
			})
		})
	})

	Context("setting permissions on data objects", func() {
		When("removing access for a group", func() {
			It("should remove an ACL", func() {
				item, err := client.ListItem(ex.Args{ACL: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).To(ContainElement(publicRead))

				testObj.IACLs = []ex.ACL{publicNull}
				_, err = client.Chmod(ex.Args{}, testObj)
				Expect(err).NotTo(HaveOccurred())

				item, err = client.ListItem(ex.Args{ACL: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IACLs).NotTo(ContainElement(publicRead))
			})
		})
	})
})

var _ = Describe("Metadata query", func() {
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
		workColl = tmpRodsPath(rootColl, "ExtendoMeta")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	When("querying without specifying object or collection", func() {
		It("should raise an error", func() {
			var emptyArgs = ex.Args{}
			_, err := client.MetaQuery(emptyArgs,
				ex.RodsItem{IAVUs: []ex.AVU{{Attr: "test_attr_a", Value: "1"}}})
			Expect(err).To(HaveOccurred())

			Expect(err).To(MatchError("metaquery arguments must " +
				"specify Object and/or Collection targets; " +
				"neither were specified"))
		})
	})

	Context("querying collections", func() {
		BeforeEach(func() {
			testColl := ex.RodsItem{
				IPath: filepath.Join(workColl, "testdata")}
			testColl.IAVUs = []ex.AVU{{Attr: "test_attr_x", Value: "y"}}
			_, err = client.MetaAdd(ex.Args{}, testColl)
			Expect(err).NotTo(HaveOccurred())
		})

		When("a query is run", func() {
			It("should return collections", func() {
				items, err := client.MetaQuery(ex.Args{Collection: true},
					ex.RodsItem{IAVUs: []ex.AVU{{Attr: "test_attr_x", Value: "y"}}})
				Expect(err).NotTo(HaveOccurred())

				expected :=
					[]ex.RodsItem{{IPath: filepath.Join(workColl, "testdata")}}
				Expect(items).To(Equal(expected))
				Expect(items[0].IsCollection()).To(BeTrue())
			})
		})
	})

	Context("querying data objects", func() {
		BeforeEach(func() {
			testColl := ex.RodsItem{
				IPath: filepath.Join(workColl, "testdata")}
			items, err := client.List(ex.Args{Recurse: true}, testColl)
			Expect(err).NotTo(HaveOccurred())

			for _, item := range items {
				item.IAVUs = []ex.AVU{{Attr: "test_attr_a", Value: "1"}}
				_, err = client.MetaAdd(ex.Args{}, item)
			}
			Expect(err).NotTo(HaveOccurred())
		})

		When("a query is run", func() {
			It("should return data objects", func() {
				items, err := client.MetaQuery(ex.Args{Object: true},
					ex.RodsItem{IAVUs: []ex.AVU{{Attr: "test_attr_a", Value: "1"}}})
				Expect(err).NotTo(HaveOccurred())

				var expected []ex.RodsItem
				for _, filePath := range filePaths {
					objPath := filepath.Join(workColl, filePath)
					expected = append(expected,
						ex.RodsItem{
							IPath: filepath.Dir(objPath),
							IName: filepath.Base(objPath)},
					)
				}

				Expect(items).To(Equal(expected))
				for _, item := range items {
					Expect(item.IsDataObject()).To(BeTrue())
				}
			})
		})
	})
})

var _ = Describe("Add metadata", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		testColl ex.RodsItem
		testObj  ex.RodsItem

		newAVU = ex.AVU{Attr: "abcdefgh", Value: "1234567890"}
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoMeta")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		testColl = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata")}
		testObj = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata/1/reads/fast5"),
			IName: "reads1.fast5"}
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("adding metadata to collections", func() {
		When("adding an AVU", func() {
			It("should be added", func() {
				item, err := client.ListItem(ex.Args{AVU: true}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IAVUs).NotTo(ContainElement(newAVU))

				testColl.IAVUs = []ex.AVU{newAVU}
				_, err = client.MetaAdd(ex.Args{}, testColl)
				Expect(err).NotTo(HaveOccurred())

				item, err = client.ListItem(ex.Args{AVU: true}, testColl)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IAVUs).To(ContainElement(newAVU))
			})
		})
	})

	Context("adding metadata to data objects", func() {
		When("adding an AVU", func() {
			It("should be added", func() {
				item, err := client.ListItem(ex.Args{AVU: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IAVUs).NotTo(ContainElement(newAVU))

				testObj.IAVUs = []ex.AVU{newAVU}
				_, err = client.MetaAdd(ex.Args{}, testObj)
				Expect(err).NotTo(HaveOccurred())

				item, err = client.ListItem(ex.Args{AVU: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(item.IAVUs).To(ContainElement(newAVU))
			})
		})
	})
})

var _ = Describe("Replace metadata", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		testObj ex.RodsItem

		avuA0, avuA1, avuA2, avuB0, avuB1 ex.AVU
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoReplace")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		testObj = ex.RodsItem{
			IPath: filepath.Join(workColl, "testdata/1/reads/fast5"),
			IName: "reads1.fast5"}

		avuA0 = ex.MakeAVU("a", "0", "z")
		avuA1 = ex.MakeAVU("a", "1")
		avuA2 = ex.MakeAVU("a", "2", "z")

		avuB0 = ex.MakeAVU("b", "0", "z")
		avuB1 = ex.MakeAVU("b", "1", "z")

		testObj.IAVUs = []ex.AVU{avuA0, avuA1, avuA2, avuB0, avuB1}
		_, err := client.MetaAdd(ex.Args{}, testObj)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("replacing metadata on data objects", func() {
		When("it shares attributes with existing metadata", func() {
			It("should be added", func() {
				newAVU := ex.MakeAVU("a", "nvalue", "nunits")
				_, err := client.ReplaceAVUs(testObj, []ex.AVU{newAVU})
				Expect(err).NotTo(HaveOccurred())

				updated, err := client.ListItem(ex.Args{AVU: true}, testObj)
				Expect(err).NotTo(HaveOccurred())

				Expect(updated.IAVUs).To(Equal([]ex.AVU{newAVU, avuB0, avuB1}))
			})
		})
	})

})

var _ = Describe("Archive a file to iRODS", func() {
	var (
		client *ex.Client
		err    error

		rootColl string
		workColl string

		existingObject ex.RodsItem
		testObj        ex.RodsItem

		testChecksum = "1181c1834012245d785120e3505ed169"
		newChecksum  = "348bd3ce10ec00ecc29d31ec97cd5839"
	)

	BeforeEach(func() {
		client, err = ex.FindAndStart(batonArgs...)
		Expect(err).NotTo(HaveOccurred())

		rootColl = "/testZone/home/irods"
		workColl = tmpRodsPath(rootColl, "ExtendoPut")

		err = putTestData("testdata/", workColl)
		Expect(err).NotTo(HaveOccurred())

		objDir, err := filepath.Abs("testdata/1/reads/fast5")
		Expect(err).NotTo(HaveOccurred())
		objFile := "reads99.fast5"

		testObj = ex.RodsItem{IDirectory: objDir,
			IFile: "reads1.fast5",
			IPath: workColl, IName: objFile}

		coll := filepath.Join(workColl, "testdata/1/reads/fast5")
		existingObject = ex.RodsItem{IDirectory: objDir,
			IFile: "reads1.fast5",
			IPath: coll, IName: "reads1.fast5"}
	})

	AfterEach(func() {
		err = removeTestData(workColl)
		Expect(err).NotTo(HaveOccurred())

		client.StopIgnoreError()
	})

	Context("archiving a data object", func() {
		When("a new data object", func() {
			When("the testChecksum is matched", func() {
				It("should be present in iRODS afterwards", func() {
					item, err := client.Archive(ex.Args{}, testObj, testChecksum)
					Expect(err).NotTo(HaveOccurred())

					item, err = client.ListItem(ex.Args{}, item)
					Expect(err).NotTo(HaveOccurred())

					Expect(item.IPath).To(Equal(testObj.IPath))
					Expect(item.IName).To(Equal(testObj.IName))
				})

				It("should return the testChecksum", func() {
					item, err := client.Archive(ex.Args{}, testObj, testChecksum)
					Expect(err).NotTo(HaveOccurred())

					Expect(item.IChecksum).To(Equal(testChecksum))
				})
			})

			When("the testChecksum is mismatched", func() {
				It("archiving should fail", func() {
					dummyChecksum := "no_such_checksum"
					_, err := client.Archive(ex.Args{}, testObj, dummyChecksum)
					Expect(err).To(HaveOccurred())

					Expect(err).To(MatchError(fmt.Sprintf(
						"failed to archive %s: local checksum '%s' "+
							"did not match remote checksum '%s'",
						testObj.RodsPath(), dummyChecksum, testChecksum)))
				})
			})
		})

		When("overwriting a data object with a different file", func() {
			When("the data object has the new checksum", func() {
				It("should be present in iRODS afterwards", func() {
					// same data object, new local file (was reads1.fast5)
					existingObject.IFile = "reads2.fast5"
					_, err := client.Archive(ex.Args{},
						existingObject, newChecksum)
					Expect(err).NotTo(HaveOccurred())

					item, err := client.ListItem(ex.Args{}, existingObject)
					Expect(err).NotTo(HaveOccurred())

					Expect(item.RodsPath()).To(Equal(existingObject.RodsPath()))
				})

				It("should have the new checksum", func() {
					// same data object, new local file (was reads1.fast5)
					existingObject.IFile = "reads2.fast5"
					_, err := client.Archive(ex.Args{},
						existingObject, newChecksum)
					Expect(err).NotTo(HaveOccurred())

					checksum, err := client.ListChecksum(existingObject)
					Expect(err).NotTo(HaveOccurred())

					Expect(checksum).To(Equal(newChecksum))
				})

			})
		})
	})

	Context("archiving a collection", func() {
		It("should fail to archive", func() {
			coll := ex.RodsItem{IDirectory: "testdata", IPath: workColl}
			_, err := client.Archive(ex.Args{}, coll, "")
			Expect(err).To(HaveOccurred())

			Expect(err).To(MatchError(fmt.Sprintf("cannot archive %s "+
				"as it is not a file", coll.RodsPath())))
		})
	})
})

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

func tmpRodsPath(root string, prefix string) string {
	s := rand.NewSource(GinkgoRandomSeed())
	r := rand.New(s)
	d := fmt.Sprintf("%s.%d.%010d", prefix, os.Getpid(), r.Uint32())
	return filepath.Join(root, d)
}
