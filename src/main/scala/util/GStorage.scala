package util

import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage}

import java.io.IOException
import java.nio.file.Paths

object GStorage {

	/*
	main() {

		//val path = Paths.get(URI.create(OUTPUT_DIR + "/" + "out.csv"))
		//val path = Path.of("out.csv")
		//println("Path " + path.toAbsolutePath)
		//println("Path " + path.toFile.getAbsolutePath)

		//val out = new DataOutputStream( new FileOutputStream(path.toFile) )
		//out.writeChars("a,b,c\n")
		//out.close()


		//// Create a new GCS client
		//val storage = StorageOptions.newBuilder.setProjectId(projectId).build.getService
		//// The blob ID identifies the newly created blob, which consists of a bucket name and an object
		//// name
		//val blobId = BlobId.of(bucketName, objectName)
		//val blobInfo = BlobInfo.newBuilder(blobId).build
		//// The filepath on our local machine that we want to upload
		//val filePath = objectName
		//// upload the file and print the status
		//storage.createFrom(blobInfo, Paths.get(filePath))
		//System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName)
		}
	 */



	// upload file to GCS  // upload file to GCS
	@throws[IOException]
	def uploadFile(storage: Storage, bucket_name: String, remote_filename: String, local_filepath: String): Unit = {
		// The blob ID identifies the newly created blob, which consists of a bucket name and an object
		// name
		val blobId = BlobId.of(bucket_name, remote_filename)
		val blobInfo = BlobInfo.newBuilder(blobId).build
		// upload the file and print the status
		storage.createFrom(blobInfo, Paths.get(local_filepath))
		System.out.println("File " + local_filepath + " uploaded to bucket " + bucket_name + " as " + remote_filename)
	}

	//@throws[IOException]
	def deleteFile(storage: Storage, bucket_name: String, remote_filename: String): Unit = {
		System.out.println("Deleting file " + remote_filename + " from bucket " + bucket_name)
		val blob = getFileBlob(storage, bucket_name, remote_filename)
		// delete the file and print the status
		if (blob != null && blob.exists()) {
			blob.delete()
			System.out.println("File " + remote_filename + " deleted from bucket " + bucket_name)
		} else {
			System.out.println("File doesnt exist: " + remote_filename + " deleted from bucket " + bucket_name)
		}
	}

	// download file from GCS  // download file from GCS
	@throws[IOException]
	def downloadFile(storage: Storage, bucket_name: String, remote_filename: String, local_filepath: String): Unit = {
		val blobId = BlobId.of(bucket_name, remote_filename)
		val blob = storage.get(blobId)
		// download the file and print the status
		blob.downloadTo(Paths.get(local_filepath))
		System.out.println("File " + remote_filename + " downloaded to " + local_filepath)
	}


	// list all files in a folder or bucket
	@throws[IOException]
	def listFiles(storage: Storage, bucket_name: String, remote_filename: String) {

		System.out.println("Files in bucket " + bucket_name + ":");
		// list all the blobs in the bucket
		val blobs = storage.list(bucket_name, BlobListOption.currentDirectory(), BlobListOption.prefix(""))
		val blobIterator = blobs.iterateAll().iterator()

		while (blobIterator.hasNext) {
			val blob: Blob = blobIterator.next()
			println(blob.getName + " " + blob.getBlobId + " " + blob.toString)
		}
	}

	// list all files in a folder or bucket
	@throws[IOException]
	def getFileBlob(storage: Storage, bucket_name: String, remote_filepath: String): Blob = {

		val blobs = storage.list(bucket_name, BlobListOption.currentDirectory(), BlobListOption.prefix(""))
		val blobIterator = blobs.iterateAll().iterator()

		while (blobIterator.hasNext) {
			val blob: Blob = blobIterator.next()
			if (blob.getBlobId.getBucket.equals(bucket_name) && blob.getBlobId.getName.equals(remote_filepath))
				return blob
		}
		null
	}

	// read contents of the file
	@throws[IOException]
	def readFile(storage: Storage, bucket_name: String, remote_filename: String): Unit = {
		val blob = getFileBlob(storage, bucket_name, remote_filename)
		// read the contents of the file and print it
		val contents = new String(blob.getContent())
		System.out.println("Contents of file " + remote_filename + ": " + contents)
	}

}
