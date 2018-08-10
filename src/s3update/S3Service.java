package s3update;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.Base64;



/**
 * <p>
 * S3Service
 * </p>
 * <p>
 * Provide basic read and write operations for AWS S3 objects.
 * </p>
 * <p>
 * This class is instantiated for a particular S3 bucket (the top level name in the S3 path).
 * </p>
 * <p>
 * This code is based on the S3Service code that was originally written for the nderground (www.nderground.net)
 * social network.
 * </p>
 *  
 * @author Ian Kaplan, Topstone Software (www.topstonesoftware.com), iank@bearcave.com
 */
class S3Service implements IS3Keys {
    private final static Regions REGIONS = Regions.US_WEST_1;
    private Logger log = Logger.getLogger(getClass().getName());
		
    // The AmazonS3Client is thread safe. In the AWS S3 Forum an Amazon engineer commented:
    // "All the clients in the AWS SDK for Java are thread safe, and you're encouraged to reuse client 
    // objects as much as possible since they manage resources like connection pools, etc." 
    private static AmazonS3 s3client = null;
    
    private final String S3_BucketName;

    /**
     * 
     * @param s3BucketName the bucket name is the root name for the S3 path. On the AWS S3 page these are the names
     * that are listed in the S3 table.
     */
    public S3Service(String s3BucketName ) {
        this.S3_BucketName = s3BucketName;
    }


	public String getBucketName() {
        return S3_BucketName;
    }
	
	
	protected synchronized AmazonS3 getS3Client() {
		if (s3client == null) {
		    AWSCredentials credentials = new BasicAWSCredentials( S3_ID, S3_KEY );
			AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);
			ClientConfiguration config = new ClientConfiguration();
			config.setProtocol(Protocol.HTTP);
			s3client = AmazonS3Client.builder().withCredentials(provider).withClientConfiguration(config).withRegion(REGIONS).build();
		}
		return s3client;
	}

    
    /**
     * Write an InputStream to an S3 bucket.
     * 
     * @param s3Key the "path" and file name for the object (e.g., /foo/bar/mySelfie.jpg)
     * @param istream an InputStream for the object to be written to S3
     * @param numBytes the size of the object, in bytes
     * @param contentType the HTML content type.
     * @return true if the hash of the S3 object is the same as the hash calculated from the input stream. False if there
     *         was a write failure or if the hash does not match.
     * @throws AmazonClientException 
     * @throws NoSuchAlgorithmException 
     */
	public boolean writeStream(String s3Key, InputStream istream, long numBytes ) throws AmazonClientException {
		boolean hashOK = false;
		// Calculate the MD5 hash as the data is written
		DigestInputStream distream = null;
		try {
		    MessageDigest md = MessageDigest.getInstance("MD5");
		    distream = new DigestInputStream(istream, md);
		    ObjectMetadata metadata = new ObjectMetadata();
		    metadata.setContentLength( numBytes );
		    PutObjectRequest putRequest = new PutObjectRequest( getBucketName(), s3Key, distream, metadata );
		    PutObjectResult rslt = getS3Client().putObject( putRequest );
		    String md5Hash = rslt.getContentMd5();
		    byte[] s3Digest = md.digest();
		    String s3MD5 = Base64.encodeAsString( s3Digest );
		    hashOK = md5Hash.equals(s3MD5);
		    if (! hashOK) {
		        log.error(this.getClass().getName() + "::writeStream: error writing to S3 storage for " + s3Key);
		    }
		} catch (NoSuchAlgorithmException e) {
		    log.error("Error allocating an MD5 message digest: " + e.getLocalizedMessage() );
        }
		finally {
		    if (distream != null) {
		        try { distream.close(); } catch (IOException e) {}
		    }
		}
		return hashOK;
    }
	
	
	/**
	 * <p>
	 * Write a file from the local system to an S3 path (key).
	 * </p> 
	 * @param s3Key The S3 path (key) that the file should be written to.
	 * @param outFile A File object for the file that will be written to S3
	 * @return true if the write operation succeeded. False otherwise.
	 * @throws AmazonClientException
	 * @throws FileNotFoundException
	 */
    public boolean writeFile(String s3Key, File outFile ) throws AmazonClientException, FileNotFoundException {
        InputStream istream = new FileInputStream( outFile );
        long length = outFile.length();
        boolean hashOK = false;
		try {
			hashOK = writeStream(s3Key, istream, length );
		}
		finally {
			if (istream != null) {
				try { istream.close(); } catch(IOException e) {}
			}
		}
        return hashOK;
    }
    
    
	
	/**
	 * <p>
	 * Return true if the S3 path exists, false otherwise
	 * </p>
	 * <p>
	 * This function uses the (at the time) new S3 client method doesObjectExist which is, presumably,
	 * a very efficient way to do this query.
	 * </p>
	 * @param s3Path
	 * @return true if the S3 path (key) exists, false otherwise.
	 */
	public boolean pathExists( String s3Path ) {
		boolean exists = false;
		if (s3Path != null) {
			try {
				exists = getS3Client().doesObjectExist(getBucketName(), s3Path );
			}
			catch(AmazonS3Exception e) {
				log.error(this.getClass().getName() + "::pathExists error: " + e.getLocalizedMessage() );
			}
		}
		return exists;
	}
	
	/**
	 * <p>
	 * Return the MD5 hash for a file stored in AWS S3.
	 * </p>
	 * @param s3Path the S3 path (key) for the object
	 * @return the MD5 hash for the object.
	 */
	public String getMD5Hash(String s3Path) {
	    String md5Hash = null; 
	    try {
	        ObjectMetadata metaData = getS3Client().getObjectMetadata( getBucketName(),  s3Path );
	        if (metaData != null) {
	            md5Hash = metaData.getContentMD5();
	        }
	    }
	    catch (AmazonClientException e) {
	        log.error("Error obtaining metadata for the S3 object on the path " + s3Path + ": " + e.getLocalizedMessage() );
	    }
	    return md5Hash;
	}
	
	
	/**
	 * Check that the bucket that the S3Service object was created for actually exists.
	 * 
	 * @return true if the bucket exists, false otherwise.
	 */
	public boolean s3BucketExist() {
	    boolean bucketExists = false;
	    try {
	        bucketExists = getS3Client().doesBucketExistV2( getBucketName() );
	    }
	    catch (AmazonClientException e) {
	        // Well, that didn't work...
	    }
	    return bucketExists;
	}
        
}
