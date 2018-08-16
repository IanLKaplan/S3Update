package s3update;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
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
    private final static String USER_MD5_HASH = "userMetaData";
    private Logger log = Logger.getLogger(getClass().getName());
    
    /**
     * <p>
     * S3ContentType
     * </p>
     * <p>
     * The media content for the file downloaded to Amazon S3
     * </p>
     * <p>
     * Mar 14, 2015
     * </p>
     * 
     * @author Ian Kaplan, iank@bearcave.com
     */
    public enum S3ContentType {
        TEXT("text/html"),
        GIF("image/gif"),   // GIF image; Defined in RFC 2045 and RFC 2046
        JPEG("image/jpeg"), // JPEG JFIF image; Defined in RFC 2045 and RFC 2046
        PNG("image/png"),   // Portable Network Graphics; Registered,[13] Defined in RFC 2083
        TIFF("image/tiff"); // TIF image;
        
        private final String contentType;
        private S3ContentType( final String type ) {
            contentType = type;
        }
        public String getType() { return contentType; }
    }
    
    /**
     * Return the content type for a file. By default the content type is "text/html"
     * 
     * @param fileName the path to the file
     * @return the HTTP content type
     */
    private String findContentType( String fileName ) {
        String contentType = S3ContentType.TEXT.getType();
        if (fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")) {
            contentType = S3ContentType.JPEG.getType();
        } else if (fileName.endsWith(".png")) {
            contentType = S3ContentType.PNG.getType();
        } else if (fileName.endsWith(".tiff")) {
            contentType = S3ContentType.TIFF.getType();
        } else if (fileName.endsWith(".gif")) {
            contentType = S3ContentType.GIF.getType();
        }
        return contentType;
    }
		
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
	
	
	public static String calculateMD5Hash(InputStream stream, String path, Logger log) {
        String md5 = "";
        try {
            md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex( stream );
        } catch (IOException e) {
            log.error("Error calculating md5 hash for " + path );
        }
        return md5;
    }
    
    /**
     * Calculate the MD5 cryptographic hash for a file on the local computer.
     * @param file
     * @return
     */
    public static String calculateMD5Hash(File file, Logger log ) {
        String md5 = "";
        FileInputStream fis = null;
        try {
            fis = new FileInputStream( file );
            md5 = calculateMD5Hash( fis, file.getPath(), log );
        } catch (IOException e) {
            log.error("Error referencing file " + file.getPath() );
        }
        finally {
            if (fis != null) {
                try { fis.close(); } catch (IOException e) {}
            }
        }
        return md5;
    }

    
    /**
     * <p>
     * Return the MD5 hash value for the file, stored in as user metadata in the S3 ObjectMetadata.
     * </p>
     * <p>
     * Files stored in S3 have an ETag value, which may (or may not be) an MD5 hash value. To avoid having
     * to deal with the ETag value and it's definition, the code in this application that writes files to
     * S3 calculates the MD5 hash and stores it in the user metadata.
     * </p>
     * @param s3Path
     * @return
     */
    public String getS3FileMD5Hash( String s3Path ) {
        String md5Hash = "";
        try {
            ObjectMetadata s3metaData = getS3Client().getObjectMetadata(getBucketName(), s3Path );
            if (s3metaData != null) {
                String s3Hash = s3metaData.getUserMetaDataOf( USER_MD5_HASH);
                if (s3Hash != null && s3Hash.length() > 0) {
                    md5Hash = s3Hash;
                }
            }
        } catch (AmazonClientException e) {
            log.error("Error reading metadata for S3 path " + s3Path + ": " + e.getLocalizedMessage());
        }
        return md5Hash;
    }
    
    /**
     * <p>
     * Write an InputStream to an S3 bucket.
     * </p>
     * <p>
     * Setting the content type is important. If the type of an html file is not set to "text/html" the file will not be
     * served properly. Instead it will be treated as a downloadable file. 
     * </p>
     * <p>
     * The MD5 hash value for the file is stored in the user metadata for the S3 file. This avoids issues with calculating
     * the S3 ETag value for the file.
     * </p>
     * 
     * @param s3Key the "path" and file name for the object (e.g., /foo/bar/mySelfie.jpg)
     * @param istream an InputStream for the object to be written to S3
     * @param numBytes the size of the object, in bytes
     * @param contentType the HTML content type.
     * @param md5Hash the MD5 hash value for the source (local file) being written to S3
     * @return true if the hash of the S3 object is the same as the hash calculated from the input stream. False if there
     *         was a write failure or if the hash does not match.
     * @throws AmazonClientException 
     * @throws NoSuchAlgorithmException 
     */
    public boolean writeStream(String s3Key, InputStream istream, long numBytes, String contentType, String md5Hash ) throws AmazonClientException {
        boolean hashOK = false;
        // Calculate the MD5 hash as the data is written
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength( numBytes );
        metadata.setContentType( contentType );
        HashMap<String, String> userMetaData = new HashMap<String, String>();
        userMetaData.put(USER_MD5_HASH, md5Hash);
        metadata.setUserMetadata(userMetaData);
        PutObjectRequest putRequest = new PutObjectRequest( getBucketName(), s3Key, istream, metadata );
        PutObjectResult rslt = getS3Client().putObject( putRequest );
        // get the MD5 hash that was calculated for this write (put) transaction
        String s3MD5HashBase64 = rslt.getContentMd5();
        byte[] s3Bytes = Base64.decode(s3MD5HashBase64);
        String s3MD5Hash = Hex.encodeHexString(s3Bytes);
        if (! md5Hash.equals( s3MD5Hash )) {
            log.error(this.getClass().getName() + "::writeStream: error writing to S3 storage for " + s3Key);
        }
		return hashOK;
    }
	
	
	/**
	 * <p>
	 * Write a file from the local system to an S3 path (key).
	 * </p> 
	 * @param s3Key The S3 path (key) that the file should be written to.
	 * @param outFile A File object for the file that will be written to S3
	 * @param md5Hash the MD5 hash for the file being written to S3
	 * @return true if the write operation succeeded. False otherwise.
	 * @throws AmazonClientException
	 * @throws FileNotFoundException
	 */
    public boolean writeFile(String s3Key, File outFile, String md5Hash ) throws AmazonClientException, FileNotFoundException {
        // if a hash is not provided, calcuate the hash
        if (md5Hash == null || md5Hash.length() == 0) {
            md5Hash = calculateMD5Hash( outFile, log);
        }
        InputStream istream = new FileInputStream( outFile );
        long length = outFile.length();
        boolean hashOK = false;
		try {
		    String contentType = findContentType( outFile.getPath() );
			hashOK = writeStream(s3Key, istream, length, contentType, md5Hash );
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
     * Return an input stream associated with an S3 object (e.g., a text file or an image).
     * 
     * @param s3Key the S3 path and file name for the object (e.g., /foo/bar/imnaked.jpg)
     * @return an InputStream associated with the object.
     * @throws AmazonClientException 
     */
    public InputStream s3ToInputStream(String s3Key) throws AmazonClientException {
        GetObjectRequest objRequest = new GetObjectRequest( getBucketName(), s3Key);
        S3Object s3Obj = getS3Client().getObject( objRequest );
        InputStream istream = s3Obj.getObjectContent();
        return istream;
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
