/** \file
 * 
 * Aug 17, 2018
 *
 * Copyright Ian Kaplan 2018
 *
 * @author Ian Kaplan, www.bearcave.com, iank@bearcave.com
 */
package s3update;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;

/**
 * <h4>
 * Update
 * </h4>
 * <p>
 * Copy or update a file on Amazon Web Services S3 storage.
 * </p>
 * <p>
 * If the file does not exist on S3, the file is copied from the local system.
 * </p>
 * <p>
 * If the file does exist, the MD5 check some on the S3 file is checked against the local MD5 check sum.
 * The MD5 check sum is stored in the user meta-data of the S3 object.
 * </p>
 * <p>
 * Aug 17, 2018
 * </p>
 * 
 * @author Ian Kaplan, iank@bearcave.com
 */
public class Update {
    private final S3Service mService;
    private Logger log = Logger.getLogger(getClass().getName());
    
    /**
     * @param service
     */
    public Update(final S3Service service) {
        this.mService = service;
    }
    
    /**
     * @param file
     * @param s3Path
     */
    public void update(final File file, final String s3Path) {
        boolean copyLocalFile = false;
        String sourceMD5 = "";
        if (mService.pathExists(s3Path)) {
            // calculate the MD5 hash for the local file
            sourceMD5 = S3Service.calculateMD5Hash( file, log );
            // get the MD5 hash for the S3 file from the user metadata
            String s3MD5 = mService.getS3FileMD5Hash(s3Path);
            if (s3MD5.length() > 0) {
                copyLocalFile = (! sourceMD5.equals( s3MD5 ));
            } else {
                // write the file out with MD5 metadata
                copyLocalFile = true;
            }
        } else { // the object doesn't exist on S3
            copyLocalFile = true;
        }        
        if (copyLocalFile) {
            // write the local file to S3 on the path s3Path
            try {
                log.info("Copying " + file.getPath() );
                mService.writeFile(s3Path, file, sourceMD5 );
            } catch (AmazonClientException | FileNotFoundException e) {
                log.error("Error writing file to S3 path " + s3Path);
            }
        }
    }
}
