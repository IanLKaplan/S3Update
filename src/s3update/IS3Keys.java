/** \file
 * 
 * Aug 8, 2018
 *
 * Copyright Ian Kaplan 2018
 *
 * @author Ian Kaplan, www.bearcave.com, iank@bearcave.com
 */
package s3update;

/**
 * <h4>
 * IS3Keys
 * </h4>
 * <p>
 * An interface that defines the Amazon Web Services S3 access ID and secret key. The permissions for these keys
 * should be limited to read/write access to S3.
 * </p>
 * <p>
 * Aug 8, 2018
 * </p>
 * 
 * @author Ian Kaplan, Topstone Software (www.topstonesoftware.com), iank@bearcave.com
 * 
 */
public interface IS3Keys {
    public final String S3_ID = "Your S3 read/write ID here";
    public final String S3_KEY = "Your S3 read/write secret key here";
}
