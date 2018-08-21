/** \file
 * 
 * Aug 8, 2018
 *
 * Copyright Ian Kaplan 2018
 *
 * @author Ian Kaplan, www.bearcave.com, iank@bearcave.com
 */
package s3update;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;

/**
 * <h4>S3Update</h4>
 * <p>
 * Update an Amazon Web Services S3 "directory tree" from a local directory
 * tree. If a file exists in the S3 tree but does not match the local file
 * (hased on the cryptographic hash) the file will be copied from the local
 * system.
 * </p>
 * <p>
 * The phrase "directory tree" is in quotes because S3 files don't really exist
 * in directories. Rather they exist in buckets with names that may have "/"
 * separators. These path names are treated as if they were directory paths, but
 * they are not actual directory paths in S3.
 * </p>
 * <p>
 * When updating an S3 tree from the local computer file system, the S3 bucket name and the
 * directory name for the local directory tree should be the same. For example, if there is a
 * local directory tree whose top level directory is /home/iank/topstonesoftware.com, the S3 bucket name
 * should be topstonesoftware.com.
 * </p>
 * <p>
 * When the directory trees on the local computer matches the S3 (logical) tree, the hash value
 * for the S3 file is checked against the hash value for the local file. If they do not match
 * the local file is copied to S3. 
 * </p>
 * <p>
 * The MD5 hash value for the S3 file is stored in the user metadata for the file.
 * </p>
 * <p>
 * S3 is a REST service accessed via HTTP. HTTP is relatively slow and there is a lot of waiting
 * for the HTTP transaction to complete. To make this application faster, it is multi-threaded
 * so that multiple HTTP threads can be active at the same time.
 * </p>
 * <p>
 * Aug 8, 2018
 * </p>
 * 
 * @author Ian Kaplan, Topstone Software (www.topstonesoftware.com), iank@bearcave.com
 */
public class S3Update {
    private static final int NUM_THREADS = 32;
    private Logger log = Logger.getLogger(getClass().getName());
    private String pathPrefix = "";
    private S3Service s3Service = null;
    private Update update = null;
    private ArrayList<Pair<File, String>> updateList = new ArrayList<Pair<File, String>>();
    
    protected void usage() {
        System.err.println("usage: " + this.getClass().getName() + "<source dir> <s3 root path>");
    }
    
    
    protected boolean gitPath(File file) {
        boolean isGitPath = false;
        String path = file.getPath();
        int ix = path.lastIndexOf( File.separatorChar );
        if (ix >= 0) {
            path = path.substring(ix+1);
        }
        if (path.length() > 0) {
            isGitPath = path.equals(".git");
        }
        return isGitPath;
    }


    /**
     * <p>
     * Traverse the directory tree and build a list of (File, s3 path) pairs.
     * </p>
     * <p>
     * The match is determined on the basis of the MD5 hash.
     * </p>
     * @param sourcePath
     * @param s3Root
     */
    protected void traverseDirTree(File file) {
        if (file.exists()) {
            if (file.canRead()) {
                if (file.isDirectory()) {
                    if (! gitPath( file )) {
                        File[] fileList = file.listFiles();
                        for (File fileElem : fileList ) {
                            // Replace back-slash characters for Windows file systems.
                            traverseDirTree( fileElem );
                        }
                    }
                } else { // it's not a directory, so presumably it's a file
                    // A hack to convert Windows paths to slash separated paths
                    String s3Path = file.getPath().replace('\\', '/');
                    if (! s3Path.endsWith("~")) {  // don't copy emacs temp files
                        if (pathPrefix.length() > 0) {
                            s3Path = s3Path.substring( pathPrefix.length());
                            if (s3Path.startsWith("/")) {
                                s3Path = s3Path.substring(1);
                            }
                        }
                        Pair<File, String> pair = new ImmutablePair<File, String>(file, s3Path);
                        updateList.add(pair);
                    }
                }
            } else {
                log.error("Cannot read path " + file.getPath() );
            }
        } else {
            log.error("The path " + file.getPath() + " does not exist");
        }
    }
    

    protected void application( String[] args ) {
        if (args.length == 2) {
            String sourceDir = args[0];
            String s3Root = args[1];
            int offset = sourceDir.indexOf(s3Root);
            if (offset >= 0) {
                if (offset > 0) {
                    pathPrefix = sourceDir.substring(0, offset + s3Root.length());
                }
                s3Service = new S3Service( s3Root );
                if (s3Service.s3BucketExist()) {
                    update = new Update( s3Service );
                    File file = new File(sourceDir);
                    traverseDirTree(file);
                    SynchronizedList<Pair<File, String>> queue = new SynchronizedList<Pair<File, String>>( updateList );
                    ExecutorService pool = Executors.newFixedThreadPool( NUM_THREADS );
                    for (int i = 0; i < NUM_THREADS; i++) {
                        pool.execute(new UpdateThread(queue, update));
                    }
                    pool.shutdown();
                    while (! pool.isTerminated() ) {
                    }
                } else {
                    log.error("There is no S3 bucket with the name " + s3Root );
                }
            } else {
                log.error("The source directory path must contain the S3 root name " + s3Root );
            }
        } else {
            usage();
        }        
    }

    /**
     * <p>
     * The main() function for the S3Update application. The function takes two arguments. The first argument is
     * the source directory on the local system that the files should be copied from. The second argument is the
     * S3 root "directory" (bucket name). The S3 root directory should be a prefix in the source directory path.
     * </p>
     * <p>
     * For example, there is a directory tree on the local computer system
     * </p>
     * <pre>
     * /home/iank/topstonesoftware.com
     * </pre>
     * <p>
     * In S3 there is a corresponding root, topstonesoftware.com. So the arguments will be
     * </p>
     * <ol>
     * <li>/home/iank/topstonesoftware.com</li>
     * <li>topstonesoftware.com</li>
     * </ol>
     * <p>
     * Note that in the source you can also have a subdirectory. For example:
     * </p>
     * <pre>
     *   /home/iank/topstonesoftware.com/galleries topstonesoftware.com
     * </pre>
     * @param args [path on local system] [S3 bucket name]
     */
    public static void main(String[] args) {
        // Initialize the Apache Log4j logger
        org.apache.log4j.BasicConfigurator.configure();
        S3Update app = new S3Update();
        app.application( args );
    }

}
