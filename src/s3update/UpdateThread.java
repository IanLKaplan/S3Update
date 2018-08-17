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

import org.apache.commons.lang3.tuple.Pair;

/**
 * <h4>
 * UpdateThread
 * </h4>
 * <p>
 * A thread that removes a File/s3 path pair from a thread safe list and
 * calls the S3 update code.
 * </p>
 * <p>
 * Aug 17, 2018
 * </p>
 * 
 * @author Ian Kaplan, iank@bearcave.com
 */
public class UpdateThread implements Runnable {
    private final SynchronizedList<Pair<File, String>> mList;
    private final Update mUpdate;
    
    /**
     * @param list
     * @param update
     */
    public UpdateThread(SynchronizedList<Pair<File, String>> list, Update update) {
        this.mList = list;
        this.mUpdate = update;
    }

    @Override
    public void run() {
        Pair<File, String> pair = mList.get();
        while (pair != null) {
            File file = pair.getLeft();
            String s3Path = pair.getRight();
            mUpdate.update(file, s3Path);
            pair = mList.get();
        }
    }

}
