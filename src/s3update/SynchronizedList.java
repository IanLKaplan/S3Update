/** \file
 * 
 * Aug 17, 2018
 *
 * Copyright Ian Kaplan 2018
 *
 * @author Ian Kaplan, www.bearcave.com, iank@bearcave.com
 */
package s3update;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;

import java.io.File;

/**
 * <h4>
 * SynchronizedList
 * </h4>
 * <p>
 * A simple synchronized list designed for a multi-thread application that only
 * removes elements from the list.
 * </p>
 * <p>
 * Aug 17, 2018
 * </p>
 * 
 * @author Ian Kaplan, iank@bearcave.com
 * @param <T> The type of the list element
 */
public class SynchronizedList<T> {
    private final ArrayList<T> mList;
    
    /**
     * @param list 
     */
    public SynchronizedList(ArrayList<T> list) {
        if (list != null) {
            this.mList = list;
        }
        else throw( new IllegalArgumentException("SynchronizedList passed a null value for the list"));
    }
    
    /**
     * Remove an element from the front of the queue. If the queue is empty, return null.
     * 
     * @return the element at the front of the queue.
     * @throws InterruptedException 
     */
    public synchronized T get() {
        T elem = null;
        if (! mList.isEmpty()) {
            elem = mList.remove(0);
        }
        return elem;
    }
    
}
