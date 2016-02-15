package simpledb;

import java.io.*;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

	//public static Page[] pages;
    public int m_numPages;
    Map<PageId, Page> m_pages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
		m_pages = new ConcurrentHashMap<PageId, Page>();
        m_numPages = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // we used to use an array for the buffer pool but it failed the ram test, kept code in comments
		/*for (Page p : this.pages) {
			if (p == null)
				continue;
			if (p.getId().equals(pid))
				return p;
		}
		for (int x = 0; x < this.pages.length; x++) {
            if (pages[x] == null) {
                pages[x] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
				return pages[x];
            }
		}
        
        // extend buffer pool array
        m_numPages *= 2;
        Page[] newArray = new Page[m_numPages];
        for(int i = 0; i < this.pages.length; i++) {
            newArray[i] = this.pages[i];
        }
        this.pages = newArray;
        return getPage(tid, pid, perm);*/
            
            Page p = m_pages.get(pid);
            if (p == null) {
                if (m_pages.size() >= m_numPages) {
                    evictPage();
                }
                p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                m_pages.put(pid, p);
            }
            return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here - d?
        // not necessary for lab1 -- do this one
            Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here -d
        // not necessary for lab1 -- do this one
            Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here - done
        // not necessary for lab1 -- do last
        for (PageId p : m_pages.keySet()) {
            flushPage(p);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here - done
        // not necessary for lab1 -- do this one
        try {
            HeapPage page = (HeapPage)m_pages.get(pid);
            if (page.isDirty() != null)
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // we used to use an array for the buffer pool but it failed the ram test, kept code in comments
    /*private synchronized Page findPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1 -- do this one
        for (Page p : this.pages) {
            if (p == null)
                continue;
            if (p.getId().equals(pid))
                return p;
        }
        throw new IOException("page not found");
    }*/

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here - onde
        // not necessary for lab1 -- ? mayb
        if (m_numPages == 0)
            throw new DbException("no pages in buffer pool");
        
        // we used to use an array for the buffer pool but it failed the ram test, kept code in comments
        /*for (Page p : this.pages) { // find first marked dirty in buffer pool
            if (p.isDirty() == null) {
                try {
                    flushPage(p.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                p = null;
                break;
            }
        }*/
        
        for (PageId p : m_pages.keySet()) {
            if (m_pages.get(p).isDirty() == null) {
                try {
                    flushPage(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                m_pages.remove(p);
                break;
            }
        }
    }

}
