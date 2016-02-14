package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    private File file;
    private TupleDesc tupleDesc;
    private int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here - done
        file = f;
        tupleDesc = td;
        id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here - done
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here - done
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here - done
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here - done?
        HeapPage ret = null;
        try {
            RandomAccessFile randomAccess = new RandomAccessFile(file, "r");
            int offset = pid.pageNumber() * 4096;
            randomAccess.seek(offset);
            byte[] page = new byte[4096];
            randomAccess.read(page, 0, 4096);
            randomAccess.close();
            
            HeapPageId id = (HeapPageId) pid;
            ret = new HeapPage(id, page);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here -d
        // not necessary for lab1 - nice - damn
        RandomAccessFile randomAccess = new RandomAccessFile(file, "rw");
        int offset = page.getId().pageNumber() * 4096;
        byte[] pageData = page.getPageData();
        randomAccess.seek(offset);
        randomAccess.write(pageData);
        
        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here - done
        return (int)(file.length() / 4096);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here - ?
        // not necessary for lab1 - nice
                ArrayList<Page> pagesList = new ArrayList<Page>();
                HeapPageId pageid;
                HeapPage page;
                for (int i = 0; i < numPages(); i++) { // find empty slot
                    pageid = new HeapPageId(getId(), i);
                    page = (HeapPage)Database.getBufferPool().getPage(tid, pageid, Permissions.READ_WRITE);
                    if (page.getNumEmptySlots() > 0) {
                        page.insertTuple(t);
                        page.markDirty(true, tid);
                        pagesList.add(page);
                        return pagesList;
                    }
                }
                pageid = new HeapPageId(getId(), numPages());
                page = new HeapPage(pageid, HeapPage.createEmptyPageData());
                writePage(page); // no empty slots found
                page = (HeapPage)Database.getBufferPool().getPage(tid, pageid, Permissions.READ_WRITE);
                page.insertTuple(t);
                pagesList.add(page);
                return pagesList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1 - nice
                ArrayList<Page> deleteList = new ArrayList<Page>();
                if (t.getRecordId() != null && t.getRecordId().getPageId().getTableId() == getId()) {
                    HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
                    page.deleteTuple(t);
                    page.markDirty(true, tid);
                    deleteList.add(page);
                    return deleteList;
                }
                
                throw new DbException("tuple cannot be deleted");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here - done
        return new HeapFileIterator(this, tid);
    }
    
    private class HeapFileIterator implements DbFileIterator {
        
        private HeapFile file;
        private TransactionId tid;
        private int index;
        private Iterator<Tuple> itrTuple;
        
        public HeapFileIterator(HeapFile hf, TransactionId t) {
            file = hf;
            tid = t;
            index = 0;
        }
        
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId hpid = new HeapPageId(file.getId(), index);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
            itrTuple = page.iterator();
        }
        
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (itrTuple == null)
                return false;
            if (itrTuple.hasNext())
                return true;
            
            if (++index < file.numPages()) {
                open();
                return itrTuple.hasNext();
            }
            return false;
        }
        
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext())
                return itrTuple.next();
            throw new NoSuchElementException();
        }
        
        public void close() {
            itrTuple = null;
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }
    }
}