package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

	private TransactionId t;
	private DbIterator child;

	private TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
		this.t = t;
		this.child = child;

		this.td = new TupleDesc(
			new Type[] { Type.INT_TYPE },
			new String[] { "" }
		);
    }

    public TupleDesc getTupleDesc() {
		return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
		child.open();
		super.open();
    }

    public void close() {
		super.close();
		child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if (!child.hasNext())
			return null;

		BufferPool bPool = Database.getBufferPool();

		int tuplesDeleted = 0;

		while (child.hasNext()) {
			Tuple tup = child.next();

			try {
				bPool.deleteTuple(t, tup);
			} catch(Exception e) {
				throw new DbException("IOException in buffer pool deletion");
			}

			tuplesDeleted++;
		}

		Tuple tup = new Tuple(this.td);

		tup.setField(0, new IntField(tuplesDeleted));

		return tup;
    }

    @Override
    public DbIterator[] getChildren() {
		return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
		if (this.child != children[0])
			this.child = children[0];
    }

}
