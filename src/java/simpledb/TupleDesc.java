package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    //list of TDItems that compose the Tuple Descriptor
    public List<TDItem> itemList = new ArrayList<TDItem>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public boolean equals(Object o) {
			if (!(o instanceof TDItem))
				return false;

			TDItem other = (TDItem)o;

            if (this.fieldType == other.fieldType && this.fieldName == other.fieldName)
                return true;
            return false;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return itemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length != fieldAr.length)
            System.out.println("Type and field-name arrays don't have matching lengths.");

        for (int i = 0; i < typeAr.length; i++) {
            this.itemList.add(
                new TDItem(typeAr[i], fieldAr[i])
            );
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for (int i = 0; i < typeAr.length; i++) {
            this.itemList.add(
                new TDItem(typeAr[i], "")
            );
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return itemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return itemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return itemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
		if (name == null)
			throw new NoSuchElementException();

        Iterator<TDItem> it = this.iterator();
        int index = 0;

        while(it.hasNext()) {
			String curName = it.next().fieldName;
				
            if (name.equals(curName))
                return index;
            index++;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
		Iterator<TDItem> it = this.iterator();

		int bytes = 0;
		
		while (it.hasNext()) {
			TDItem curItem = it.next();
			bytes += curItem.fieldType.getLen();
		}

        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		Iterator<TDItem> listIt1 = td1.iterator();
		Iterator<TDItem> listIt2 = td2.iterator();

		int listSize = td1.numFields() + td2.numFields();

		Type[] typeArr = new Type[listSize];
		String[] fieldArr = new String[listSize];

		int curIndex = 0;
		
		while (listIt1.hasNext()) {
			TDItem curItem = listIt1.next();
			typeArr[curIndex] = curItem.fieldType;
			fieldArr[curIndex] = curItem.fieldName;
			curIndex++;
		}

		while(listIt2.hasNext()) {
			TDItem curItem = listIt2.next();
			typeArr[curIndex] = curItem.fieldType;
			fieldArr[curIndex] = curItem.fieldName;
			curIndex++;
		}
		
		return new TupleDesc(typeArr, fieldArr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc))
            return false;

		TupleDesc other = (TupleDesc)o;

		if (this.getSize() != other.getSize())
			return false;

        if (this.numFields() != other.numFields())
            return false;

		Iterator<TDItem> it = this.iterator();
		Iterator<TDItem> otherIt = other.iterator();

        while (it.hasNext() && otherIt.hasNext()) {
            if (!(it.next().equals(otherIt.next())))
                return false;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
		Iterator<TDItem> it = this.iterator();

		String str = "";

		while (it.hasNext()) {
			if (str != "")
				str += ", ";

			str += it.next().toString();
		}

        return str;
    }
}
