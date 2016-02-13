package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private int numberOfFieldsAveraged;

	private TupleDesc aggTupleDesc;
	private ArrayList<Tuple> tuplelist;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
	 *            INT_TYPE(), STRING_TYPE()
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
	 *            MIN, MAX, SUM, AVG, COUNT
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;

		this.numberOfFieldsAveraged = 0;
		
		tuplelist = new ArrayList<Tuple>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
		
		boolean grouping = false;

		if (gbfield != NO_GROUPING)
			grouping = true;

		int aggValueIndex = 0;

		if (grouping)
			aggValueIndex = 1;

		// if we don't have an aggregate tuple yet, establish one
		if (tuplelist.size() == 0) {
			TupleDesc aggTupleDesc;
			Type[] typeArr;
			String[] fieldArr;

			if (grouping) {
				//[group by value, aggregate value]
				typeArr = new Type[] { gbfieldtype, Type.INT_TYPE};
				fieldArr = new String[] { tup.getTupleDesc().getFieldName(gbfield), what.toString() };
			} else { //no grouping, so our tuple is just { aggregateVal }
				typeArr = new Type[] { Type.INT_TYPE};
				fieldArr = new String[] { what.toString() };
			}

			this.aggTupleDesc = new TupleDesc( typeArr, fieldArr );

			Tuple aggTuple = new Tuple(this.aggTupleDesc);


			switch(what) {
				case COUNT:
					aggTuple.setField(aggValueIndex, new IntField(1));
					break;
				case AVG:
					numberOfFieldsAveraged = 1;
				case MIN: 
				case MAX:
				case SUM:
					aggTuple.setField(aggValueIndex, tup.getField(afield));
			}

			if (grouping)
				aggTuple.setField(0, tup.getField(gbfield));

			tuplelist.add(aggTuple);

		} else { //the list has already been seeded with an aggregate tuple

			Tuple tupleToManipulate = null;

			if (grouping) {
				boolean tupleFound = false;

				for (Tuple curTuple : tuplelist) {
					if (tup.getField(gbfield).compare(Predicate.Op.EQUALS, curTuple.getField(0))) {
						tupleToManipulate = curTuple;
						tupleFound = true;
						break;
					}
				}
					
				//if we did not find a matching group by aggregate we create a new one
				if (!tupleFound) {
					Tuple aggTuple = new Tuple(aggTupleDesc);

					switch(what) {
						case COUNT:
							aggTuple.setField(aggValueIndex, new IntField(1));
							break;
						case AVG:
							numberOfFieldsAveraged = 1;
						case MIN: 
						case MAX:
						case SUM:
							aggTuple.setField(aggValueIndex, tup.getField(afield));
							break;
					}

					aggTuple.setField(0, tup.getField(gbfield));

					tuplelist.add(aggTuple);

					return;
			
				}

			} else { //we are not grouping, so we'll only have 1 tuple, which is the first
				tupleToManipulate = tuplelist.get(0);
			}
			
			IntField otherTupAggField = (IntField) tup.getField(afield);
			IntField aggTupAggField = (IntField) tupleToManipulate.getField(aggValueIndex);
			
			switch(what) {
				case MIN:
					
					if (otherTupAggField.getValue() < aggTupAggField.getValue())
						tupleToManipulate.setField(aggValueIndex, otherTupAggField);
					break;
				case MAX:
					if (otherTupAggField.getValue() > aggTupAggField.getValue())
						tupleToManipulate.setField(aggValueIndex, otherTupAggField);
					break;
				case SUM:
					tupleToManipulate.setField(
						aggValueIndex,
						new IntField(
							otherTupAggField.getValue() +
							aggTupAggField.getValue()
						)
					);
					break;
				case AVG:
					tupleToManipulate.setField(
						aggValueIndex,
						new IntField(
							(aggTupAggField.getValue() * numberOfFieldsAveraged +
							otherTupAggField.getValue()) / (numberOfFieldsAveraged + 1)
						)
					);
					numberOfFieldsAveraged++;
					break;
				case COUNT: 
					tupleToManipulate.setField(aggValueIndex, 
						new IntField(
							aggTupAggField.getValue() + 1
						)
					);
					break;
			}
		}
	}

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
		return new TupleIterator(
			this.aggTupleDesc,
			tuplelist
		);
    }

}
