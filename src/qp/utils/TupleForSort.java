package qp.utils;

public class TupleForSort {

    public final Tuple tuple;
    public final int sortedRunNum;// the sorted run number
    public final int tupleNum;// the tuple number in its sorted run.

    public TupleForSort(Tuple tuple, int sortedRunNum, int tupleNum) {
        this.tuple = tuple;
        this.sortedRunNum = sortedRunNum;
        this.tupleNum = tupleNum;
    }
}