package qp.operators;

import qp.utils.*;

import java.util.*;

/**
 * Sort Merge Join Algorithm
 */
public class SortMergeJoin extends Join {

    private int batchsize; // the number of tuple each page
    private ArrayList<Integer> leftindex; // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex; // Indices of the join attributes in right table
    private Batch leftbatch; // Buffer page for left input stream
    private Batch rightbatch; // Buffer page for right input stream
    private Tuple lefttuple = null;
    private Tuple righttuple = null;

    private ArrayList<Tuple> rightpartition = new ArrayList<>();
    private int currrightindex = 0; // current right tuple index
    private Tuple nextrighttuple = null; // next right tuple

    private int lcurs = 0; // Cursor for left side buffer
    private int rcurs = 0; // Cursor for right side buffer
    private boolean eosl = false; // Whether end of stream (left table) is reached
    private boolean eosr = false; // Whether end of stream (right table) is reached

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    @Override
    public boolean open() {
        left.open();
        right.open();
        batchsize = Batch.getPageSize() / schema.getTupleSize();

        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        return super.open();
    }

    /**
     * returns a page of mergeSort output tuples
     */
    @Override
    public Batch next() {
        if (eosl || eosr) {
            close();
            return null;
        }

        if (leftbatch == null) {
            leftbatch = left.next();
            if (leftbatch == null) {
                eosl = true;
                return null;
            }
            lefttuple = getNextLeftTuple();
            if (lefttuple == null) {
                eosl = true;
                return null;
            }
        }
        if (rightbatch == null) {
            rightbatch = right.next();
            if (rightbatch == null) {
                eosr = true;
                return null;
            }
            rightpartition = getNextRightPartition();
            if (rightpartition.isEmpty()) {
                eosr = true;
                return null;
            }
            currrightindex = 0;
            righttuple = rightpartition.get(currrightindex);
        }

        Batch outputbatch = new Batch(batchsize);
        while (!outputbatch.isFull()) {
            int result = compareTuples(lefttuple, righttuple, leftindex, rightindex);
            if (result == 0) {
                outputbatch.add(lefttuple.joinWith(righttuple));

                if (currrightindex < rightpartition.size() - 1) { // scan next right tuple
                    currrightindex++;
                    righttuple = rightpartition.get(currrightindex);
                } else { // scan next left tuple
                    Tuple nextlefttuple = getNextLeftTuple();
                    if (nextlefttuple == null) {
                        eosl = true;
                        break;
                    }
                    result = compareTuples(lefttuple, nextlefttuple, leftindex, leftindex);
                    lefttuple = nextlefttuple;

                    if (result == 0) { // if the next left tuple is the same, then scan right partition again
                        currrightindex = 0;
                        righttuple = rightpartition.get(0);
                    } else { // move to next right partition
                        rightpartition = getNextRightPartition();
                        if (rightpartition.isEmpty()) {
                            eosr = true;
                            break;
                        }
                        currrightindex = 0;
                        righttuple = rightpartition.get(currrightindex);
                    }
                }
            } else if (result > 0) {
                rightpartition = getNextRightPartition();
                if (rightpartition.isEmpty()) {
                    eosr = true;
                    break;
                }
                currrightindex = 0;
                righttuple = rightpartition.get(currrightindex);

            } else {
                lefttuple = getNextLeftTuple();
                if (lefttuple == null) {
                    eosl = true;
                    break;
                }
            }
        }
        return outputbatch;
    }

    // compare two tuples in terms of attrIndex
    private int compareTuples(
            Tuple tuple1, Tuple tuple2, ArrayList<Integer> leftindex, ArrayList<Integer> rightindex) {
        for (int i = 0; i < leftindex.size(); ++i) {
            int result = Tuple.compareTuples(tuple1, tuple2, leftindex.get(i), rightindex.get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * Get next right partition
     */
    private ArrayList<Tuple> getNextRightPartition() {
        ArrayList<Tuple> rightpartition = new ArrayList<>();
        int result = 0;
        if (nextrighttuple == null) {
            nextrighttuple = getNextRightTuple();
            if (nextrighttuple == null) {
                return rightpartition;
            }
        }

        while (result == 0) {
            rightpartition.add(nextrighttuple);
            nextrighttuple = getNextRightTuple();
            if (nextrighttuple == null) {
                break;
            }
            result = compareTuples(rightpartition.get(0), nextrighttuple, rightindex, rightindex);
        }

        return rightpartition;
    }

    /**
     * Get the next tuple from right input page
     */
    private Tuple getNextRightTuple() {
        if (rightbatch == null) {
            return null;
        } else if (rcurs == rightbatch.size()) {
            rightbatch = right.next();
            rcurs = 0;
        }
        if (rightbatch == null || rcurs >= rightbatch.size()) {//no tuple left
            return null;
        }

        Tuple next = rightbatch.get(rcurs);
        rcurs += 1;
        return next;
    }

    /**
     * Get the next tuple from left input page
     */
    private Tuple getNextLeftTuple() {
        if (leftbatch == null) {
            eosl = true;
            return null;
        } else if (lcurs == leftbatch.size()) {
            leftbatch = left.next();
            lcurs = 0;
        }
        if (leftbatch == null || lcurs >= leftbatch.size()) { // no tuple left
            eosl = true;
            return null;
        }
        Tuple next = leftbatch.get(lcurs);
        lcurs += 1;
        return next;
    }

    @Override
    public boolean close() {
        right.close();
        left.close();
        return super.close();
    }
}
