package qp.operators;

import qp.utils.*;

import java.util.*;

/**
 * GroupBy Operator used to group data according to attrs
 */
public class GroupBy extends Operator {

    private Operator base;
    private int batchSize; // number of tuples per page
    private int numBuff;
    private final ArrayList attrs;
    private ArrayList<Integer> attrsIndex = new ArrayList<>();
    private ExternalSort externalSort;
    private Batch inputBatch = null;
    private int currIndex = 0;
    private boolean eos = false;
    private Tuple prevTuple = null;

    public GroupBy(Operator base, ArrayList attrs) {
        super(OpType.DISTINCT);
        this.base = base;
        this.attrs = attrs;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    @Override
    public boolean open() {
        batchSize = Batch.getPageSize() / schema.getTupleSize();
        for (int i = 0; i < attrs.size(); i++) {
            Attribute attribute = (Attribute) attrs.get(i);
            attrsIndex.add(schema.indexOf(attribute));
        }
        externalSort = new ExternalSort(base, attrs, numBuff);
        return externalSort.open();
    }

    @Override
    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if (inputBatch == null) {
            inputBatch = externalSort.next();
        }

        Batch outputBatch = new Batch(batchSize);
        while (!outputBatch.isFull()) {
            if (inputBatch == null || currIndex >= inputBatch.size()) {
                eos = true;
                break;
            }
            Tuple current = inputBatch.get(currIndex);
            if (prevTuple == null || !isEqual(prevTuple, current)) {
                outputBatch.add(current);
                prevTuple = current;
            }
            currIndex += 1;
            if (currIndex == batchSize) {
                inputBatch = externalSort.next();
                currIndex = 0;
            }
        }
        return outputBatch;
    }

    private boolean isEqual(Tuple t1, Tuple t2) {
        for (int index : attrsIndex) {
            int result = Tuple.compareTuples(t1, t2, index);
            if (result != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean close() {
        return externalSort.close();
    }

    @Override
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        ArrayList<Attribute> newAttrs = new ArrayList<>();
        for (int i = 0; i < attrs.size(); i++) {
            Attribute attribute = (Attribute) ((Attribute) attrs.get(i)).clone();
            newAttrs.add(attribute);
        }
        Distinct newDistinct = new Distinct(newBase, newAttrs);
        newDistinct.setSchema(newBase.getSchema());
        return newDistinct;
    }
}

