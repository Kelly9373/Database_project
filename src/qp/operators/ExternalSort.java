package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

/**
 * External Sort Algorithm
 */
public class ExternalSort extends Operator {

    private final int numBuff;
    private final Operator base;
    private final int batchSize; // the number of tuple each page
    private final String sortID = UUID.randomUUID().toString();
    private final ArrayList<Integer> attrsIndex = new ArrayList<>(); // the attributes used for sorting
    private ObjectInputStream sortResult; // the input stream of sorting result
    private boolean eosResult = false; // Whether end of stream of sortResult is reached

    public ExternalSort(Operator base, ArrayList attrs, int numBuff) {
        super(OpType.SORT);
        this.base = base;
        this.numBuff = numBuff;
        this.schema = base.schema;
        this.batchSize = Batch.getPageSize() / schema.getTupleSize();

        for (int i = 0; i < attrs.size(); i++) {
            Attribute attribute = (Attribute) attrs.get(i);
            attrsIndex.add(schema.indexOf(attribute));
        }
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }
        int numOfSortedRuns = createSortedRuns(); // first phase: create sorted runs
        int resultNum =
                mergeSortedRuns(numOfSortedRuns, 1); // second phase: merge multiple sorted runs into one
        if (resultNum == 1) {
            return true;
        } else return false;
    }

    /**
     * create sorted runs and store them into disk.
     */
    private int createSortedRuns() {
        int numOfRuns = 0;
        Batch inputBatch = base.next();
        while (inputBatch != null) {
            ArrayList<Tuple> tuples = new ArrayList<>(); // all the tuples for this particular run.

            for (int i = 0; i < numBuff && inputBatch != null; i++) {
                tuples.addAll(inputBatch.getAll());
                if (i != numBuff - 1) { // if reach the last page, then don't read next
                    inputBatch = base.next();
                }
            }
            tuples.sort(this::compareTuples); // sort all tuples with in-memory sorting
            try {
                ObjectOutputStream stream =
                        new ObjectOutputStream(new FileOutputStream(generateFileName(0, numOfRuns)));
                for (Tuple tuple : tuples) {
                    stream.writeObject(tuple);
                }
                stream.close();
            } catch (IOException e) {
                System.out.printf("sort: cannot write sorted runs");
                System.exit(1);
            }

            inputBatch = base.next();
            numOfRuns += 1;
        }
        return numOfRuns;
    }

    /**
     * Merges sorted runs created.
     */
    private int mergeSortedRuns(int numOfRuns, int passNum) {
        if (numOfRuns <= 1) { // only one run left, merge complete already
            try {
                sortResult =
                        new ObjectInputStream(
                                new FileInputStream(generateFileName(passNum - 1, numOfRuns - 1)));
            } catch (IOException e) {
                System.out.printf("sort: cannot create sortResult stream");
            }
            return numOfRuns;
        }

        int outputCounter = 0;
        for (int start = 0; start < numOfRuns; start += numBuff - 1) {
            int end = Math.min(start + numBuff - 1, numOfRuns);
            try {
                mergeSubgroups(start, end, passNum, outputCounter);
            } catch (IOException e) {
                System.out.printf("sort: cannot mergeRuns");
                System.exit(1);
            } catch (ClassNotFoundException e) {
                System.out.printf("sort: class not found");
                System.exit(1);
            }
            outputCounter += 1;
        }
        return mergeSortedRuns(outputCounter, passNum + 1);
    }

    /**
     * Merge all the sorted runs from start to end
     */
    private void mergeSubgroups(int start, int end, int passNum, int outNum)
            throws IOException, ClassNotFoundException {
        Batch[] inputBatches = new Batch[end - start];
        boolean[] inputeos = new boolean[end - start];
        ObjectInputStream[] inStreams =
                new ObjectInputStream[end - start]; // input streams for each input sorted runs
        for (int i = start; i < end; i++) {
            ObjectInputStream inStream =
                    new ObjectInputStream(new FileInputStream(generateFileName(passNum - 1, i)));
            inStreams[i - start] = inStream;

            Batch inputBatch = new Batch(batchSize);
            while (!inputBatch.isFull()) {
                try {
                    Tuple data = (Tuple) inStream.readObject();
                    inputBatch.add(data);
                } catch (EOFException eof) {
                    break;
                }
            }
            inputBatches[i - start] = inputBatch;
        }

        PriorityQueue<TupleForSort> resultPriorityQueue =
                new PriorityQueue<>(batchSize, (o1, o2) -> compareTuples(o1.tuple, o2.tuple));
        ObjectOutputStream outStream =
                new ObjectOutputStream(new FileOutputStream(generateFileName(passNum, outNum)));
        for (int i = 0; i < end - start; i++) {
            Batch inputBatch = inputBatches[i];
            if (inputBatch == null || inputBatch.isEmpty()) {
                inputeos[i] = true;
                continue;
            }
            Tuple current = inputBatch.get(0);
            resultPriorityQueue.add(new TupleForSort(current, i, 0));
        }

        while (!resultPriorityQueue.isEmpty()) {
            TupleForSort outTuple = resultPriorityQueue.poll();//get the smallest element
            outStream.writeObject(outTuple.tuple);
            int nextTupleNum = outTuple.tupleNum + 1;
            int nextBatchNum = outTuple.sortedRunNum;

            if (nextTupleNum == batchSize) {//need to read next page from input buffer
                Batch inputBatch = new Batch(batchSize);
                while (!inputBatch.isFull()) {
                    try {
                        Tuple data = (Tuple) inStreams[nextBatchNum].readObject();
                        inputBatch.add(data);
                    } catch (EOFException eof) {
                        break;
                    }
                }
                inputBatches[nextBatchNum] = inputBatch;
                nextTupleNum = 0;
            }

            Batch inputBatch = inputBatches[nextBatchNum];
            if (inputBatch == null || inputBatch.size() <= nextTupleNum) {
                inputeos[nextBatchNum] = true;
                continue;
            }
            Tuple nextTuple = inputBatch.get(nextTupleNum);
            resultPriorityQueue.add(new TupleForSort(nextTuple, nextBatchNum, nextTupleNum));
        }

        for (ObjectInputStream inStream : inStreams) {
            inStream.close();
        }
        outStream.close();
    }

    /**
     * returns a page of sorted output tuples
     */
    @Override
    public Batch next() {
        if (eosResult) {
            close();
            return null;
        }

        Batch outputBatch = new Batch(batchSize);
        while (!outputBatch.isFull()) {
            try {
                Tuple data = (Tuple) sortResult.readObject();
                outputBatch.add(data);
            } catch (ClassNotFoundException cnf) {
                System.out.printf("sort: class not found");
                System.exit(1);
            } catch (EOFException EOF) {
                eosResult = true;
                return outputBatch;
            } catch (IOException e) {
                System.out.printf("sort: error reading from sortResult");
                System.exit(1);
            }
        }
        return outputBatch;
    }

    @Override
    public boolean close() {
        super.close();
        try {
            sortResult.close();
        } catch (IOException e) {
            System.out.printf("Sort: cannot close sortResult");
            return false;
        }
        return true;
    }

    private String generateFileName(int passNumber, int runNumber) {
        return "sort-" + sortID + "-" + passNumber + "-" + runNumber;
    }

    // compare two tuples in terms of attrIndex
    private int compareTuples(Tuple t1, Tuple t2) {
        for (int index : attrsIndex) {
            int result = Tuple.compareTuples(t1, t2, index);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}
