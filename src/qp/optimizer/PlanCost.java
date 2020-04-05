/**
 * This method calculates the cost of the generated plans
 * also estimates the statistics of the result relation
 **/

package qp.optimizer;

import qp.operators.Distinct;
import qp.operators.GroupBy;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
import qp.operators.Scan;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class PlanCost {

    int cost;
    int numtuple;

    /**
     * If buffers are not enough for a selected join
     * * then this plan is not feasible and return
     * * a cost of infinity
     **/
    boolean isFeasible;

    /**
     * Hashtable stores mapping from Attribute name to
     * * number of distinct values of that attribute
     **/
    HashMap<Attribute, Integer> ht;

    /**
     * PlanCost constructor
     */
    public PlanCost() {
        ht = new HashMap<>();
        cost = 0;
    }

    /**
     * Returns the cost of the plan
     **/
    public int getCost(Operator root) {
        isFeasible = true;
        numtuple = calculateCost(root);
        if (isFeasible) {
            return cost;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * Returns number of tuples in the root
     **/
    protected int calculateCost(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return getStatistics((Distinct) node);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return getStatistics((GroupBy) node);
        }
        return -1;
    }

    /**
     * Projection will not change any statistics
     * * No cost involved as done on the fly
     **/
    protected int getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * Get the cost of a distinct node.
     *
     * @param node the plan for Distinct Operator.
     * @return tuple number after DISTINCT.
     */
    private int getStatistics(Distinct node) {
        return calculate(node.getBase());
    }

    /**
     * Get the cost of a GROUPBY node.
     *
     * @param node the plan for GROUP_BY Operator.
     * @return tuple number after GROUPBY.
     */
    private int getStatistics(GroupBy node) {
        return calculate(node.getBase());
    }

    /**
     * Calculates the statistics and cost of join operation
     *
     * @param node is the plan for Join Operator.
     * @return the cost of the plan.
     **/
    protected int getStatistics(Join node) {
        int lefttuples = calculateCost(node.getLeft());
        int righttuples = calculateCost(node.getRight());

        if (!isFeasible) {
            return -1;
        }

        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();

        /** Get size of the tuple in output & correspondigly calculate
         / ** buffer capacity, i.e., number of tuples per page **/
        int tuplesize = node.getSchema().getTupleSize();
        int outcapacity = Math.max(1, Batch.getPageSize() / tuplesize);
        int leftuplesize = leftschema.getTupleSize();
        int leftcapacity = Math.max(1, Batch.getPageSize() / leftuplesize);
        int righttuplesize = rightschema.getTupleSize();
        int rightcapacity = Math.max(1, Batch.getPageSize() / righttuplesize);
        int leftpages = (int) Math.ceil(1.0 * lefttuples / leftcapacity);
        int rightpages = (int) Math.ceil(1.0 * righttuples / rightcapacity);

        double tuples = (double) lefttuples * righttuples;
        for (Condition con : node.getConditionList()) {
            Attribute leftjoinAttr = con.getLhs();
            Attribute rightjoinAttr = (Attribute) con.getRhs();
            int leftattrind = leftschema.indexOf(leftjoinAttr);
            int rightattrind = rightschema.indexOf(rightjoinAttr);
            leftjoinAttr = leftschema.getAttribute(leftattrind);
            rightjoinAttr = rightschema.getAttribute(rightattrind);

            /** Number of distinct values of left and right join attribute **/
            int leftattrdistn = ht.get(leftjoinAttr);
            int rightattrdistn = ht.get(rightjoinAttr);
            tuples /= (double) Math.max(leftattrdistn, rightattrdistn);
            int mindistinct = Math.min(leftattrdistn, rightattrdistn);
            ht.put(leftjoinAttr, mindistinct);
            ht.put(rightjoinAttr, mindistinct);
        }
        int outtuples = (int) Math.ceil(tuples);

        /** Calculate the cost of the operation **/
        int joinType = node.getJoinType();
        int numbuff = BufferManager.getBuffersPerJoin();
        int joincost;

        switch (joinType) {
            case JoinType.NESTEDJOIN:
                joincost = leftpages * rightpages;
                break;
            case JoinType.BLOCKNESTED:
                int leftBlocks = (int) Math.ceil(leftpages / (numbuff - 2));
                joincost = leftBlocks * rightpages;
                break;
            case JoinType.SORTMERGE:
                int numOfLeftSortedRuns = (int) Math.ceil(1.0 * leftpages / numbuff);
                int numOfLeftPasses = (int) Math.ceil(Math.log(numOfLeftSortedRuns) / Math.log(numbuff - 1)) + 1;
                int leftSortCost = 2 * leftpages * numOfLeftPasses;

                int numOfRightSortedRuns = (int) Math.ceil(1.0 * rightpages / numbuff);
                int numOfRightPasses = (int) Math.ceil(Math.log(numOfRightSortedRuns) / Math.log(numbuff - 1)) + 1;
                int rightSortCost = 2 * leftpages * numOfRightPasses;

                joincost = leftSortCost + rightSortCost + rightpages;
                break;
            default:
                joincost = 0;
                break;
        }

        cost = cost + joincost;
        return outtuples;
    }

    /**
     * Find number of incoming tuples, Using the selectivity find # of output tuples
     * * And statistics about the attributes
     * * Selection is performed on the fly, so no cost involved
     *
     * @param node the plan for Select Operator.
     * @return the cost of the plan.
     **/
    protected int getStatistics(Select node) {
        int intuples = calculateCost(node.getBase());
        if (!isFeasible) {
            return Integer.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        Attribute fullattr = schema.getAttribute(index);
        int exprtype = con.getExprType();

        /** Get number of distinct values of selection attributes **/
        int numdistinct = ht.get(fullattr);
        int outtuples;

        /** Calculate the number of tuples in result **/
        if (exprtype == Condition.EQUAL) {
            outtuples = (int) Math.ceil(1.0 * intuples / numdistinct);
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (int) Math.ceil(intuples - 1.0 * intuples / numdistinct);
        } else {
            outtuples = (int) Math.ceil(0.5 * intuples);
        }

        /** Modify the number of distinct values of each attribute
         ** Assuming the values are distributed uniformly along entire
         ** relation
         **/
        for (int i = 0; i < schema.getNumCols(); ++i) {
            Attribute attri = schema.getAttribute(i);
            int oldvalue = ht.get(attri);
            int newvalue = (int) Math.ceil(1.0 * outtuples / intuples * oldvalue);
            ht.put(attri, outtuples);
        }
        return outtuples;
    }

    /**
     * The statistics file <tablename>.stat to find the statistics
     * * about that table;
     * * This table contains number of tuples in the table
     * * number of distinct values of each attribute
     *
     * @param node the plan for Scan Operator.
     * @return the cost of the plan.
     **/
    protected int getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat";
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in readin first line of " + filename);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        String temp = tokenizer.nextToken();
        int numtuples = Integer.parseInt(temp);

        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + filename);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        for (int i = 0; i < numAttr; ++i) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            int distinctValues = Integer.valueOf(temp);
            ht.put(attr, distinctValues);
        }

        /** Number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        int pagesize = Math.max(Batch.getPageSize() / tuplesize, 1);
        int numpages = (int) Math.ceil(1.0 * numtuples / pagesize);

        cost = cost + numpages;

        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }
        return numtuples;
    }

    /**
     * Calculate the number of distinct tuples
     *
     * @param base
     * @return the number of distinct tuples
     */
    private int calculate(Operator base) {
        int inTupleNum = calculateCost(base);
        int inCapacity = Batch.getPageSize() / base.getSchema().getTupleSize();
        int inPageNum = (int) Math.ceil(1.0 * inTupleNum / inCapacity);
        int bufferNum = BufferManager.getBuffersPerJoin();
        int numOfSortedRuns = (int) Math.ceil(1.0 * inPageNum / bufferNum);
        int numOfPasses = (int) Math.ceil(Math.log(numOfSortedRuns) / Math.log(bufferNum - 1)) + 1;

        cost += 2 * inPageNum * numOfPasses;

        return inTupleNum;
    }

}











