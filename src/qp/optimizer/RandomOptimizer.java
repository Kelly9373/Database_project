/**
 * performs randomized optimization, iterative improvement algorithm
 **/

package qp.optimizer;

import qp.operators.BlockNestedJoin;
import qp.operators.Debug;
import qp.operators.Distinct;
import qp.operators.GroupBy;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.NestedJoin;
import qp.operators.Project;
import qp.operators.Select;
import qp.operators.ExternalSort;
import qp.operators.SortMergeJoin;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

import java.util.ArrayList;

public abstract class RandomOptimizer {

    /**
     * enumeration of different ways to find the neighbor plan
     **/
    public static final int METHODCHOICE = 0;  // Selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE = 1;   // By rearranging the operators by commutative rule
    public static final int ASSOCIATIVE = 2;   // Rearranging the operators by associative rule

    /**
     * Number of altenative methods available for a node as specified above
     **/
    public static final int NUMCHOICES = 3;

    SQLQuery sqlquery;  // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;        // Number of joins in this query plan

    /**
     * constructor
     **/

    public RandomOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }

    /**
     * To be more intuitive, this method is changed to abstract and will be used in SimulatedAnnealing classes
     *
     * @return optimal plan
     */
    public abstract Operator getOptimizedPlan();

    /**
     * After finding a choice of method for each operator
     * * prepare an execution plan by replacing the methods with
     * * corresponding join operator implementation
     **/
    public static Operator makeExecPlan(Operator node) {
        int numbuff = BufferManager.getBuffersPerJoin();
        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            switch (joinType) {
                case JoinType.NESTEDJOIN:
                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    nj.setLimit(node.getLimit());
                    nj.setOffset(node.getOffset());
                    return nj;
                case JoinType.BLOCKNESTED:
                    BlockNestedJoin bnj = new BlockNestedJoin((Join) node);
                    bnj.setLeft(left);
                    bnj.setRight(right);
                    bnj.setNumBuff(numbuff);
                    bnj.setLimit(node.getLimit());
                    bnj.setOffset(node.getOffset());
                    return bnj;
                case JoinType.SORTMERGE:
                    SortMergeJoin smj = new SortMergeJoin((Join) node);

                    ArrayList<Attribute> leftAttrs = new ArrayList<>();
                    leftAttrs.add(smj.getCondition().getLhs());
                    smj.setLeft(new ExternalSort(left, leftAttrs, numbuff));

                    ArrayList<Attribute> rightAttrs = new ArrayList<>();
                    rightAttrs.add((Attribute) smj.getCondition().getRhs());
                    smj.setRight(new ExternalSort(right, rightAttrs, numbuff));

                    smj.setNumBuff(numbuff);
                    smj.setLimit(node.getLimit());
                    smj.setOffset(node.getOffset());
                    return smj;
                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.DISTINCT) {
            Distinct operator = (Distinct) node;
            operator.setNumBuff(numbuff);
            Operator base = makeExecPlan(operator.getBase());
            operator.setBase(base);
            return node;
        } else if (node.getOpType() == OpType.GROUPBY) {
            GroupBy operator = (GroupBy) node;
            operator.setNumBuff(numbuff);
            Operator base = makeExecPlan(operator.getBase());
            operator.setBase(base);
            return node;
        } else {
            return node;
        }
    }

    /**
     * Randomly selects a neighbour
     **/
    protected Operator getNeighbor(Operator root) {
        // Randomly select a node to be altered to get the neighbour
        int nodeNum = RandNumb.randInt(0, numJoin - 1);
        // Randomly select type of alteration: Change Method/Associative/Commutative
        int changeType = RandNumb.randInt(0, NUMCHOICES - 1);
        Operator neighbor = null;
        switch (changeType) {
            case METHODCHOICE:   // Select a neighbour by changing the method type
                neighbor = neighborMeth(root, nodeNum);
                break;
            case COMMUTATIVE:
                neighbor = neighborCommut(root, nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor = neighborAssoc(root, nodeNum);
                break;
        }
        return neighbor;
    }

    /**
     * Selects a random method choice for join wiht number joinNum
     * *  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     * * returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum) {
        System.out.println("------------------neighbor by method change----------------");
        int numJMeth = JoinType.numJoinTypes();
        if (numJMeth > 1) {
            /** find the node that is to be altered **/
            Join node = (Join) findNodeAt(root, joinNum);
            int prevJoinMeth = node.getJoinType();
            int joinMeth = RandNumb.randInt(0, numJMeth - 1);
            while (joinMeth == prevJoinMeth) {
                joinMeth = RandNumb.randInt(0, numJMeth - 1);
            }
            node.setJoinType(joinMeth);
        }
        return root;
    }

    /**
     * Applies join Commutativity for the join numbered with joinNum
     * *  e.g.,  A X B  is changed as B X A
     * * returns the modifies plan
     **/
    protected Operator neighborCommut(Operator root, int joinNum) {
        System.out.println("------------------neighbor by commutative---------------");
        /** find the node to be altered**/
        Join node = (Join) findNodeAt(root, joinNum);
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        node.getCondition().flip();
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum
     * *  e.g., (A X B) X C is changed to A X (B X C)
     * *  returns the modifies plan
     **/
    protected Operator neighborAssoc(Operator root, int joinNum) {
        /** find the node to be altered**/
        Join op = (Join) findNodeAt(root, joinNum);
        Operator left = op.getLeft();
        Operator right = op.getRight();

        if (left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN) {
            transformLefttoRight(op, (Join) left);
        } else if (left.getOpType() != OpType.JOIN && right.getOpType() == OpType.JOIN) {
            transformRighttoLeft(op, (Join) right);
        } else if (left.getOpType() == OpType.JOIN && right.getOpType() == OpType.JOIN) {
            if (RandNumb.flipCoin())
                transformLefttoRight(op, (Join) left);
            else
                transformRighttoLeft(op, (Join) right);
        } else {
            // The join is just A X B,  therefore Association rule is not applicable
        }

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * This is given plan (A X B) X C
     **/
    protected void transformLefttoRight(Join op, Join left) {
        System.out.println("------------------Left to Right neighbor--------------");
        Operator right = op.getRight();
        Operator leftleft = left.getLeft();
        Operator leftright = left.getRight();
        Attribute leftAttr = op.getCondition().getLhs();
        Join temp;

        if (leftright.getSchema().contains(leftAttr)) {
            System.out.println("----------------CASE 1-----------------");
            /** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftright, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftleft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());

        } else {
            System.out.println("--------------------CASE 2---------------");
            /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
             ** a1b1,  a4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftleft, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftright);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newcond = left.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    protected void transformRighttoLeft(Join op, Join right) {
        System.out.println("------------------Right to Left Neighbor------------------");
        Operator left = op.getLeft();
        Operator rightleft = right.getLeft();
        Operator rightright = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRhs();
        Join temp;

        if (rightleft.getSchema().contains(rightAttr)) {
            System.out.println("----------------------CASE 3-----------------------");
            /** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightleft, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightright);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());
        } else {
            System.out.println("-----------------------------CASE 4-----------------");
            /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightright, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightleft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newcond = right.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    /**
     * This method traverses through the query plan and
     * * returns the node mentioned by joinNum
     **/
    protected Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join) node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp = findNodeAt(((Join) node).getLeft(), joinNum);
                if (temp == null)
                    temp = findNodeAt(((Join) node).getRight(), joinNum);
                return temp;
            }
        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            // if sort/project/select operator
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return findNodeAt(((Distinct) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return findNodeAt(((GroupBy) node).getBase(), joinNum);
        } else {
            return null;
        }
    }

    /**
     * Modifies the schema of operators which are modified due to selecting an alternative neighbor plan
     **/
    static void modifySchema(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            modifySchema(base);
            ArrayList attrlist = ((Project) node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        } else if (node.getOpType() == OpType.DISTINCT) {
            Operator base = ((Distinct) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.GROUPBY) {
            Operator base = ((GroupBy) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        }
    }

    /**
     * Prints out the information
     *
     * @param name name of the plan.
     * @param plan the execution plan.
     * @return the cost of the execution plan.
     */
    int printPlanCost(String name, Operator plan) {
        PlanCost planCost = new PlanCost();
        int cost = planCost.getCost(plan);
        printPlanCost(name, plan, cost);
        return cost;
    }

    /**
     * Prints out the information
     *
     * @param name name of the plan.
     * @param plan the execution plan.
     * @param cost the cost of the execution plan.
     */
    void printPlanCost(String name, Operator plan, int cost) {
        System.out.println("---------------------------" + name + "---------------------------");
        Debug.PPrint(plan);
        System.out.println(" " + cost);
    }
}
