package qp.optimizer;

import qp.operators.Operator;
import qp.utils.SQLQuery;

public class IterativeImprovement extends RandomOptimizer {
    /**
     * Constructor
     *
     * @param sqlQuery is the SQL query to be optimized.
     */
    public IterativeImprovement(SQLQuery sqlQuery) {
        super(sqlQuery);
    }

    /**
     * Implements iterative improvement algorithm
     *
     * @return an optimized plan
     */
    @Override
    public Operator getOptimizedPlan() {
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();
        Operator finalPlan = null;
        int finalCost = Integer.MAX_VALUE;
        int optimizationNum = 0;

        if (numJoin == 0) {
            finalPlan = rip.prepareInitialPlan();
            printPlanCost("Final Plan", finalPlan);
            return finalPlan;
        }

        for (int j = 0; j < 3 * numJoin && optimizationNum < 10; j++) {
            Operator initPlan = rip.prepareInitialPlan();
            RandomOptimizer.modifySchema(initPlan);
            int initCost = printPlanCost("Initial Plan", initPlan);
            boolean notMin = true;

            while (notMin) {
                Operator minNeighborPlan = initPlan;
                int minNeighborCost = initCost;
                System.out.println("---------------while---------------");

                for (int i = 0; i < 2 * numJoin; i++) {
                    Operator initPlanCopy = (Operator) initPlan.clone();
                    Operator neighbor = getNeighbor(initPlanCopy);
                    int neighborCost = printPlanCost("Neighbor", neighbor);

                    if (neighborCost < minNeighborCost) {
                        minNeighborPlan = neighbor;
                        minNeighborCost = neighborCost;
                    }
                }

                if (minNeighborCost < initCost) {
                    initPlan = minNeighborPlan;
                    initCost = minNeighborCost;
                } else {
                    notMin = false;
                }
            }

            printPlanCost("Local Minimum", initPlan, initCost);
            if (initCost < finalCost) {
                finalPlan = initPlan;
                finalCost = initCost;
                optimizationNum++;
            }
        }
        System.out.println("HERE4");

        printPlanCost("Final Plan from Iterative Improvement", finalPlan, finalCost);
        return finalPlan;
    }

}
