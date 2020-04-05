package qp.optimizer;

import qp.operators.Operator;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

public class SimulatedAnnealing extends RandomOptimizer {
    private static final double END_TEMPERATURE = 1;
    private static final double ALPHA = 0.8;
    private final double INIT_TEMP_PARAM = 2;

    /**
     * Constructor
     *
     * @param sqlQuery the SQL query
     */
    public SimulatedAnnealing(SQLQuery sqlQuery) {
        super(sqlQuery);
    }

    /**
     * Implement the simulated annealing algorithm
     *
     * @return the optimized plan
     */
    @Override
    public Operator getOptimizedPlan() {
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();

        Operator initPlan = rip.prepareInitialPlan();
        Operator minPlan = initPlan;

        int initCost;
        int minCost = printPlanCost("Initial Plan", minPlan);

        /** NUMITER is number of times random restart **/
        int NUMITER;
        if (numJoin !=0) {
            NUMITER = 2 * numJoin;
        } else {
            printPlanCost("Final Plan", minPlan);
            return minPlan;
        }

        /**
         * Randomly restart the gradient descent until
         * the maximum specified number of random restarts (NUMITER)
         * has satisfied
         */
        for (int j = 0; j < NUMITER; ++j) {
            if (j != 0) {
                initPlan = rip.prepareInitialPlan();
                RandomOptimizer.modifySchema(initPlan);
                minPlan = initPlan;
                minCost = printPlanCost("Initial Plan", minPlan);
            }

            for (double temperature = minCost * INIT_TEMP_PARAM; temperature > END_TEMPERATURE; temperature *= ALPHA) {
                initPlan = minPlan;
                initCost = minCost;

                for (int i = 0; i < 12 * numJoin; i++) {
                    Operator initPlanCopy = (Operator) initPlan.clone();
                    Operator currentPlan = getNeighbor(initPlanCopy);
                    int currentCost = printPlanCost("Neighbor", currentPlan);

                    if (currentCost <= initCost || ifAccept(temperature, currentCost, initCost)) {
                        initPlan = currentPlan;
                        initCost = currentCost;
                    }
                }

                printPlanCost("Local Minimum", initPlan, initCost);
                if (initCost < minCost) {
                    minPlan = initPlan;
                    minCost = initCost;
                }
            }
        }

        printPlanCost("Final Plan", minPlan, minCost);
        return minPlan;
    }

    /**
     * Whether accept this uphill move
     *
     * @param temperature current annealing temperature
     * @param currentCost current cost
     * @param initCost initial cost
     * @return return true if this move is accepted
     */
    private boolean ifAccept(double temperature, int currentCost, int initCost) {
        int delta = Math.abs(currentCost - initCost);
        double prob = Math.exp(-delta / temperature);
        return RandNumb.randDouble() < prob;
    }

}
