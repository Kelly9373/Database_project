package qp.optimizer;

import qp.operators.Operator;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

public class SimulatedAnnealing extends RandomOptimizer {
    // When current temperature is lower than END_TEMPERATURE, minCost should be reached
    private static final double END_TEMPERATURE = 1;
    // To decrement the temperature each time
    private static final double ALPHA = 0.85;
    // The parameter used to get initial temperature
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
        /** get an initial plan for the given sql query **/
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();

        Operator initPlan = rip.prepareInitialPlan();
        Operator minPlan = initPlan;

        int initCost;
        int minCost = printPlanCost("Initial Plan", minPlan);

        /** NUMITER is number of times random restart **/
        int NUMITER;
        if (numJoin != 0) {
            NUMITER = 2 * numJoin;
        } else {
            // Exit if there is no join in the query
            printPlanCost("Final Plan", minPlan);
            return minPlan;
        }

        /**
         * Randomly restart the gradient descent until
         * the maximum specified number of random restarts (NUMITER)
         * has satisfied
         */
        for (int j = 0; j < NUMITER; ++j) {
            // Prepare a random initial plan for more randomness each loop, except the first round
            if (j != 0) {
                initPlan = rip.prepareInitialPlan();
                RandomOptimizer.modifySchema(initPlan);
                minPlan = initPlan;
                minCost = printPlanCost("Initial Plan", minPlan);
            }

            // Loop until the current temperature is lower than END_TEMPERATURE
            for (double temperature = minCost * INIT_TEMP_PARAM; temperature > END_TEMPERATURE; temperature *= ALPHA) {
                initPlan = minPlan;
                initCost = minCost;

                // Break if reach equilibrium
                // We set equilibrium equals to 10 * numJoin here
                for (int i = 0; i < 10 * numJoin; i++) {
                    Operator initPlanCopy = (Operator) initPlan.clone();
                    Operator neighborPlan = getNeighbor(initPlanCopy);
                    int neighborCost = printPlanCost("Neighbor", neighborPlan);

                    if (neighborCost <= initCost || ifAccept(temperature, neighborCost, initCost)) {
                        initPlan = neighborPlan;
                        initCost = neighborCost;
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
     * @param temperature  current annealing temperature
     * @param neighborCost current cost
     * @param initCost     initial cost
     * @return return true if this move is accepted
     */
    private boolean ifAccept(double temperature, int neighborCost, int initCost) {
        int delta = Math.abs(neighborCost - initCost);
        double probability = Math.exp(-delta / temperature);
        return RandNumb.randDouble() < probability;
    }

}
