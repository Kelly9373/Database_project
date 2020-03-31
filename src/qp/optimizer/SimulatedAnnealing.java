package qp.optimizer;

import qp.operators.Operator;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

public class SimulatedAnnealing extends RandomOptimizer {
    private static final double END_TEMPERATURE = 1;
    private static final double ALPHA = 0.85;

    private Operator initPlan;
    private final double initTempParam;

    /**
     * Constructor
     *
     * @param sqlQuery the SQL query
     */
    public SimulatedAnnealing(SQLQuery sqlQuery) {
        super(sqlQuery);
        initTempParam = 2;
    }

    /**
     * Constructor
     *
     * @param sqlQuery the SQL query
     * @param initialPlan the initial plan
     */
    public SimulatedAnnealing(SQLQuery sqlQuery, Operator initialPlan) {
        super(sqlQuery);
        this.initPlan = initialPlan;
        initTempParam = 0.4;
    }

    /**
     * Implements the simulated annealing algorithm
     *
     * @return the optimized plan
     */
    @Override
    public Operator getOptimizedPlan() {
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();

        if (initPlan == null) {
            initPlan = rip.prepareInitialPlan();
        }

        Operator minPlan = initPlan;
        RandomOptimizer.modifySchema(minPlan);
        int minCost = printPlanCost("Initial Plan", minPlan);

        if (numJoin == 0) {
            printPlanCost("Final Plan", minPlan);
            return minPlan;
        }

        boolean isFirstRound = true;
        for (double temperature = minCost * initTempParam; temperature > END_TEMPERATURE; temperature *= ALPHA) {
            Operator initPlan = minPlan;
            int initCost = minCost;

            if (!isFirstRound) {
                System.out.println("\n=============================================================================");
                initPlan = rip.prepareInitialPlan();
                RandomOptimizer.modifySchema(initPlan);
                initCost = printPlanCost("Initial Plan", initPlan);
            }
            isFirstRound = false;

            for (int i = 0; i < 12 * numJoin; i++) {
                Operator initPlanCopy = (Operator) initPlan.clone();
                Operator currentPlan = getNeighbor(initPlanCopy);
                int currentCost = printPlanCost("Neighbor", currentPlan);

                if (currentCost <= initCost || ifAccept(temperature, currentCost, initCost)) {
                    System.out.printf("Switched to another plan, initCost changes from %d to %d\n", initCost, currentCost);
                    initPlan = currentPlan;
                    initCost = currentCost;
                }
            }

            printPlanCost("Local Minimum", initPlan, initCost);
            if (initCost < minCost) {
                System.out.printf("Applied minimum from the current round, minCost changes from %d to %d\n", minCost, initCost);
                minPlan = initPlan;
                minCost = initCost;
            }
        }

        printPlanCost("Final Plan from SA", minPlan, minCost);
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
