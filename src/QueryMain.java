/**
 * This is main driver program of the query processor
 **/

import qp.operators.Debug;
import qp.operators.Operator;
import qp.optimizer.BufferManager;
import qp.optimizer.PlanCost;
import qp.optimizer.RandomInitialPlan;
import qp.optimizer.RandomOptimizer;
import qp.optimizer.SimulatedAnnealing;
import qp.parser.Scaner;
import qp.parser.parser;
import qp.utils.*;

import java.io.*;

public class QueryMain {

    static PrintWriter out;
    static int numAtts;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: java QueryMain <queryfilename> <resultfile> <pagesize> <numbuffer>");
            System.exit(1);
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        Batch.setPageSize(getPageSize(args, in));

        SQLQuery sqlquery = getSQLQuery(args[0]);
        configureBufferManager(sqlquery, args, in);

        Operator root = getQueryPlan(sqlquery);
        printFinalPlan(root, args, in);
        executeQuery(root, args[1]);
    }

    /**
     * Get page size from arguments, if not provided request as input
     **/
    private static int getPageSize(String[] args, BufferedReader in) {
        int pagesize = -1;
        if (args.length < 3) {
            /** Enter the number of bytes per page **/
            System.out.println("enter the number of bytes per page");
            try {
                String temp = in.readLine();
                pagesize = Integer.parseInt(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else pagesize = Integer.parseInt(args[2]);
        return pagesize;
    }

    /**
     * Parse query from query file
     **/
    public static SQLQuery getSQLQuery(String queryfile) {
        /** Read query file **/
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryfile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryfile);
            System.exit(1);
        }

        /** Scan the query **/
        Scaner sc = new Scaner(source);
        parser p = new parser();
        p.setScanner(sc);

        /** Parse the query **/
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occured while parsing");
            System.exit(1);
        }

        return p.getSQLQuery();
    }

    /**
     * If there are joins then assigns buffers to each join operator while preparing the plan.
     * As buffer manager is not implemented, just input the number of buffers available.
     **/
    private static void configureBufferManager(SQLQuery sqlQuery, String[] args, BufferedReader in) {
        int numJoin = sqlQuery.getNumJoin();
        if (numJoin != 0 || sqlQuery.isDistinct() || sqlQuery.isGroupby()) {
            int numBuff = 1000;
            if (args.length < 4) {
                System.out.println("enter the number of buffers available");
                try {
                    String temp = in.readLine();
                    numBuff = Integer.parseInt(temp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else numBuff = Integer.parseInt(args[3]);
            BufferManager bm = new BufferManager(numBuff, numJoin);
        }

        /** Check the number of buffers available is enough or not **/
        int numBuff = BufferManager.getBuffersPerJoin();
        if (numJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per join operator ");
            System.exit(1);
        }
    }

    /**
     * Run optimiser and get the final query plan as an Operator
     **/
    public static Operator getQueryPlan(SQLQuery sqlquery) {
        Operator root = null;

        /*
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        Operator planroot = rip.prepareInitialPlan();
        PlanCost pc = new PlanCost();
        int initCost = pc.getCost(planroot);
        Debug.PPrint(planroot);
        System.out.print("   "+initCost);
        System.out.println();*/


        RandomOptimizer SAOptimizer = new SimulatedAnnealing(sqlquery);
        Operator planroot = SAOptimizer.getOptimizedPlan();

        if (planroot == null) {
            System.out.println("DPOptimizer: query plan is null");
            System.exit(1);
        }

        root = RandomOptimizer.makeExecPlan(planroot);

        return root;
    }

    /**
     * Print final Plan and ask user whether to continue
     **/
    private static void printFinalPlan(Operator root, String[] args, BufferedReader in) {
        System.out.println("----------------------Execution Plan----------------");
        Debug.PPrint(root);
        System.out.println();
        /** Ask user whether to continue execution of the program **/
        System.out.println("enter 1 to continue, 0 to abort ");
        try {
            String temp = in.readLine();
            int flag = Integer.parseInt(temp);
            if (flag == 0) {
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Execute query and print run statistics
     **/
    public static double executeQuery(Operator root, String resultfile) {
        long starttime = System.currentTimeMillis();
        if (root.open() == false) {
            System.out.println("Root: Error in opening of root");
            System.exit(1);
        }
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile)));
        } catch (IOException io) {
            System.out.println("QueryMain:error in opening result file: " + resultfile);
            System.exit(1);
        }

        /** Print the schema of the result **/
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        printSchema(schema);

        int limit = root.getLimit();
        int offset = root.getOffset();
        boolean withLimit = false;
        boolean endPrinting = false;
        if (limit >= 0) {
            withLimit = true;
        }

        /** Print each tuple in the result **/
        Batch resultbatch;
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); ++i) {
                /** Skips the first #offset rows of tuple **/
                if (offset > 0) {
                    offset--;
                    continue;
                }
                printTuple(resultbatch.get(i));
                /** Stops printing if limit reaches 0 **/
                if (withLimit && limit > 0) {
                    limit--;
                    if (limit == 0) {
                        endPrinting = true;
                        break;
                    }
                }
            }
            if (endPrinting) {
                break;
            }
        }
        root.close();
        out.close();

        long endtime = System.currentTimeMillis();
        double executiontime = (endtime - starttime) / 1000.0;
        System.out.println("Execution time = " + executiontime);
        return executiontime;
    }

    protected static void printSchema(Schema schema) {
        String[] aggregates = new String[]{"", "MAX", "MIN", "SUM", "COUNT", "AVG"};
        for (int i = 0; i < numAtts; ++i) {
            Attribute attr = schema.getAttribute(i);
            int aggregate = attr.getAggType();
            String tabname = attr.getTabName();
            String colname = attr.getColName();
            if (aggregate == 0) {
                out.print(tabname + "." + colname + "  ");
            } else {
                out.print(aggregates[aggregate] + "(" + tabname + "." + colname + ")  ");
            }
        }
        out.println();
    }

    protected static void printTuple(Tuple t) {
        for (int i = 0; i < numAtts; ++i) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                out.print(((Float) data).floatValue() + "\t");
            } else if (data == null) {
                out.print("-NULL-\t");
            } else {
                out.print(((String) data) + "\t");
            }
        }
        out.println();
    }
}