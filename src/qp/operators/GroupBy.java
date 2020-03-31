package qp.operators;

import java.util.*;

/**
 * GroupBy Operator inherit from Distinct Operator
 */
public class GroupBy extends Distinct {

    public GroupBy(Operator base, ArrayList attrs) {
        super(base, attrs);
    }
}
