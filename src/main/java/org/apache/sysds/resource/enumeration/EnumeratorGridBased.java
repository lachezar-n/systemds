package org.apache.sysds.resource.enumeration;

import java.util.LinkedList;
import java.util.Map;

public class EnumeratorGridBased extends Enumerator {
    private int stepSize;
    public EnumeratorGridBased(Builder builder, int stepSize) {
        super(builder);
        this.stepSize = stepSize;
    }

    @Override
    public SolutionPoint enumerate() {
        SearchSpace searchSpace = new SearchSpace();
        searchSpace.initSpace(instances);
        SolutionPoint optSolutionPoint = new SolutionPoint(
                new ConfigurationPoint(null, null, -1),
                Double.MAX_VALUE,
                Double.MAX_VALUE
        );
        for (Map.Entry<Long, LinkedList<InstanceNode>> dEntry: searchSpace.entrySet()) {
            // loop over the search space to enumerate the driver configurations
            for (InstanceNode dNode: dEntry.getValue()) {
                setDriverConfigurations(dEntry.getKey(), dNode._1);
                // enumeration for single node execution
                if (minExecutors == 0) {
                    setExecutorConfigurations(0, -1, -1);
                    // TODO: full recompilation in single node mode
                    Enumerator.ConfigurationPoint newPoint = new ConfigurationPoint(dNode._2, null, 0);
                    // cost estimation
                    double[] cost = getCostEstimate(newPoint);
                    // update optimal solution if better configuration point found
                    updateOptimalSolution(optSolutionPoint, newPoint, cost[0], cost[1]);
                }
                // dummy "minimal" configuration used to trigger generation hybrid plan
                setExecutorConfigurations(1, 512*1024*1024, 1);
                // TODO: full recompile to establish the execution type of each operation
                // enumeration for distributed execution
                for (int n = Math.min(minExecutors, 1); n <= maxExecutors; n+=stepSize) {
                    for (Map.Entry<Long, LinkedList<InstanceNode>> eEntry: searchSpace.entrySet()) {
                        // loop over the search space to enumerate the executor configurations
                        for (InstanceNode eNode: eEntry.getValue()) {
                            setExecutorConfigurations(n, eEntry.getKey(), eNode._1);
                            // TODO: recompile only Spark blocks
                            Enumerator.ConfigurationPoint newPoint = new ConfigurationPoint(dNode._2, eNode._2, n);
                            // cost estimation
                            double[] cost = getCostEstimate(newPoint);
                            // update optimal solution if better configuration point found
                            updateOptimalSolution(optSolutionPoint, newPoint, cost[0], cost[1]);
                        }
                    }
                }

            }
        }
        return optSolutionPoint;
    }
}
