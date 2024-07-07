package org.apache.sysds.resource.enumeration;

import java.util.LinkedList;
import java.util.Map;

public class EnumeratorGridBased extends Enumerator {
    public EnumeratorGridBased(Builder builder) {
        super(builder);
    }

    @Override
    protected SolutionPoint enumerate() {
        SearchSpace searchSpace = new SearchSpace();
        searchSpace.initSpace(instances);
        SolutionPoint optSolutionPoint = new SolutionPoint(
                new ConfigurationPoint(null, null, -1),
                Double.MAX_VALUE,
                Double.MAX_VALUE
        );
        for (Map.Entry<Long, LinkedList<InstanceNode>> dEntry: searchSpace.driverSpace.entrySet()) {
            for (InstanceNode dNode: dEntry.getValue()) {
                setDriverConfigurations(dEntry.getKey(), dNode._1);
                // TODO: full recompile
                for (Map.Entry<DistributedMemory, LinkedList<InstanceNode>> eEntry: searchSpace.executorSpace.entrySet()) {
                    DistributedMemory eMemoryTuple = eEntry.getKey();
                    for (InstanceNode eNode: eEntry.getValue()) {
                        setExecutorConfigurations(eMemoryTuple._1, eMemoryTuple._2, eNode._1);
                        // TODO: recompile only Spark blocks
                        Enumerator.ConfigurationPoint newPoint = new ConfigurationPoint(dNode._2, eNode._2, eMemoryTuple._2);
                        // cost estimation
                        double[] cost = getCostEstimate(newPoint);
                        // update optimal solution if better configuration point found
                        updateOptimalSolution(optSolutionPoint, newPoint, cost[0], cost[1]);
                    }
                }
            }
        }
        return optSolutionPoint;
    }
}
