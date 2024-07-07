package org.apache.sysds.api.ropt;

import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import scala.Tuple3;
import scala.Tuple4;

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
                        // TODO: time cost estimation
                        double timeCost = 0.0;
                        // TODO: estimate the monetary cost based on the estimated execution time (time cost)
                        double monetaryCost = 0.0;
                        // update optimal solution if better configuration point found
                        updateOptimalSolution(optSolutionPoint, newPoint, timeCost, monetaryCost);
                    }
                }
            }
        }
        return optSolutionPoint;
    }
}
