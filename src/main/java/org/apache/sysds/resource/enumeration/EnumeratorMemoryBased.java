package org.apache.sysds.resource.enumeration;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.runtime.controlprogram.*;

import java.util.*;
import java.util.stream.Collectors;

public class EnumeratorMemoryBased extends Enumerator {
    public final static long MINIMUM_RELEVANT_MEM_ESTIMATE = 2 * 1024^3; // 2GB
    public final static long MEMORY_DELTA = 500 * 1024^2; // 500MB
    public final static double BROADCAST_MEMORY_FACTOR = 0.21; // as fraction of the entire executor memory
    public EnumeratorMemoryBased(Builder builder) {
        super(builder);
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

        List<Long> availableNodesMemory = new ArrayList<>(searchSpace.keySet());
        // extract relevant (based on memory estimates) memory points for enumeration driver configs
        List<Long> estimatesForCp = getMemoryEstimateCP(program.getProgramBlocks());
        List<Long> driverMemoryPoints = getMemoryPoints(estimatesForCp, availableNodesMemory);
        // extract relevant (based on memory estimates) memory points for enumeration executors configs
        List<Long> estimatesForSpark = getMemoryEstimateSpark(program.getProgramBlocks());
        List<Long> executorMemoryPoints = getMemoryPoints(estimatesForSpark, availableNodesMemory);
        // get the largest memory estimates for further reducing the enumeration space
        long maxEstimate = estimatesForCp.stream().max(Long::compareTo).orElse(0L);

        for (long dMemory: driverMemoryPoints) {
            // loop over the relevant driver memory configurations
            for (InstanceNode dNode: searchSpace.get(dMemory)) {
                setDriverConfigurations(dMemory, dNode._1);
                // enumeration for single node execution only in case CP has enough memory for all estimates
                if (maxExecutors == 0 && dMemory > maxEstimate) {
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
                // get all relevant memory points for enumeration
                for (int n = Math.min(minExecutors, 1); n <= maxExecutors; n+=1) {
                    for (long eMemory: executorMemoryPoints) {
                        // loop over the relevant executor memory configurations
                        if ((n * eMemory) < maxExecutors) {
                            // skip enumeration of a point that would not have sufficient distributed memory
                            continue;
                        }
                        for (InstanceNode eNode: searchSpace.get(eMemory)) {
                            setExecutorConfigurations(n, eMemory, eNode._1);
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

    private List<Long> getMemoryPoints(List<Long> estimates, List<Long> availableMemory) {
        ArrayList<Long> listEstimates = new ArrayList<>(estimates);
        Collections.sort(listEstimates);

        ArrayList<Long> result = new ArrayList<>();

        List<Long> relevantPoints = new ArrayList<>(availableMemory);
        for (long estimate: listEstimates) {
            // get new point for enumeration
            List<Long> smallerPoints = relevantPoints.stream()
                    .filter(mem -> mem >= (estimate - MEMORY_DELTA) && mem < estimate)
                    .collect(Collectors.toList());
            result.addAll(smallerPoints);
            // reduce the list of relevant points
            relevantPoints = relevantPoints.stream()
                    .dropWhile(mem -> mem < estimate)
                    .collect(Collectors.toList());
        }
        // get point for enumeration bigger than the biggest memory estimate
        long biggestEstimate = listEstimates.isEmpty()? 0 : listEstimates.get(listEstimates.size()  - 1);
        List<Long> biggerPoints = relevantPoints.stream()
                .filter(mem -> mem >= biggestEstimate && mem < (biggestEstimate + MEMORY_DELTA))
                .collect(Collectors.toList());
        result.addAll(biggerPoints);

        return result;
    }

    private List<Long> getMemoryEstimateCP(ArrayList<ProgramBlock> pbs) {
        HashSet<Long> estimates = new HashSet<>();
        getMemoryEstimates(program.getProgramBlocks(), estimates, false);
        return estimates.stream()
                .map(mem -> (long) (mem / OptimizerUtils.MEM_UTIL_FACTOR))
                .filter(mem -> mem > MINIMUM_RELEVANT_MEM_ESTIMATE)
                .collect(Collectors.toList());
    }

    private List<Long> getMemoryEstimateSpark(ArrayList<ProgramBlock> pbs) {
        HashSet<Long> estimates = new HashSet<>();
        getMemoryEstimates(program.getProgramBlocks(), estimates, true);
        return estimates.stream()
                .map(mem -> (long) (mem / BROADCAST_MEMORY_FACTOR))
                .filter(mem -> mem > MINIMUM_RELEVANT_MEM_ESTIMATE)
                .collect(Collectors.toList());
    }

    private void getMemoryEstimates(ArrayList<ProgramBlock> pbs, HashSet<Long> mem, boolean spark) {
        for( ProgramBlock pb : pbs )
            getMemoryEstimates(pb, mem, spark);
    }

    private void getMemoryEstimates(ProgramBlock pb, HashSet<Long> mem, boolean spark) {
        if (pb instanceof FunctionProgramBlock)
        {
            FunctionProgramBlock fpb = (FunctionProgramBlock)pb;
            getMemoryEstimates(fpb.getChildBlocks(), mem, spark);
        }
        else if (pb instanceof WhileProgramBlock)
        {
            WhileProgramBlock fpb = (WhileProgramBlock)pb;
            getMemoryEstimates(fpb.getChildBlocks(), mem, spark);
        }
        else if (pb instanceof IfProgramBlock)
        {
            IfProgramBlock fpb = (IfProgramBlock)pb;
            getMemoryEstimates(fpb.getChildBlocksIfBody(), mem, spark);
            getMemoryEstimates(fpb.getChildBlocksElseBody(), mem, spark);
        }
        else if (pb instanceof ForProgramBlock) // including parfor
        {
            ForProgramBlock fpb = (ForProgramBlock)pb;
            getMemoryEstimates(fpb.getChildBlocks(), mem, spark);
        }
        else
        {
            StatementBlock sb = pb.getStatementBlock();
            if( sb != null && sb.getHops() != null ){
                Hop.resetVisitStatus(sb.getHops());
                for( Hop hop : sb.getHops() )
                    getMemoryEstimates(hop, mem, spark);
            }
        }
    }

    private void getMemoryEstimates(Hop hop, HashSet<Long> mem, boolean spark)
    {
        if( hop.isVisited() )
            return;

        //process children
        for(Hop hi : hop.getInput())
            getMemoryEstimates(hi, mem, spark);

        if (spark) {
            long estimate = (long) hop.getOutputMemEstimate(0);
            if (estimate > 0)
                mem.add(estimate);
        } else {
            mem.add((long) hop.getMemEstimate());
        }

        hop.setVisited();
    }
}
