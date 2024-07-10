package org.apache.sysds.resource.enumeration;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.cost.CostEstimationWrapper;
import org.apache.sysds.resource.cost.CostEstimationException;
import org.apache.sysds.resource.cost.CostEstimator;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.resource.enumeration.CloudUtils.InstanceType;
import org.apache.sysds.resource.enumeration.CloudUtils.InstanceSize;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


public abstract class Enumerator {
    public enum EnumerationStrategy {
        GridBased, // considering all combination within a given range of configuration
        MemoryBased, // considering only combinations of configurations with memory budge close to memory estimates
    }

    public enum OptimizationStrategy {
        MinTime, // always prioritize execution time minimization
        MinPrice, // always prioritize operation price minimization
    }

    public static final int DEFAULT_MIN_EXECUTORS = 0; // Single Node execution allowed
    /**
     * A reasonable upper bound for the possible number of executors
     * is required to set limits for the search space and to avoid
     * evaluating cluster configurations that most probably would
     * have too high distribution overhead
     */
    public static final int DEFAULT_MAX_EXECUTORS = 200;
    Object costEstimator; // placeholder for the cost estimator
    Object compiler; // placeholder for potential re-compiler class
    HashMap<String, CloudInstance> instances = null;
    Program program;
    CloudUtils utils;
    EnumerationStrategy enumStrategy;
    OptimizationStrategy optStrategy;
    private final double maxTime;
    private final double maxPrice;
    protected final int minExecutors;
    protected final int maxExecutors;
    protected final Set<InstanceType> instanceTypesRange;
    protected final Set<InstanceSize> instanceSizeRange;


    public Enumerator(Builder builder) {
        if (builder.provider.equals(CloudUtils.CloudProvider.AWS)) {
            utils = new AWSUtils();
        } // as of now no other provider is supported
        this.program = builder.program;
        this.enumStrategy = builder.enumStrategy;
        this.optStrategy = builder.optStrategy;
        this.maxTime = builder.maxTime;
        this.maxPrice = builder.maxPrice;
        this.minExecutors = builder.minExecutors;
        this.maxExecutors = builder.maxExecutors;
        this.instanceTypesRange = builder.instanceTypesRange;
        this.instanceSizeRange = builder.instanceSizeRange;
    }

    public void loadInstanceTableFile(String path) throws IOException {
        instances = utils.loadInstanceInfoTable(path);
    }

    public void setInstanceTable(HashMap<String, CloudInstance> input) {
        instances = input;
    }

    /**
     * Enumerate all possible configurations that fit
     * the given limits and comply with the cost budget
     * following a custom enumeration strategy. The method returns
     * a {@code ConfigurationPoint} object, which encapsulates the
     * cluster configuration being the optimal solution.
     *
     * @return
     */
    public abstract SolutionPoint enumerate();

    protected double[] getCostEstimate(ConfigurationPoint point) {
        // get the estimated time cost
        double timeCost;
        try {
            // estimate execution time based on the state of the current local and spark configurations;
            // TODO: pass further relevant cluster configurations to cost estimator after extending it
            //  like for example: FLOPS, I/O and networking speed
            timeCost = CostEstimator.estimateExecutionTime(program);
        } catch (CostEstimationException e) {
            timeCost = Double.MAX_VALUE;
        }
        // calculate monetary cost
        double monetaryCost = utils.calculateClusterPrice(point, timeCost);
        return new double[] {timeCost, monetaryCost}; // time cost, monetary cost
    }

    protected void updateOptimalSolution(SolutionPoint currentOptimal, ConfigurationPoint newPoint, double newTimeCost, double newMonetaryCost) {
        // TODO(1): clarify if setting max time and max price simultaneously makes really sense
        // TODO(2): consider more dynamic comparison, e.g.:
        //  -> MinTime: slightly longer execution time within the maxTime can have much lower price
        //  -> MinPrice: slightly higher operation cost within the maxCost can have much faster execution time
        if (optStrategy == OptimizationStrategy.MinTime) {
            if (newMonetaryCost > maxPrice || newTimeCost >= currentOptimal.timeCost) {
                return;
            }
        } else if (optStrategy == OptimizationStrategy.MinPrice) {
            if (newTimeCost > maxTime || newMonetaryCost >= currentOptimal.monetaryCost) {
                return;
            }
        }
        currentOptimal.update(newPoint, newTimeCost, newMonetaryCost);
    }

    protected static void setDriverConfigurations(long nodeMemory, int nodeNumCores) {
        // TODO: think of reasonable factor for the JVM heap as prt of the node's memory
        InfrastructureAnalyzer.setLocalMaxMemory(nodeMemory);
        InfrastructureAnalyzer.setLocalPar(nodeNumCores);
    }

    protected static void setExecutorConfigurations(int numExecutors, long nodeMemory, int nodeNumCores) {
        // TODO: think of reasonable factor for the JVM heap as prt of the node's memory
        if (numExecutors > 0) {
            DMLScript.setGlobalExecMode(Types.ExecMode.HYBRID);
            SparkConf sparkConf = SparkExecutionContext.createSystemDSSparkConf();
            // ------------------ Static Configurations -------------------
            // TODO: think how to avoid setting them every time
            sparkConf.set("spark.master", "local[*]");
            sparkConf.set("spark.app.name", "SystemDS");
            sparkConf.set("spark.memory.useLegacyMode", "false");
            // ------------------ Static Configurations -------------------
            // ------------------ Dynamic Configurations -------------------
            sparkConf.set("spark.executor.memory", nodeMemory+"m");
            sparkConf.set("spark.executor.instances", Integer.toString(numExecutors));
            sparkConf.set("spark.executor.cores", Integer.toString(nodeNumCores));
            // ------------------ Dynamic Configurations -------------------
            SparkExecutionContext.initVirtualSparkContext(sparkConf);
        } else {
            DMLScript.setGlobalExecMode(Types.ExecMode.SINGLE_NODE);
        }
    }


    /**
     * Data structure representing the search space for VM instances
     * as node's memory mapped to list of {@code InstanceNode}
     * objects carrying the numbers of cores for particular VM instance
     * having the same memory resource and a corresponding
     * VM instance object ({@code CloudInstance}).
     * This representation allows compact storing of VM instance
     * characteristics relevant for program compilation while
     * still keeping a reference to the object carrying the
     * whole instance information, relevant for cost estimation.
     */
    public static class SearchSpace extends TreeMap<Long, LinkedList<InstanceNode>> {
        void initSpace(HashMap<String, CloudInstance> instances) {
            for (CloudInstance instance: instances.values()) {
                long currentMemory = instance.getMemory();
                LinkedList<InstanceNode> currentList = this.getOrDefault(currentMemory, new LinkedList<>());
                this.putIfAbsent(currentMemory, currentList);
                InstanceNode.appendInstance(instance, currentList);
            }
        }
    }

    /**
     * Data structure holding a combination of integer,representing number of virtual CPU cores
     * of a corresponding VM instance, and {@code CloudInstance} object of the same VM instance
     * to carry all additional instance characteristics needed for cost estimation.
     * @_1 - number of cores per instance
     * @_2 - VM instance object representation
     */
    public static class InstanceNode extends Tuple2<Integer, CloudInstance> implements Comparable<InstanceNode> {

        public InstanceNode(Integer _1, CloudInstance _2) {
            super(_1, _2);
        }

        @Override
        public int compareTo(@NotNull Enumerator.InstanceNode o) {
            int coreComparison = Integer.compare(this._1, o._1);
            if (coreComparison != 0) {
                return coreComparison;
            }
            int flopsComparison = Long.compare(this._2.getFLOPS(), o._2.getFLOPS());
            if (flopsComparison != 0) {
                return coreComparison;
            }
            int diskComparison = Double.compare(this._2.getDiskSpeed(), o._2.getDiskSpeed());
            if (diskComparison != 0) {
                return coreComparison;
            }
            int networkComparison = Double.compare(this._2.getNetworkSpeed(), o._2.getNetworkSpeed());
            // last comparison -> in theory ensures unique order (depending on the FLOPS evaluation of CPUs)
            return networkComparison;
        }

        static void appendInstance(CloudInstance instance, LinkedList<InstanceNode> list) {
            InstanceNode newNode = new InstanceNode(instance.getVCPUs(), instance);

            ListIterator<InstanceNode> iterator = list.listIterator();

            while (iterator.hasNext()) {
                InstanceNode currentNode = iterator.next();
                if (newNode.compareTo(currentNode) < 0) {
                    iterator.previous();
                    iterator.add(newNode);
                    return;
                }
            }
            // in case the new node is the greater one
            list.add(newNode);
        }
    }

    /**
     * Simple data structure to hold cluster configurations
     */
    public static class ConfigurationPoint {
        public CloudInstance driverInstance;
        public CloudInstance executorInstance;
        public int numberExecutors;

        public ConfigurationPoint(CloudInstance driverInstance, CloudInstance executorInstance, int numberExecutors) {
            this.driverInstance = driverInstance;
            this.executorInstance = executorInstance;
            this.numberExecutors = numberExecutors;
        }
    }

    /**
     * Data structure to hold all data related to cost estimation
     */
    public static class SolutionPoint extends ConfigurationPoint {
        double timeCost;
        double monetaryCost;

        public SolutionPoint(ConfigurationPoint inputPoint, double timeCost, double monetaryCost) {
            super(inputPoint.driverInstance, inputPoint.executorInstance, inputPoint.numberExecutors);
            this.timeCost = timeCost;
            this.monetaryCost = monetaryCost;
        }

        public void update(ConfigurationPoint point, double timeCost, double monetaryCost) {
            this.driverInstance = point.driverInstance;
            this.executorInstance = point.executorInstance;
            this.numberExecutors = point.numberExecutors;
            this.timeCost = timeCost;
            this.monetaryCost = monetaryCost;
        }
    }

    public static class Builder {
        private CloudUtils.CloudProvider provider = CloudUtils.CloudProvider.AWS; // currently default and only choice
        private Program program;
        private EnumerationStrategy enumStrategy = null;
        private OptimizationStrategy optStrategy = null;
        private double maxTime = -1d;
        private double maxPrice = -1d;
        private int minExecutors = DEFAULT_MIN_EXECUTORS;
        private int maxExecutors = DEFAULT_MAX_EXECUTORS;
        private Set<InstanceType> instanceTypesRange;
        private Set<InstanceSize> instanceSizeRange;
        private int stepSize = 1; // optional for grid-based enumeration

        public Builder() {}

        public Builder withCloudProvider(CloudUtils.CloudProvider provider) {
            this.provider = provider;
            return this;
        }

        public Builder withRuntimeProgram(Program program) {
            this.program = program;
            return this;
        }

        public Builder withEnumerationStrategy(EnumerationStrategy strategy) {
            this.enumStrategy = strategy;
            return this;
        }

        public Builder withOptimizationStrategy(OptimizationStrategy strategy) {
            this.optStrategy = strategy;
            return this;
        }

        public Builder withTimeLimit(double time) {
            this.maxTime = time;
            return this;
        }

        public Builder withBudget(double price) {
            this.maxPrice = price;
            return this;
        }

        public Builder withExecutorNumberRange(int min, int max) {
            this.minExecutors = min;
            this.maxExecutors = max;
            return this;
        }

        public Builder withInstanceTypeRange(String[] instanceTypes) {
            this.instanceTypesRange = typeRangeFromStrings(instanceTypes);
            return this;
        }

        public Builder withInstanceSizeRange(String[] instanceSizes) {
            this.instanceSizeRange = sizeRangeFromStrings(instanceSizes);
            return this;
        }

        public Builder withStepSize(int stepSize) {
            this.stepSize = stepSize;
            return this;
        }

        public Enumerator build() {
            if (this.provider != CloudUtils.CloudProvider.AWS) { // currently obsolete but avoids uncertain builder state
                throw new UnsupportedOperationException("Only AWS is supported as of now.");
            }

            if (this.program == null) {
                throw new IllegalArgumentException("Providing runtime program is required");
            }

            switch (optStrategy) {
                case MinTime:
                    if (this.maxPrice < 0) {
                        throw new IllegalArgumentException("Budget not specified but required " +
                                "for the chosen optimization strategy: " + optStrategy.toString());
                    }
                    break;
                case MinPrice:
                    if (this.maxTime < 0) {
                        throw new IllegalArgumentException("Time limit not specified but required " +
                                "for the chosen optimization strategy: " + optStrategy.toString());
                    }
                    break;
                default: // in case optimization strategy was not configured
                    throw new IllegalArgumentException("Setting an optimization strategy is required.");
            }

            switch (enumStrategy) {
                case GridBased:
                    return new EnumeratorGridBased(this, stepSize);
                case MemoryBased:
                    return new EnumeratorMemoryBased(this);
                default:
                    throw new IllegalArgumentException("Setting an enumeration strategy is required.");
            }
        }

        protected static Set<InstanceType> typeRangeFromStrings(String[] types) {
            Set<InstanceType> result = EnumSet.noneOf(InstanceType.class);
            for (String typeAsString: types) {
                InstanceType type = InstanceType.valueOf(typeAsString); // can throw IllegalArgumentException
                result.add(type);
            }
            return result;
        }

        protected static Set<InstanceSize> sizeRangeFromStrings(String[] sizes) {
            Set<InstanceSize> result = EnumSet.noneOf(InstanceSize.class);
            for (String sizeAsString: sizes) {
                InstanceSize size = InstanceSize.valueOf(sizeAsString); // can throw IllegalArgumentException
                result.add(size);
            }
            return result;
        }
    }
}
