package org.apache.sysds.resource.enumeration;

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

import java.util.*;


public abstract class Enumerator {
    /**
     * A reasonable upper bound for the possible number of executors
     * is required to set limits for the search space and to avoid
     * evaluating cluster configurations that most probably would
     * have too high distribution overhead
     */
    private static final int UPPER_BOUND_NUM_EXECUTORS = 200;
    public enum EnumerationStrategy {
        RangeBased, // considering all combination within a given range of configuration
        MemoryBased, // considering only combinations of configurations with memory budge close to memory estimates
    }

    public enum OptimizationStrategy {
        MinTime, // always prioritize execution time minimization
        MinPrice, // always prioritize operation price minimization
    }

    public static final int DEFAULT_MIN_EXECUTORS = 0; // Single Node execution allowed
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

    public Enumerator(Builder builder) {
        if (builder.provider.equals(CloudUtils.CloudProvider.AWS)) {
            utils = new AWSUtils();
        } // as of now no other provider is supported
        this.enumStrategy = builder.enumStrategy;
        this.optStrategy = builder.optStrategy;
        this.maxTime = builder.maxTime;
        this.maxPrice = builder.maxPrice;
        this.minExecutors = builder.minExecutors;
        this.maxExecutors = builder.maxExecutors;
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
    protected abstract SolutionPoint enumerate();

    protected double[] getCostEstimate(ConfigurationPoint point) {
        // get the estimated time cost
        double timeCost;
        try {
            timeCost = CostEstimator.estimateExecutionTime(program);
        } catch (CostEstimationException e) {
            timeCost = Double.MAX_VALUE;
        }
        // calculate monetary cost

        // and then execute the cost estimator
        // estimate also the monastery cost
        return new double[] {0, 0}; // time cost, monetary cost
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

    protected static void setExecutorConfigurations(long nodeMemory, int numExecutors, int nodeNumCores) {
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

    static class ConfigurationPoint {
        CloudInstance driverInstance;
        CloudInstance executorInstance;
        int numberExecutors;

        public ConfigurationPoint(CloudInstance driverInstance, CloudInstance executorInstance, int numberExecutors) {
            this.driverInstance = driverInstance;
            this.executorInstance = executorInstance;
            this.numberExecutors = numberExecutors;
        }
    }

    static class SolutionPoint extends ConfigurationPoint {
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
        private EnumerationStrategy enumStrategy = null;
        private OptimizationStrategy optStrategy = null;
        private double maxTime = -1d;
        private double maxPrice = -1d;
        private int minExecutors = DEFAULT_MIN_EXECUTORS;
        private int maxExecutors = DEFAULT_MAX_EXECUTORS;
        private Object[] args;

        public Builder() {}

        public Builder withCloudProvider(CloudUtils.CloudProvider provider) {
            this.provider = provider;
            return this;
        }

        public Builder withEnumerationStrategy(EnumerationStrategy strategy) {
            if (strategy == EnumerationStrategy.RangeBased) {
                args = new Set[4];
            }
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
            if (enumStrategy != EnumerationStrategy.RangeBased) {
                throw new IllegalArgumentException("Setting ranges is allowed only after " +
                        "specifying enumeration strategy ot type RangeBased");
            }
            this.minExecutors = min;
            this.maxExecutors = max;
            return this;
        }

        // RangeBased specific -----------------------------------------------------------------------------------------
        public Builder withInstanceTypeRange(String[] instanceTypes) {
            if (enumStrategy != EnumerationStrategy.RangeBased) {
                throw new IllegalArgumentException("Setting ranges is allowed only after " +
                        "specifying enumeration strategy ot type RangeBased");
            }
            Set<InstanceType> types = EnumeratorRangeBased.typeRangeFromStrings(instanceTypes);
            args[0] = types;
            return this;
        }

        public Builder withInstanceSizeRange(String[] instanceSizes) {
            if (enumStrategy != EnumerationStrategy.RangeBased) {
                throw new IllegalArgumentException("Setting ranges is allowed only after " +
                        "specifying enumeration strategy ot type RangeBased");
            }
            Set<InstanceSize> sizes = EnumeratorRangeBased.sizeRangeFromStrings(instanceSizes);
            args[1] = sizes;
            return this;
        }

        public Enumerator build() {
            if (this.provider != CloudUtils.CloudProvider.AWS) { // currently obsolete but avoids uncertain builder state
                throw new UnsupportedOperationException("Only AWS is supported as of now.");
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
                case RangeBased:
                    InstanceType[] types = (InstanceType[]) Objects.requireNonNullElseGet((InstanceType[]) args[0], InstanceType::values);
                    InstanceSize[] sizes = (InstanceSize[]) Objects.requireNonNullElseGet((InstanceSize[]) args[1], InstanceSize::values);
                    int min = (int) Objects.requireNonNullElseGet(args[2], () -> DEFAULT_MIN_EXECUTORS);
                    int max = (int) Objects.requireNonNullElseGet(args[3], () -> DEFAULT_MAX_EXECUTORS);
                    return new EnumeratorRangeBased(this, types, sizes, min, max);
                case MemoryBased:
                    return new EnumeratorMemoryBased(this);
                default:
                    throw new IllegalArgumentException("Setting an enumeration strategy is required.");
            }
        }
    }

    /**
     * Data structure representing the whole search space for SystemDS cluster configurations.
     * This search space has 4 dimensions and on higher abstraction level it can be represented
     * in 2 dimensions with 2 hyperparameters each:
     * <li>search space for CP (Spark driver) node's configurations ({@code driverSpace}):
     *  mapping node's memory (1. parameter) to the corresponding number of CPU cores (2. parameter)
     *  bind to a {@code CloudInstance} object</li>
     * <li>search space for distributed (Spark executor) nodes' configurations ({@code executorSpace}):
     *  mapping node's memory bind to number of executors, representing the total distributed memory, (3. parameter)
     *  to the corresponding number of CPU cores (4. parameter) bind to a {@code CloudInstance} object</li>
     * <li>Ascending ordered 2-D data structures - {@code driverSpace} and {@code executorSpace}:
     * {@code TreeMap} ensures the ordering of the first dimension (keys) and the order
     * of the second dimension (values) must be kept constantly at adding new elements
     * to the corresponding lists</li>
     */
    public static class SearchSpace {
        TreeMap<Long, LinkedList<InstanceNode>> driverSpace;
        TreeMap<DistributedMemory, LinkedList<InstanceNode>> executorSpace;

        public SearchSpace() {
            this.driverSpace = new TreeMap<>();
            this.executorSpace = new TreeMap<>();
        }

        public void initSpace(HashMap<String, CloudInstance> instances) {
            initSpace(instances, 0, UPPER_BOUND_NUM_EXECUTORS);
        }

        void initSpace(HashMap<String, CloudInstance> instances, int minExecutors, int maxExecutors) {
            initDriverSpace(instances);
            initExecutorSpace(instances,minExecutors, maxExecutors);
        }

        void initDriverSpace(HashMap<String, CloudInstance> instances) {
            for (CloudInstance instance: instances.values()) {
                long currentMemory = instance.getMemory();
                LinkedList<InstanceNode> currentList = driverSpace.getOrDefault(currentMemory, new LinkedList<>());
                driverSpace.putIfAbsent(currentMemory, currentList);
                InstanceNode.appendInstance(instance, currentList);
            }
        }

        void initExecutorSpace(HashMap<String, CloudInstance> instances, int minNumber, int maxNumber) {
            if (minNumber < 0 || maxNumber <= minNumber || maxNumber > UPPER_BOUND_NUM_EXECUTORS) {
                throw new IllegalArgumentException("Given range for number of execurtors is not valid. " +
                        "It should fit in [0, " + UPPER_BOUND_NUM_EXECUTORS + "]");
            }
            if (minNumber == 0) {
                DistributedMemory zeroKey = new DistributedMemory(-1L, 0);
                executorSpace.put(zeroKey, null); // TODO: consider if there is a better approach for 0 executors (single node execution)
            }
            for (int n = 1; n <= maxNumber; n++) {
                for (CloudInstance instance: instances.values()) {
                    long currentMemory = instance.getMemory();
                    DistributedMemory currentKey = new DistributedMemory(currentMemory, n);
                    LinkedList<InstanceNode> currentList = executorSpace.getOrDefault(currentKey, new LinkedList<>());
                    executorSpace.putIfAbsent(currentKey, currentList);
                    InstanceNode.appendInstance(instance, currentList);
                }
            }
        }
//        public static LinkedList<Tuple3<Long, Integer, CloudInstance>> flattenDriverEntry(Map.Entry<Long, LinkedList<InstanceNode>> entry) {
//            LinkedList<Tuple3<Long, Integer, CloudInstance>> result = new LinkedList<>();
//            for (InstanceNode node: entry.getValue()) {
//                Tuple3<Long, Integer, CloudInstance> newTuple = new Tuple3<>(entry.getKey(), node._1, node._2);
//                result.add(newTuple);
//            }
//            return result;
//        }
//
//
//        public static LinkedList<Tuple4<Long, Integer, Integer, CloudInstance>> flattenExecutorEntry(Map.Entry<DistributedMemory, LinkedList<InstanceNode>> entry) {
//            LinkedList<Tuple4<Long, Integer, Integer, CloudInstance>> result = new LinkedList<>();
//            for (InstanceNode node: entry.getValue()) {
//                Tuple4<Long, Integer, Integer, CloudInstance> newTuple = new Tuple4<>(
//                        entry.getKey()._1,
//                        entry.getKey()._2,
//                        node._1,
//                        node._2
//                );
//                result.add(newTuple);
//            }
//            return result;
//        }
    }

    /**
     *
     * @_1 - node memory in MB
     * @_2 - number of executor nodes
     */
    static class DistributedMemory extends Tuple2<Long, Integer> implements Comparable<DistributedMemory>{

        public DistributedMemory(Long _1, Integer _2) {
            super(_1, _2);
        }

        @Override
        public int compareTo(@NotNull Enumerator.DistributedMemory o) {
            long o1TotalMemory = this._1 * this._2;
            long o2TotalMemory = o._1 * o._2;
            int totalMemoryComparison = Long.compare(o1TotalMemory, o2TotalMemory);
            if (totalMemoryComparison != 0) {
                return totalMemoryComparison;
            }
            // further comparison based on the number of executors
            return Integer.compare(this._2, o._2);
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

    // -----------------------------------------------------------------------------------------------------------------
    // OLD - TODO: delete after the implementation is stable

//    static class DriverSpace {
//        /**
//         * <p>key: memory in MB</p>
//         * <p>value: driver memory node</p>
//         * Using TreeMap to ensure ascending order based on the memory size.
//         */
//        TreeMap<Long, DriverMemoryNode> memoryNodes = new TreeMap<>();
//
//        void initialize(HashMap<String, CloudInstance> instances) {
//            for (CloudInstance instance: instances.values()) {
//                long currentMemory = instance.getMemory();
//                DriverMemoryNode currentNode;
//                if (memoryNodes.containsKey(currentMemory)) {
//                    currentNode = memoryNodes.get(currentMemory);
//                } else {
//                    currentNode = new DriverMemoryNode(currentMemory);
//                    memoryNodes.put(currentMemory, currentNode);
//                }
//                CharacteristicNode.appendInstance(instance, currentNode.characteristicsNodes);
//            }
//        }
//    }
//
//    static class ExecutorSpace {
//        /**
//         * <p>key: total memory in MB (sum of all executors' memory)</p>
//         * <p>value: driver memory node</p>
//         * Using TreeMap to ensure ascending order based on the memory size.
//         */
//        TreeMap<Tuple2<Long, Integer>, ExecutorMemoryNode> memoryNodes = new TreeMap<>((o1, o2) -> {
//            long o1TotalMemory = o1._1 * o1._2;
//            long o2TotalMemory = o2._1 * o2._2;
//            int totalMemoryComparison = Long.compare(o1TotalMemory, o2TotalMemory);
//            if (totalMemoryComparison != 0) {
//                return totalMemoryComparison;
//            }
//            // further comparison based on the number of executors
//            return Integer.compare(o1._2, o2._2);
//        });
//
//        void initialize(HashMap<String, CloudInstance> instances, int minNumber, int maxNumber) {
//            if (minNumber < 0 || maxNumber <= minNumber || maxNumber > UPPER_BOUND_NUM_EXECUTORS) {
//                throw new IllegalArgumentException("Given range for number of execurtors is not valid. " +
//                        "It should fit in [0, " + UPPER_BOUND_NUM_EXECUTORS + "]");
//            }
//            if (minNumber == 0) {
//                Tuple2<Long, Integer> zeroKey = new Tuple2<>(-1L, 0);
//                memoryNodes.put(zeroKey, null); // TODO: consider if there is a better approach for 0 executors (single node execution)
//            }
//            for (int n = 1; n <= maxNumber; n++) {
//                for (CloudInstance instance: instances.values()) {
//                    long currentMemory = instance.getMemory();
//                    Tuple2<Long, Integer> currentKey = new Tuple2<>(currentMemory, n);
//                    ExecutorMemoryNode currentNode;
//                    if (memoryNodes.containsKey(currentKey)) {
//                        currentNode = memoryNodes.get(currentKey);
//                    } else {
//                        // here put the instance memory and not the total one
//                        currentNode = new ExecutorMemoryNode(instance.getMemory(), n);
//                        memoryNodes.put(currentKey, currentNode);
//                    }
//                    CharacteristicNode.appendInstance(instance, currentNode.characteristicsNodes);
//                }
//            }
//        }
//
//
//    }
//    static class DriverMemoryNode {
//        long nodeMemory; // in MB
//        LinkedList<CharacteristicNode> characteristicsNodes;
//
//        public DriverMemoryNode(long memory) {
//            this.nodeMemory = memory;
//            characteristicsNodes = new LinkedList<>();
//        }
//
//        LinkedList<Tuple3<Long, Integer, CloudInstance>> flatten(DriverMemoryNode mNode) {
//            LinkedList<Tuple3<Long, Integer, CloudInstance>> result = new LinkedList<>();
//            for (CharacteristicNode cNode: mNode.characteristicsNodes) {
//                Tuple3<Long, Integer, CloudInstance> newTuple = new Tuple3<>(mNode.nodeMemory, cNode.cores, cNode.instance);
//                result.add(newTuple);
//            }
//            return result;
//        }
//    }
//
//    static class ExecutorMemoryNode {
//        long nodeMemory; // in MB
//        int numberExecutors;
//        LinkedList<CharacteristicNode> characteristicsNodes;
//
//        public ExecutorMemoryNode(long memory, int number) {
//            this.nodeMemory = memory;
//            this.numberExecutors = number;
//            characteristicsNodes = new LinkedList<>();
//        }
//
//        LinkedList<Tuple4<Long, Integer, Integer, CloudInstance>> flatten(ExecutorMemoryNode mNode) {
//            LinkedList<Tuple4<Long, Integer, Integer, CloudInstance>> result = new LinkedList<>();
//            for (CharacteristicNode cNode: mNode.characteristicsNodes) {
//                Tuple4<Long, Integer, Integer, CloudInstance> newTuple = new Tuple4<>(mNode.nodeMemory, mNode.numberExecutors, cNode.cores, cNode.instance);
//                result.add(newTuple);
//            }
//            return result;
//        }
//    }
//
//    public static class OldDriverNode {
//        CloudInstance instance;
//        Map<InstanceType, Map<InstanceSize, ExecutorNode>> executors;
//    }
//
//    public static class ExecutorNode {
//        CloudInstance instance;
//        int[] numberExecutors;
//    }
//
//    /**
//     * This class represents the search space for the enumeration.
//     * An object of this class is to be returned by the {@code enumerate()}
//     * method of the {@code Enumerator} class.
//     */
//    public static class ConfigurationSpace extends HashMap<InstanceType, HashMap<InstanceSize, OldDriverNode>> {
//        public static ArrayList<ConfigurationPoint> flattenSpace(ConfigurationSpace space) {
//            ArrayList<ConfigurationPoint> result = new ArrayList<>();
//            // parse all tree roots
//            for (HashMap<InstanceSize, OldDriverNode> root:
//                    space.values()) {
//                // parse all driver nodes for each root
//                for (OldDriverNode driverNode: root.values()) {
//                    // parse all possible executor instance types for each driver node
//                    for (Map<InstanceSize, ExecutorNode> executorTypes:
//                            driverNode.executors.values()) {
//                        // parse all executor nodes for each executor instance type
//                        for (ExecutorNode executorNode:
//                                executorTypes.values()) {
//                            // parse all possible number of executors for each executor instance
//                            for (int numberExecutors: executorNode.numberExecutors) {
//                                ConfigurationPoint point = new ConfigurationPoint();
//                                point.driverInstance = driverNode.instance;
//                                point.executorInstance = executorNode.instance;
//                                point.numberExecutors = numberExecutors;
//                                result.add(point);
//                            }
//                        }
//                    }
//                }
//            }
//            return result;
//        }
//    }
}
