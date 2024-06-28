package org.apache.sysds.api.ropt.new_impl;

import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.api.ropt.new_impl.CloudUtils.InstanceType;
import org.apache.sysds.api.ropt.new_impl.CloudUtils.InstanceSize;

import java.util.*;


public abstract class Enumerator {
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
    HashMap<String, CloudInstance> instances;
    Program program;
    CloudUtils utils;
    EnumerationStrategy enumStrategy;
    OptimizationStrategy optStrategy;
    double maxTime;
    double maxPrice;

    public Enumerator(Builder builder) {
        if (builder.provider.equals(CloudUtils.CloudProvider.AWS)) {
            utils = new AWSUtils();
        } // as of now no other provider is supported
        this.enumStrategy = builder.enumStrategy;
        this.optStrategy = builder.optStrategy;
        this.maxTime = builder.maxTime;
        this.maxPrice = builder.maxPrice;
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
    protected abstract ConfigurationPoint enumerate();

    protected double[] getTimeEstimate(ConfigurationPoint point) {
        // recompile by setting the Spark configs to the given point values
        // and then execute the cost estimator
        // estimate also the monastery cost
        return new double[] {0, 0}; // time cost, monetary cost
    }

    protected SolutionPoint updateOptimalSolution(SolutionPoint currentOptimal, SolutionPoint newPoint) {
        // TODO(1): clarify if setting max time and max price simultaneously makes really sense
        // TODO(2): consider more dynamic comparison, e.g.:
        //  -> MinTime: slightly longer execution time within the maxTime can have much lower price
        //  -> MinPrice: slightly higher operation cost within the maxCost can have much faster execution time
        if (optStrategy == OptimizationStrategy.MinTime) {
            if (newPoint.monetaryCost < maxPrice) {
                return (newPoint.timeCost < currentOptimal.timeCost) ? newPoint : currentOptimal;
            }
        } else if (optStrategy == OptimizationStrategy.MinPrice) {
            if (newPoint.timeCost < maxTime) {
                return (newPoint.monetaryCost < currentOptimal.monetaryCost) ? newPoint : currentOptimal;
            }
        }
        return currentOptimal;
    }

    static class ConfigurationPoint {
        CloudInstance driverInstance;
        CloudInstance executorInstance;
        int numberExecutors;
    }

    static class SolutionPoint extends ConfigurationPoint {
        double timeCost;
        double monetaryCost;
    }

    public static class Builder {
        private CloudUtils.CloudProvider provider = CloudUtils.CloudProvider.AWS; // currently default and only choice
        private EnumerationStrategy enumStrategy = null;
        private OptimizationStrategy optStrategy = null;
        private double maxTime = -1d;
        private double maxPrice = -1d;
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

        public Builder withExecutorNumberRange(int min, int max) {
            if (enumStrategy != EnumerationStrategy.RangeBased) {
                throw new IllegalArgumentException("Setting ranges is allowed only after " +
                        "specifying enumeration strategy ot type RangeBased");
            }
            args[2] = min;
            args[3] = max;
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

    // -----------------------------------------------------------------------------------------------------------------
    // OLD - TODO: delete after the implementation is stable

    public static class DriverNode  {
        CloudInstance instance;
        Map<InstanceType, Map<InstanceSize, ExecutorNode>> executors;
    }

    public static class ExecutorNode {
        CloudInstance instance;
        int[] numberExecutors;
    }

    /**
     * This class represents the search space for the enumeration.
     * An object of this class is to be returned by the {@code enumerate()}
     * method of the {@code Enumerator} class.
     */
    public static class ConfigurationSpace extends HashMap<InstanceType, HashMap<InstanceSize, DriverNode>> {
        public static ArrayList<ConfigurationPoint> flattenSpace(ConfigurationSpace space) {
            ArrayList<ConfigurationPoint> result = new ArrayList<>();
            // parse all tree roots
            for (HashMap<InstanceSize, DriverNode> root:
                    space.values()) {
                // parse all driver nodes for each root
                for (DriverNode driverNode: root.values()) {
                    // parse all possible executor instance types for each driver node
                    for (Map<InstanceSize, ExecutorNode> executorTypes:
                            driverNode.executors.values()) {
                        // parse all executor nodes for each executor instance type
                        for (ExecutorNode executorNode:
                                executorTypes.values()) {
                            // parse all possible number of executors for each executor instance
                            for (int numberExecutors: executorNode.numberExecutors) {
                                ConfigurationPoint point = new ConfigurationPoint();
                                point.driverInstance = driverNode.instance;
                                point.executorInstance = executorNode.instance;
                                point.numberExecutors = numberExecutors;
                                result.add(point);
                            }
                        }
                    }
                }
            }
            return result;
        }
    }
}
