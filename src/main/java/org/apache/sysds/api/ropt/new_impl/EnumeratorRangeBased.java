package org.apache.sysds.api.ropt.new_impl;
import org.apache.sysds.api.ropt.new_impl.CloudUtils.InstanceType;
import org.apache.sysds.api.ropt.new_impl.CloudUtils.InstanceSize;

import java.util.*;

public class EnumeratorRangeBased extends Enumerator {
    private final Set<InstanceType> instanceTypesRange;
    private final Set<InstanceSize> instanceSizeRange;
    private final int minExecutors;
    private final int maxExecutors;

    public EnumeratorRangeBased(Builder builder, InstanceType[] instanceTypesRange, InstanceSize[] instanceSizeRange, int minExecutors, int maxExecutors) {
        super(builder);
        this.instanceTypesRange = new HashSet<>(Arrays.asList(Objects.requireNonNullElseGet(instanceTypesRange, InstanceType::values)));
        this.instanceSizeRange = new HashSet<>(Arrays.asList(instanceSizeRange));
        this.minExecutors = minExecutors;
        this.maxExecutors = maxExecutors;
    }

    /**
     * TODO: Think of possibilities of improving the performance by skipping "bigger" instances
     *  if there is no chance of improving the execution time within the given budget or no time
     *  improvement is expected for this more expensive machines/configurations
     * @return the optimal solution
     */
    @Override
    protected ConfigurationPoint enumerate() {
        for (String driverInstance: instances.keySet()) {
            InstanceType driverType = utils.getTypeEnum(driverInstance);
            InstanceSize driverSize = utils.getSizeEnum(driverInstance);
            if (instanceTypesRange.contains(driverType) && instanceSizeRange.contains(driverSize)) {
                for (String executorInstance: instances.keySet()) {
                    InstanceType executorType = utils.getTypeEnum(executorInstance);
                    InstanceSize executorSize = utils.getSizeEnum(executorInstance);
                    if (instanceTypesRange.contains(executorType) && instanceSizeRange.contains(executorSize)) {
                        // TODO: here is impl. in case of removing optimalSolution()
                        for (int i = minExecutors; i <= maxExecutors; i++) {
                            ConfigurationPoint point = new ConfigurationPoint();
                            point.driverInstance = instances.get(driverInstance);
                            point.executorInstance = instances.get(executorInstance);
                            point.numberExecutors = i;

                            // TODO: update the current optimal solution with the result
                            double[] cost = getTimeEstimate(point); // {est. exec. time, est. spending}

                        }
                    }
                }
            }
        }

        return null; // TODO: replace after implementing the logic for deciding about the optimal solution
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
