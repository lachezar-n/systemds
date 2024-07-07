package org.apache.sysds.resource.enumeration;
import org.apache.sysds.resource.enumeration.CloudUtils.InstanceType;
import org.apache.sysds.resource.enumeration.CloudUtils.InstanceSize;

import java.util.*;

public class EnumeratorRangeBased extends Enumerator {
    private final Set<InstanceType> instanceTypesRange;
    private final Set<InstanceSize> instanceSizeRange;


    public EnumeratorRangeBased(Builder builder, InstanceType[] instanceTypesRange, InstanceSize[] instanceSizeRange, int minExecutors, int maxExecutors) {
        super(builder);
        this.instanceTypesRange = new HashSet<>(Arrays.asList(Objects.requireNonNullElseGet(instanceTypesRange, InstanceType::values)));
        this.instanceSizeRange = new HashSet<>(Arrays.asList(instanceSizeRange));
    }

    /**
     * TODO: Think of possibilities of improving the performance by skipping "bigger" instances
     *  if there is no chance of improving the execution time within the given budget or no time
     *  improvement is expected for this more expensive machines/configurations
     * @return the optimal solution
     */
    @Override
    protected SolutionPoint enumerate() {
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
