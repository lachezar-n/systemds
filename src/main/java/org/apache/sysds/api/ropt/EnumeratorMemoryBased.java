package org.apache.sysds.api.ropt;

public class EnumeratorMemoryBased extends Enumerator {
    public EnumeratorMemoryBased(Builder builder) {
        super(builder);
    }

    @Override
    protected SolutionPoint enumerate() {
        return null;
    }
}
