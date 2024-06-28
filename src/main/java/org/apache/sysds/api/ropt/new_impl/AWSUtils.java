package org.apache.sysds.api.ropt.new_impl;

import org.apache.sysds.api.ropt.SearchSpace;
import org.apache.sysds.runtime.DMLRuntimeException;

public class AWSUtils extends CloudUtils {
    public static final String EC2_REGEX = "^([a-z]+)([0-9])(a|g|i?)([bdnez]*)\\.([a-z0-9]*)$";

    @Override
    public boolean validateInstanceName(String instanceName) {
        return instanceName.matches(EC2_REGEX);
    }

    @Override
    public String getInstanceType(String instanceName) {
        return instanceName.split("\\.")[0];
    }

    @Override
    public String getInstanceSize(String instanceName) {
        return instanceName.split("\\.")[1];
    }

    @Override
    public InstanceType getTypeEnum(String instanceName) {
        String typeAsStrung = getInstanceType(instanceName);
        return InstanceType.valueOf(typeAsStrung.toUpperCase());
    }

    @Override
    public InstanceSize getSizeEnum(String instanceName) {
        String sizeAsString = getInstanceSize(instanceName);
        return InstanceSize.valueOf("_" + sizeAsString.toUpperCase());
    }

    @Override
    public double calculateClusterPrice(double time) {
        return 0;
    }
}
