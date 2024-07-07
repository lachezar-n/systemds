package org.apache.sysds.resource.enumeration;

import scala.xml.dtd.DEFAULT;

public class AWSUtils extends CloudUtils {
    public static final String EC2_REGEX = "^([a-z]+)([0-9])(a|g|i?)([bdnez]*)\\.([a-z0-9]*)$";
    private static final double DEFAULT_CLUSTER_LAUNCH_TIME = 120; // seconds; NOTE: set always to at least 60 seconds
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
    public double calculateClusterPrice(Enumerator.ConfigurationPoint config, double time) {
        double pricePerSeconds = getClusterCostPerHour(config);
        return (DEFAULT_CLUSTER_LAUNCH_TIME + time) * pricePerSeconds;
    }

    private double getClusterCostPerHour(Enumerator.ConfigurationPoint config) {
        if (config.numberExecutors == 0) {
            return config.driverInstance.getPrice();
        }
        return config.driverInstance.getPrice() +
                config.executorInstance.getPrice()*config.numberExecutors;
    }
}
