package org.apache.sysds.api.ropt.new_impl;


import org.apache.sysds.api.ropt.CloudOptimizerUtils;

/**
 * This class describes the configurations of a single VM instance.
 * The idea is to use this class to represent instances of different
 * cloud hypervisors - currently supporting only EC2 instances by AWS.
 */
public class CloudInstance {
    private final String instanceName;
    private final long memoryMB;
    private final int vCPUCores;
    private final double pricePerHour;
    private final double gFlops;
    private final double diskSpeed;
    private final double memorySpeed;
    public CloudInstance(String instanceName, long memoryMB, int vCPUCores, double gFlops, double diskSpeed, double memorySpeed, double pricePerHour) {
        this.instanceName = instanceName;
        this.memoryMB = memoryMB;
        this.vCPUCores = vCPUCores;
        this.gFlops = gFlops;
        this.diskSpeed = diskSpeed;
        this.memorySpeed = memorySpeed;
        this.pricePerHour = pricePerHour;
    }

    public String getInstanceName() {
        return instanceName;
    }

    /**
     * Returns the memory of the instance in MB.
     * @return
     */
    public long getMemory() {
        return memoryMB;
    }

    public long getAvailableMemory() {
        // NOTE: reconsider the usage of MEM_FACTOR
        return (long) (memoryMB/ CloudOptimizerUtils.MEM_FACTOR);
    }

    /**
     * Returns the memory per core of the instance in MB.
     * @return
     */
    public long getMemoryPerCore() {
        return memoryMB/vCPUCores;
    }

    /**
     * Returns the number of virtual CPU cores of the instance.
     * @return
     */
    public int getVCPUs() {
        return vCPUCores;
    }

    /**
     * Returns the price per hour of the instance.
     * @return
     */
    public double getPrice() {
        return pricePerHour;
    }

    /**
     * Returns the number of FLOPS of the instance.
     * @return
     */
    public long getFLOPS() {
        return (long) (gFlops*1024)*1024*1024;
    }

    /**
     * Returns the disk speed of the instance in MB/s.
     * @return
     */
    public double getDiskSpeed() {
        return diskSpeed;
    }

    /**
     * Returns the memory speed of the instance in MB/s.
     * @return
     */
    public double getMemorySpeed() {
        return memorySpeed;
    }
}
