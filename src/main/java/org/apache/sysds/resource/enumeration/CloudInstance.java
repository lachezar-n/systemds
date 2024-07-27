package org.apache.sysds.resource.enumeration;

/**
 * This class describes the configurations of a single VM instance.
 * The idea is to use this class to represent instances of different
 * cloud hypervisors - currently supporting only EC2 instances by AWS.
 */
public class CloudInstance {
    private final String instanceName;
    private final long memory;
    private final int vCPUCores;
    private final double pricePerHour;
    private final double gFlops;
    private final double memorySpeed;
    private final double diskSpeed;
    private final double networkSpeed;
    public CloudInstance(String instanceName, long memory, int vCPUCores, double gFlops, double memorySpeed, double diskSpeed, double networkSpeed, double pricePerHour) {
        this.instanceName = instanceName;
        this.memory = memory;
        this.vCPUCores = vCPUCores;
        this.gFlops = gFlops;
        this.memorySpeed = memorySpeed;
        this.diskSpeed = diskSpeed;
        this.networkSpeed = networkSpeed;
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
        return memory;
    }

    /**
     * Returns the memory per core of the instance in MB.
     * @return
     */
    public long getMemoryPerCore() {
        return memory /vCPUCores;
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
     * Returns the memory speed/bandwidth of the instance in MB/s.
     * @return
     */
    public double getMemorySpeed() {
        return memorySpeed;
    }

    /**
     * Returns the disk speed/bandwidth of the instance in MB/s.
     * @return
     */
    public double getDiskSpeed() {
        return diskSpeed;
    }

    /**
     * Returns the network speed/bandwidth of the instance in MB/s.
     * @return
     */
    public double getNetworkSpeed() {
        return networkSpeed;
    }
}
