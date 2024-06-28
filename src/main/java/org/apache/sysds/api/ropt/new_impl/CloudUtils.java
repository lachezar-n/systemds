package org.apache.sysds.api.ropt.new_impl;

import org.apache.sysds.api.ropt.new_impl.CloudInstance;
import org.checkerframework.checker.units.qual.C;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public abstract class CloudUtils {
    public enum CloudProvider {
        AWS // potentially AZURE, GOOGLE
    }
    public enum InstanceType {
        // AWS EC2 instance
        C5, C5A, C6I, C6A, C6G, C7I, C7A, C7G, // compute optimized - vCores:mem~=1:2
        R5, R5A, R6I, R6A, R6G, R7I, R7A, R7G; // memory optimized - vCores:mem~=1:8
        // Potentially VM instance types for different Cloud providers
    }

    public enum InstanceSize {
        _XLARGE, _2XLARGE, _4XLARGE, _8XLARGE, _12XLARGE, _16XLARGE, _24XLARGE, _32XLARGE, _48XLARGE
        // Potentially VM instance sizes for different Cloud providers
    }

    public abstract boolean validateInstanceName(String instanceName);
    public abstract String getInstanceType(String instanceName);
    public abstract String getInstanceSize(String instanceName);

    public abstract InstanceType getTypeEnum(String s);

    public abstract InstanceSize getSizeEnum(String s);

    /**
     * This method calculates the cluster price based on the
     * estimated execution time and the cluster configuration.
     * @param time estimated execution time in seconds
     * @return price for the given time
     */
    public abstract double calculateClusterPrice(double time);

    public HashMap<String, CloudInstance> loadInstanceInfoTable(String instanceTablePath) throws IOException {
        HashMap<String, CloudInstance> result = new HashMap<>();
        int lineCount = 1;
        // try to open the file
        BufferedReader br = new BufferedReader(new FileReader(instanceTablePath));
        String parsedLine;
        // validate the file header
        parsedLine = br.readLine();
        if (!parsedLine.equals("API_Name,Memory,vCPUs,Family,Price,gFlops,diskSpeed,ramSpeed"))
            throw new IOException("Invalid CSV header inside: " + instanceTablePath);


        while ((parsedLine = br.readLine()) != null) {
            String[] values = parsedLine.split(",");
            if (values.length != 8 && validateInstanceName(values[0]))
                throw new IOException(String.format("Invalid CSV line(%d) inside: %s", lineCount, instanceTablePath));

            String API_Name = values[0];
            long Memory = (long)Double.parseDouble(values[1])*1024;
            int vCPUs = Integer.parseInt(values[2]);
            double Price = Double.parseDouble(values[4]);
            double gFlops = Double.parseDouble(values[5]);
            double diskSpeed = Double.parseDouble(values[6]);
            double ramSpeed = Double.parseDouble(values[7]);

            CloudInstance parsedInstance = new CloudInstance(
                    API_Name,
                    Memory,
                    vCPUs,
                    gFlops,
                    diskSpeed,
                    ramSpeed,
                    Price
            );
            result.put(API_Name, parsedInstance);
            lineCount++;
        }

        return result;
    }
}
