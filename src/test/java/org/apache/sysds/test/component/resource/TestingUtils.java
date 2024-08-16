package org.apache.sysds.test.component.resource;

import org.apache.sysds.resource.CloudInstance;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.sysds.resource.CloudUtils.GBtoBytes;

public class TestingUtils {
    public static void assertEqualsCloudInstances(CloudInstance expected, CloudInstance actual) {
        Assert.assertEquals(expected.getInstanceName(), actual.getInstanceName());
        Assert.assertEquals(expected.getMemory(), actual.getMemory());
        Assert.assertEquals(expected.getVCPUs(), actual.getVCPUs());
        Assert.assertEquals(expected.getFLOPS(), actual.getFLOPS());
        Assert.assertEquals(expected.getMemorySpeed(), actual.getMemorySpeed(), 0.0);
        Assert.assertEquals(expected.getDiskSpeed(), actual.getDiskSpeed(), 0.0);
        Assert.assertEquals(expected.getNetworkSpeed(), actual.getNetworkSpeed(), 0.0);
        Assert.assertEquals(expected.getPrice(), actual.getPrice(), 0.0);

    }

    public static HashMap<String, CloudInstance> getSimpleCloudInstanceMap() {
        HashMap<String, CloudInstance> instanceMap =  new HashMap<>();
        // fill the map wsearchStrategyh enough cloud instances to allow testing all search space dimension searchStrategyerations
        instanceMap.put("m5.xlarge", new CloudInstance("m5.xlarge", GBtoBytes(16), 4, 0.5, 0.0, 143.75, 160, 1.5));
        instanceMap.put("m5.2xlarge", new CloudInstance("m5.2xlarge", GBtoBytes(32), 8, 1.0, 0.0, 0.0, 0.0, 1.9));
        instanceMap.put("c5.xlarge", new CloudInstance("c5.xlarge", GBtoBytes(8), 4, 0.5, 0.0, 0.0, 0.0, 1.7));
        instanceMap.put("c5.2xlarge", new CloudInstance("c5.2xlarge", GBtoBytes(16), 8, 1.0, 0.0, 0.0, 0.0, 2.1));

        return instanceMap;
    }

    public static File generateTmpInstanceInfoTableFile() throws IOException {
        File tmpFile = File.createTempFile("systemds_tmp", ".csv");

        List<String> csvLines = Arrays.asList(
                "API_Name,Memory,vCPUs,gFlops,ramSpeed,diskSpeed,networkSpeed,Price",
                "m5.xlarge,16.0,4,0.5,0,143.75,160,1.5",
                "m5.2xlarge,32.0,8,1.0,0,0,0,1.9",
                "c5.xlarge,8.0,4,0.5,0,0,0,1.7",
                "c5.2xlarge,16.0,8,1.0,0,0,0,2.1"
        );
        Files.write(tmpFile.toPath(), csvLines);
        return tmpFile;
    }
}
