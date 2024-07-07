package org.apache.sysds.test.ropt;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.sysds.resource.enumeration.CloudInstance;
import org.apache.sysds.resource.enumeration.Enumerator;
import org.junit.Assert;
import org.junit.Test;


import static org.apache.sysds.test.ropt.TestingUtils.getSimpleCloudInstanceMap;

@SuppressWarnings("unchecked")
public class EnumeratorTests {

    @Test
    public void driverSpaceGenerationTest() throws NoSuchFieldException, IllegalAccessException {
        HashMap<String, CloudInstance> instances = getSimpleCloudInstanceMap();
        Enumerator.SearchSpace searchSpace = new Enumerator.SearchSpace();
        searchSpace.initSpace(instances);

        Field driverSpaceField = Enumerator.SearchSpace.class.getDeclaredField("driverSpace");
        driverSpaceField.setAccessible(true);
        TreeMap<Long, LinkedList<Enumerator.InstanceNode>> driverSpace = (TreeMap<Long, LinkedList<Enumerator.InstanceNode>>) driverSpaceField.get(searchSpace);
        long lastMemoryEntry = 0;
        for (Map.Entry<Long, LinkedList< Enumerator.InstanceNode>> entry: driverSpace.entrySet()) {
            Assert.assertTrue(lastMemoryEntry <= entry.getKey());
            lastMemoryEntry = entry.getKey();
            Enumerator.InstanceNode lastInstanceNodeEntry = null;
            for (Enumerator.InstanceNode node: entry.getValue()) {
                if (lastInstanceNodeEntry != null) {
                    Assert.assertTrue(lastInstanceNodeEntry.compareTo(node) < 0);
                }
                lastInstanceNodeEntry = node;
            }
        }
    }

}
