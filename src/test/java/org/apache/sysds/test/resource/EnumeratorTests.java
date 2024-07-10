package org.apache.sysds.test.resource;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.sysds.resource.enumeration.CloudInstance;
import org.apache.sysds.resource.enumeration.CloudUtils;
import org.apache.sysds.resource.enumeration.Enumerator;
import org.apache.sysds.resource.enumeration.EnumeratorGridBased;
import org.apache.sysds.runtime.controlprogram.Program;
import org.junit.Assert;
import org.junit.Test;

public class EnumeratorTests {

    @Test
    public void GridBasedEnumerationTest() throws NoSuchFieldException, IllegalAccessException {
        Program emptyProgram = new Program();

        Enumerator gridBasedEnumerator = (new Enumerator.Builder())
                .withRuntimeProgram(emptyProgram)
                .withEnumerationStrategy(Enumerator.EnumerationStrategy.GridBased)
                .withOptimizationStrategy(Enumerator.OptimizationStrategy.MinPrice)
                .withTimeLimit(Double.MAX_VALUE)
                .withCloudProvider(CloudUtils.CloudProvider.AWS)
                .withExecutorNumberRange(0, 2)
                .build();

        gridBasedEnumerator.setInstanceTable(TestingUtils.getSimpleCloudInstanceMap());
        Enumerator.SolutionPoint solution = gridBasedEnumerator.enumerate();

        Assert.assertEquals("m5.xlarge", solution.driverInstance.getInstanceName());
        Assert.assertEquals(0, solution.numberExecutors);
    }

    @Test
    public void MemoryBasedEnumerationTest() throws NoSuchFieldException, IllegalAccessException {
        Program emptyProgram = new Program();

        Enumerator gridBasedEnumerator = (new Enumerator.Builder())
                .withRuntimeProgram(emptyProgram)
                .withEnumerationStrategy(Enumerator.EnumerationStrategy.MemoryBased)
                .withOptimizationStrategy(Enumerator.OptimizationStrategy.MinPrice)
                .withTimeLimit(Double.MAX_VALUE)
                .withCloudProvider(CloudUtils.CloudProvider.AWS)
                .withExecutorNumberRange(0, 2)
                .build();

        gridBasedEnumerator.setInstanceTable(TestingUtils.getSimpleCloudInstanceMap());
        Enumerator.SolutionPoint solution = gridBasedEnumerator.enumerate();
        // expect the initial values for the optimal solution since 0 estimates
        Assert.assertNull(solution.driverInstance);
        Assert.assertEquals(-1, solution.numberExecutors);
    }

}
