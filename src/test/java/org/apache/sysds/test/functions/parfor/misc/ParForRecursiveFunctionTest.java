/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.functions.parfor.misc;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;

public class ParForRecursiveFunctionTest extends AutomatedTestBase
{
	private final static String TEST_NAME1 = "parfor_recursive";
	private final static String TEST_DIR = "functions/parfor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ParForRecursiveFunctionTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 20;
	private final static int cols = 10;
	private final static double sparsity = 1.0;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[]{"Rout"}));
	}

	@Test
	public void testParForCP() {
		runParforTest(TEST_NAME1, ExecMode.SINGLE_NODE);
	}
	
	@Test
	public void testParForHybrid() {
		runParforTest(TEST_NAME1, ExecMode.HYBRID);
	}
	
	private void runParforTest( String TEST_NAME, ExecMode type )
	{
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		ExecMode oldExec = setExecMode(type);
		
		try {
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain","-stats","-args", input("V"), output("R") };
			
			double[][] V = getRandomMatrix(rows, cols, 4, 4, sparsity, 3);
			writeInputMatrixWithMTD("V", V, true);
	
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1);
			
			//compare matrices
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromOutputDir("R");
			Assert.assertEquals(5d*rows*cols, dmlfile.get(new CellIndex(1,1)), eps);
		}
		finally {
			resetExecMode(oldExec);
		}
	}
}
