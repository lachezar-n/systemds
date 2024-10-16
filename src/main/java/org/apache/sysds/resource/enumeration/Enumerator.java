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


package org.apache.sysds.resource.enumeration;

import org.apache.sysds.resource.AWSUtils;
import org.apache.sysds.resource.CloudInstance;
import org.apache.sysds.resource.CloudUtils;
import org.apache.sysds.resource.ResourceCompiler;
import org.apache.sysds.resource.cost.CostEstimationException;
import org.apache.sysds.resource.cost.CostEstimator;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.resource.enumeration.EnumerationUtils.InstanceSearchSpace;
import org.apache.sysds.resource.enumeration.EnumerationUtils.ConfigurationPoint;
import org.apache.sysds.resource.enumeration.EnumerationUtils.SolutionPoint;

import java.io.IOException;
import java.util.*;

public abstract class Enumerator {

	public enum EnumerationStrategy {
		GridBased, // considering all combination within a given range of configuration
		InterestBased, // considering only combinations of configurations with memory budge close to memory estimates
	}

	public enum OptimizationStrategy {
		MinTime, // always prioritize execution time minimization
		MinPrice, // always prioritize operation price minimization
	}

	// Static variables ------------------------------------------------------------------------------------------------

	public static final int DEFAULT_MIN_EXECUTORS = 0; // Single Node execution allowed
	/**
	 * A reasonable upper bound for the possible number of executors
	 * is required to set limits for the search space and to avoid
	 * evaluating cluster configurations that most probably would
	 * have too high distribution overhead
	 */
	public static final int DEFAULT_MAX_EXECUTORS = 200;

	// limit for the ratio number of executor and number
	// of executor per executor
	public static final int MAX_LEVEL_PARALLELISM = 1000;

	/** Time/Monetary delta for considering optimal solutions as fraction */
	public static final double COST_DELTA_FRACTION = 0.02;

	// Instance variables ----------------------------------------------------------------------------------------------
	HashMap<String, CloudInstance> instances = null;
	Program program;
	CloudUtils utils;
	EnumerationStrategy enumStrategy;
	OptimizationStrategy optStrategy;
	private final double maxTime;
	private final double maxPrice;
	protected final int minExecutors;
	protected final int maxExecutors;
	protected final Set<CloudUtils.InstanceType> instanceTypesRange;
	protected final Set<CloudUtils.InstanceSize> instanceSizeRange;

	protected final InstanceSearchSpace driverSpace = new InstanceSearchSpace();
	protected final InstanceSearchSpace executorSpace = new InstanceSearchSpace();
	protected ArrayList<SolutionPoint> solutionPool = new ArrayList<>();

	// Initialization functionality ------------------------------------------------------------------------------------

	public Enumerator(Builder builder) {
		if (builder.provider.equals(CloudUtils.CloudProvider.AWS)) {
			utils = new AWSUtils();
		} // as of now no other provider is supported
		this.program = builder.program;
		this.enumStrategy = builder.enumStrategy;
		this.optStrategy = builder.optStrategy;
		this.maxTime = builder.maxTime;
		this.maxPrice = builder.maxPrice;
		this.minExecutors = builder.minExecutors;
		this.maxExecutors = builder.maxExecutors;
		this.instanceTypesRange = builder.instanceTypesRange;
		this.instanceSizeRange = builder.instanceSizeRange;
	}

	/**
	 * Meant to be used for testing purposes
	 * @return ?
	 */
	public HashMap<String, CloudInstance> getInstances() {
		return instances;
	}

	/**
	 * Meant to be used for testing purposes
	 * @return ?
	 */
	public InstanceSearchSpace getDriverSpace() {
		return driverSpace;
	}

	/**
	 * Meant to be used for testing purposes
	 * @param inputSpace ?
	 */
	public void setDriverSpace(InstanceSearchSpace inputSpace) {
		driverSpace.putAll(inputSpace);
	}

	/**
	 * Meant to be used for testing purposes
	 * @return ?
	 */
	public InstanceSearchSpace getExecutorSpace() {
		return executorSpace;
	}

	/**
	 * Meant to be used for testing purposes
	 * @param inputSpace ?
	 */
	public void setExecutorSpace(InstanceSearchSpace inputSpace) {
		executorSpace.putAll(inputSpace);
	}

	/**
	 * Meant to be used for testing purposes
	 * @return ?
	 */
	public ArrayList<SolutionPoint> getSolutionPool() {
		return solutionPool;
	}

	/**
	 * Meant to be used for testing purposes
	 * @param solutionPool ?
	 */
	public void setSolutionPool(ArrayList<SolutionPoint> solutionPool) {
		this.solutionPool = solutionPool;
	}

	/**
	 * Setting the available VM instances manually.
	 * Meant to be used for testing purposes.
	 * @param inputInstances initialized map of instances
	 */
	public void setInstanceTable(HashMap<String, CloudInstance> inputInstances) {
		instances = new HashMap<>();
		for (String key: inputInstances.keySet()) {
			if (instanceTypesRange.contains(utils.getInstanceType(key))
					&& instanceSizeRange.contains(utils.getInstanceSize(key))) {
				instances.put(key, inputInstances.get(key));
			}
		}
	}

	/**
	 * Loads the info table for the available VM instances
	 * and filters out the instances that are not contained
	 * in the set of allowed instance types and sizes.
	 *
	 * @param path csv file with instances' info
	 * @throws IOException in case the loading part fails at reading the csv file
	 */
	public void loadInstanceTableFile(String path) throws IOException {
		HashMap<String, CloudInstance> allInstances = utils.loadInstanceInfoTable(path);
		instances = new HashMap<>();
		for (String key: allInstances.keySet()) {
			if (instanceTypesRange.contains(utils.getInstanceType(key))
				&& instanceSizeRange.contains(utils.getInstanceSize(key))) {
				instances.put(key, allInstances.get(key));
			}
		}
	}

	// Main functionality ----------------------------------------------------------------------------------------------

	/**
	 * Called once to enumerate the search space for
	 * VM instances for driver or executor nodes.
	 * These instances are being represented as
	 */
	public abstract void preprocessing();

	/**
	 * Called once after preprocessing to fill the
	 * pool with optimal solutions by parsing
	 * the enumerated search space.
	 * Within its execution the number of potential
	 * executor nodes is being estimated (enumerated)
	 * dynamically for each parsed executor instance.
	 */
	public void processing() {
		ConfigurationPoint configurationPoint;
		SolutionPoint optSolutionPoint = new SolutionPoint(
				new ConfigurationPoint(null, null, -1),
				Double.MAX_VALUE,
				Double.MAX_VALUE
		);
		for (Map.Entry<Long, TreeMap<Integer, LinkedList<CloudInstance>>> dMemoryEntry: driverSpace.entrySet()) {
			// loop over the search space to enumerate the driver configurations
			for (Map.Entry<Integer, LinkedList<CloudInstance>> dCoresEntry: dMemoryEntry.getValue().entrySet()) {
				// single node execution mode
				if (evaluateSingleNodeExecution(dMemoryEntry.getKey())) {
					program = ResourceCompiler.doFullRecompilation(
							program,
							dMemoryEntry.getKey(),
							dCoresEntry.getKey()
					);
					for (CloudInstance dInstance: dCoresEntry.getValue()) {
						configurationPoint = new ConfigurationPoint(dInstance);
						updateOptimalSolution(optSolutionPoint, configurationPoint);
					}
				}
				// enumeration for distributed execution
				for (Map.Entry<Long, TreeMap<Integer, LinkedList<CloudInstance>>> eMemoryEntry: executorSpace.entrySet()) {
					// loop over the search space to enumerate the executor configurations
					for (Map.Entry<Integer, LinkedList<CloudInstance>> eCoresEntry: eMemoryEntry.getValue().entrySet()) {
						List<Integer> numberExecutorsSet = estimateRangeExecutors(eMemoryEntry.getKey(), eCoresEntry.getKey());
						// Spark execution mode
						for (int numberExecutors: numberExecutorsSet) {
							// TODO: avoid full recompilation when the driver memory is not changed
							program = ResourceCompiler.doFullRecompilation(
									program,
									dMemoryEntry.getKey(),
									dCoresEntry.getKey(),
									numberExecutors,
									eMemoryEntry.getKey(),
									eCoresEntry.getKey()
							);
							// TODO: avoid full program cost estimation when the driver instance is not changed
							for (CloudInstance dInstance: dCoresEntry.getValue()) {
								for (CloudInstance eInstance: eCoresEntry.getValue()) {
									configurationPoint = new ConfigurationPoint(dInstance, eInstance, numberExecutors);
									updateOptimalSolution(optSolutionPoint, configurationPoint);
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Deciding in the overall best solution out
	 * of the filled pool of potential solutions
	 * after processing.
	 * @return single optimal cluster configuration
	 */
	public SolutionPoint postprocessing() {
		if (solutionPool.isEmpty()) {
			throw new RuntimeException("Calling postprocessing() should follow calling processing()");
		}
		SolutionPoint optSolution = solutionPool.get(0);
		double bestCost = Double.MAX_VALUE;
		for (SolutionPoint solution: solutionPool) {
			double combinedCost = solution.monetaryCost * solution.timeCost;
			if (combinedCost < bestCost) {
				optSolution = solution;
				bestCost = combinedCost;
			} else if (combinedCost == bestCost) {
				// the ascending order of the searching spaces for driver and executor
				// instances ensures that in case of equally good optimal solutions
				// the first one has at least resource characteristics.
				// This, however, is not valid for the number of executors
				if (solution.numberExecutors < optSolution.numberExecutors) {
					optSolution = solution;
					bestCost = combinedCost;
				}
			}
		}
		return optSolution;
	}

	// Helper methods --------------------------------------------------------------------------------------------------

	public abstract boolean evaluateSingleNodeExecution(long driverMemory);

	/**
	 * Estimates the minimum and maximum number of
	 * executors based on given VM instance characteristics
	 * and on the enumeration strategy
	 *
	 * @param executorMemory memory of currently considered executor instance
	 * @param executorCores  CPU of cores of currently considered executor instance
	 * @return - [min, max]
	 */
	public abstract ArrayList<Integer> estimateRangeExecutors(long executorMemory, int executorCores);

	/**
	 * Estimates the time cost for the current program based on the
	 * given cluster configurations and following this estimation
	 * it calculates the corresponding monetary cost.
	 * @param point - cluster configuration used for (re)compiling the current program
	 * @return - [time cost, monetary cost]
	 */
	private double[] getCostEstimate(ConfigurationPoint point) {
		// get the estimated time cost
		double timeCost;
		try {
			// estimate execution time of the current program
			// TODO: pass further relevant cluster configurations to cost estimator after extending it
			//  like for example: FLOPS, I/O and networking speed
			timeCost = CostEstimator.estimateExecutionTime(program) + CloudUtils.DEFAULT_CLUSTER_LAUNCH_TIME;
		} catch (CostEstimationException e) {
			throw new RuntimeException(e.getMessage());
		}
		// calculate monetary cost
		double monetaryCost = utils.calculateClusterPrice(point, timeCost);
		return new double[] {timeCost, monetaryCost}; // time cost, monetary cost
	}

	/**
	 * Invokes the estimation of the time and monetary cost
	 * based on the compiled program and the given cluster configurations.
	 * Following the optimization strategy, the given current optimal solution
	 * and the new cost estimation, it decides if the given cluster configuration
	 * can be potential optimal solution having lower cost or such a cost
	 * that is negligibly higher than the current lowest one.
	 * @param currentOptimal solution point with the lowest cost
	 * @param newPoint new cluster configuration for estimation
	 */
	private void updateOptimalSolution(SolutionPoint currentOptimal, ConfigurationPoint newPoint) {
		// TODO: clarify if setting max time and max price simultaneously makes really sense
		SolutionPoint newPotentialSolution;
		boolean replaceCurrentOptimal = false;
		double[] newCost = getCostEstimate(newPoint);
		if (optStrategy == OptimizationStrategy.MinTime) {
			if (newCost[1] > maxPrice || newCost[0] >= currentOptimal.timeCost * (1 + COST_DELTA_FRACTION)) {
				return;
			}
			if (newCost[0] < currentOptimal.timeCost) replaceCurrentOptimal = true;
		} else if (optStrategy == OptimizationStrategy.MinPrice) {
			if (newCost[0] > maxTime || newCost[1] >= currentOptimal.monetaryCost * (1 + COST_DELTA_FRACTION)) {
				return;
			}
			if (newCost[1] < currentOptimal.monetaryCost) replaceCurrentOptimal = true;
		}
		newPotentialSolution = new SolutionPoint(newPoint, newCost[0], newCost[1]);
		solutionPool.add(newPotentialSolution);
		if (replaceCurrentOptimal) {
			currentOptimal.update(newPoint, newCost[0], newCost[1]);
		}
	}

	// Class builder ---------------------------------------------------------------------------------------------------

	public static class Builder {
		private final CloudUtils.CloudProvider provider = CloudUtils.CloudProvider.AWS; // currently default and only choice
		private Program program;
		private EnumerationStrategy enumStrategy = null;
		private OptimizationStrategy optStrategy = null;
		private double maxTime = -1d;
		private double maxPrice = -1d;
		private int minExecutors = DEFAULT_MIN_EXECUTORS;
		private int maxExecutors = DEFAULT_MAX_EXECUTORS;
		private Set<CloudUtils.InstanceType> instanceTypesRange = null;
		private Set<CloudUtils.InstanceSize> instanceSizeRange = null;

		// GridBased specific ------------------------------------------------------------------------------------------
		private int stepSizeExecutors = 1;
		private int expBaseExecutors = -1; // flag for exp. increasing number of executors if -1
		// InterestBased specific --------------------------------------------------------------------------------------
		private boolean fitDriverMemory = true;
		private boolean fitBroadcastMemory = true;
		private boolean checkSingleNodeExecution = false;
		private boolean fitCheckpointMemory = false;
		public Builder() {}

		public Builder withRuntimeProgram(Program program) {
			this.program = program;
			return this;
		}

		public Builder withEnumerationStrategy(EnumerationStrategy strategy) {
			this.enumStrategy = strategy;
			return this;
		}

		public Builder withOptimizationStrategy(OptimizationStrategy strategy) {
			this.optStrategy = strategy;
			return this;
		}

		public Builder withTimeLimit(double time) {
			if (time < CloudUtils.MINIMAL_EXECUTION_TIME) {
				throw new IllegalArgumentException(CloudUtils.MINIMAL_EXECUTION_TIME +
						"s is the minimum target execution time.");
			}
			this.maxTime = time;
			return this;
		}

		public Builder withBudget(double price) {
			if (price <= 0) {
				throw new IllegalArgumentException("The given budget (target price) should be positive");
			}
			this.maxPrice = price;
			return this;
		}

		public Builder withNumberExecutorsRange(int min, int max) {
			this.minExecutors = min;
			this.maxExecutors = max;
			return this;
		}

		public Builder withInstanceTypeRange(String[] instanceTypes) {
			this.instanceTypesRange = typeRangeFromStrings(instanceTypes);
			return this;
		}

		public Builder withInstanceSizeRange(String[] instanceSizes) {
			this.instanceSizeRange = sizeRangeFromStrings(instanceSizes);
			return this;
		}

		public Builder withStepSizeExecutor(int stepSize) {
			this.stepSizeExecutors = stepSize;
			return this;
		}


		public Builder withFitDriverMemory(boolean fitDriverMemory) {
			this.fitDriverMemory = fitDriverMemory;
			return this;
		}

		public Builder withFitBroadcastMemory(boolean fitBroadcastMemory) {
			this.fitBroadcastMemory = fitBroadcastMemory;
			return this;
		}

		public Builder withCheckSingleNodeExecution(boolean checkSingleNodeExecution) {
			this.checkSingleNodeExecution = checkSingleNodeExecution;
			return this;
		}

		public Builder withFitCheckpointMemory(boolean fitCheckpointMemory) {
			this.fitCheckpointMemory = fitCheckpointMemory;
			return this;
		}

		public Builder withExpBaseExecutors(int expBaseExecutors) {
			if (expBaseExecutors != -1 && expBaseExecutors < 2) {
				throw new IllegalArgumentException("Given exponent base for number of executors should be -1 or bigger than 1.");
			}
			this.expBaseExecutors = expBaseExecutors;
			return this;
		}

		public Enumerator build() {
			if (this.program == null) {
				throw new IllegalArgumentException("Providing runtime program is required");
			}

			if (instanceTypesRange == null) {
				instanceTypesRange = EnumSet.allOf(CloudUtils.InstanceType.class);
			}

			if (instanceSizeRange == null) {
				instanceSizeRange = EnumSet.allOf(CloudUtils.InstanceSize.class);
			}

			switch (optStrategy) {
				case MinTime:
					if (this.maxPrice < 0) {
						throw new IllegalArgumentException("Budget not specified but required " +
								"for the chosen optimization strategy: " + optStrategy);
					}
					break;
				case MinPrice:
					if (this.maxTime < 0) {
						throw new IllegalArgumentException("Time limit not specified but required " +
								"for the chosen optimization strategy: " + optStrategy);
					}
					break;
				default: // in case optimization strategy was not configured
					throw new IllegalArgumentException("Setting an optimization strategy is required.");
			}

			switch (enumStrategy) {
				case GridBased:
					return new GridBasedEnumerator(this, stepSizeExecutors, expBaseExecutors);
				case InterestBased:
					if (fitCheckpointMemory && expBaseExecutors != -1) {
						throw new IllegalArgumentException("Number of executors cannot be fitted on the checkpoint estimates and increased exponentially simultaneously.");
					}
					return new InterestBasedEnumerator(this, fitDriverMemory, fitBroadcastMemory, checkSingleNodeExecution, fitCheckpointMemory);
				default:
					throw new IllegalArgumentException("Setting an enumeration strategy is required.");
			}
		}

		protected static Set<CloudUtils.InstanceType> typeRangeFromStrings(String[] types) {
			Set<CloudUtils.InstanceType> result = EnumSet.noneOf(CloudUtils.InstanceType.class);
			for (String typeAsString: types) {
				CloudUtils.InstanceType type = CloudUtils.InstanceType.customValueOf(typeAsString); // can throw IllegalArgumentException
				result.add(type);
			}
			return result;
		}

		protected static Set<CloudUtils.InstanceSize> sizeRangeFromStrings(String[] sizes) {
			Set<CloudUtils.InstanceSize> result = EnumSet.noneOf(CloudUtils.InstanceSize.class);
			for (String sizeAsString: sizes) {
				CloudUtils.InstanceSize size = CloudUtils.InstanceSize.customValueOf(sizeAsString); // can throw IllegalArgumentException
				result.add(size);
			}
			return result;
		}
	}
}
