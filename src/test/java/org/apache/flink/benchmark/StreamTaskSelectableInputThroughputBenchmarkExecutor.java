/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.streaming.runtime.io.benchmark.StreamTaskSelectableInputThroughputBenchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * JMH throughput benchmark runner.
 */
@Warmup(iterations = StreamTaskSelectableInputThroughputBenchmarkExecutor.WARMUP_ITERATIONS)
@Measurement(iterations = StreamTaskSelectableInputThroughputBenchmarkExecutor.MEASUREMENT_ITERATIONS)
@OperationsPerInvocation(value = StreamTaskSelectableInputThroughputBenchmarkExecutor.RECORDS_PER_INVOCATION)
public class StreamTaskSelectableInputThroughputBenchmarkExecutor extends BenchmarkBase {

	static final int WARMUP_ITERATIONS = 10;

	static final int MEASUREMENT_ITERATIONS = 10;

	static final int RECORDS_PER_INVOCATION = 100_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + StreamTaskSelectableInputThroughputBenchmarkExecutor.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public long taskInputThroughput(MultiEnvironment context) throws Exception {
		return context.executeBenchmark();
	}

	/**
	 * Setup for the benchmark(s).
	 */
	@State(Thread)
	public static class MultiEnvironment extends StreamTaskSelectableInputThroughputBenchmark {

		//@Param({"2,1,ANY", "2,1,SEQUENTIAL"})
		@Param({"2,1,ANY"})
		public String gatesChannelsReadingType;

		@Setup
		public void setUp() throws Exception {
			int numGatesPerInput = parseIntParameter(gatesChannelsReadingType, 0);
			int numChannelsPerGate = parseIntParameter(gatesChannelsReadingType, 1);
			ReadingType readingType = parseReadingType(gatesChannelsReadingType);

			int totalChannels = 2 * numGatesPerInput * numChannelsPerGate;
			checkState(RECORDS_PER_INVOCATION % totalChannels == 0);
			long numRecordsPerChannel = RECORDS_PER_INVOCATION / totalChannels;

			super.setUp(numGatesPerInput, numChannelsPerGate, numRecordsPerChannel,	readingType);
		}

		private static int parseIntParameter(String channels, int index) {
			String[] parameters = channels.split(",");
			checkArgument(parameters.length > index);
			return Integer.parseInt(parameters[index]);
		}

		private static ReadingType parseReadingType(String channelsReadingType) {
			String[] parameters = channelsReadingType.split(",");
			checkArgument(parameters.length > 2);
			return ReadingType.valueOf(parameters[2]);
		}

		@TearDown
		public void tearDown() {
			super.tearDown();
		}
	}
}
