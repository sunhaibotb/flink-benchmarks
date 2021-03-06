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

package org.apache.flink.benchmark.full;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.benchmark.FlinkEnvironmentContext;
import org.apache.flink.benchmark.SerializationFrameworkMiniBenchmarks;
import org.apache.flink.benchmark.functions.BaseSourceWithKeyRange;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import com.twitter.chill.protobuf.ProtobufSerializer;
import com.twitter.chill.thrift.TBaseSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;

/**
 * Benchmark for serializing POJOs and Tuples with different serialization frameworks.
 */
public class SerializationFrameworkAllBenchmarks extends SerializationFrameworkMiniBenchmarks {

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + SerializationFrameworkAllBenchmarks.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerPojoWithoutRegistration(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerKryoWithoutRegistration(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		env.getConfig().enableForceKryo();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerAvroReflect(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		env.getConfig().enableForceAvro();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerKryoThrift(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.enableForceKryo();
		executionConfig.addDefaultKryoSerializer(org.apache.flink.benchmark.thrift.MyPojo.class, TBaseSerializer.class);
		executionConfig.addDefaultKryoSerializer(org.apache.flink.benchmark.thrift.MyOperation.class, TBaseSerializer.class);

		env.addSource(new ThriftPojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerKryoProtobuf(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.enableForceKryo();
		executionConfig.registerTypeWithKryoSerializer(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo.class, ProtobufSerializer.class);
		executionConfig.registerTypeWithKryoSerializer(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.class, ProtobufSerializer.class);

		env.addSource(new ProtobufPojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	/**
	 * Source emitting a {@link org.apache.flink.benchmark.thrift.MyPojo POJO} generated by an Apache Thrift schema.
	 */
	public static class ThriftPojoSource extends BaseSourceWithKeyRange<org.apache.flink.benchmark.thrift.MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

		private transient org.apache.flink.benchmark.thrift.MyPojo template;

		public ThriftPojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			template = new org.apache.flink.benchmark.thrift.MyPojo(
					0,
					"myName",
					Arrays.asList("op1", "op2", "op3", "op4"),
					Arrays.asList(
							new org.apache.flink.benchmark.thrift.MyOperation(1, "op1"),
							new org.apache.flink.benchmark.thrift.MyOperation(2, "op2"),
							new org.apache.flink.benchmark.thrift.MyOperation(3, "op3")),
					1,
					2,
					3);
			template.setSomeObject("null");
		}

		@Override
		protected org.apache.flink.benchmark.thrift.MyPojo getElement(int keyId) {
			template.setId(keyId);
			return template;
		}
	}

	/**
	 * Source emitting a {@link org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo POJO} generated by a Protobuf schema.
	 */
	public static class ProtobufPojoSource extends BaseSourceWithKeyRange<org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

			private transient org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo template;

		public ProtobufPojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			template = org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo.newBuilder()
					.setId(0)
					.setName("myName")
					.addAllOperationName(Arrays.asList("op1", "op2", "op3", "op4"))
					.addOperations(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.newBuilder()
							.setId(1)
							.setName("op1"))
					.addOperations(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.newBuilder()
							.setId(2)
							.setName("op2"))
					.addOperations(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.newBuilder()
							.setId(3)
							.setName("op3"))
					.setOtherId1(1)
					.setOtherId2(2)
					.setOtherId3(3)
					.setSomeObject("null")
					.build();
		}

		@Override
		protected org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo getElement(int keyId) {
			return org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo.newBuilder(template)
					.setId(keyId)
					.build();
		}
	}
}
