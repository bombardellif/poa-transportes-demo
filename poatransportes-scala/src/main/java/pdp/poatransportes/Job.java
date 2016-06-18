package pdp.poatransportes;

/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.common.WeightVector;
import org.apache.flink.ml.regression.MultipleLinearRegression;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */
		
		// Read data input from database
		List<Tuple3<Integer, Double, Double>> database = new ArrayList<>();
		database.add(new Tuple3<Integer, Double, Double>(43, -30.07207257673100000, -51.11791210867900000));
		database.add(new Tuple3<Integer, Double, Double>(43, -30.06895057673100000, -51.14392710867900000));
		database.add(new Tuple3<Integer, Double, Double>(43, -30.05572457673100000, -51.17224110867900000));
		database.add(new Tuple3<Integer, Double, Double>(43, -30.03799357673100000, -51.20369010867900000));
		database.add(new Tuple3<Integer, Double, Double>(43, -30.02316957673100000, -51.22079910867900000));
		Map<Integer, Vector<double[]>> mapLinesCoordinates = new HashMap<>();
		for (Tuple3<Integer, Double, Double> coordinate : database) {
			Vector<double[]> value = mapLinesCoordinates.get(coordinate.f0);
			if (value == null) {
				value = new Vector<>();
				mapLinesCoordinates.put(coordinate.f0, value);
			}
			
			value.add(new double[]{coordinate.f1, coordinate.f2});
		}
		
		// Read data input from phones (batch)
		List<Tuple5<Integer, Integer, Double, Double, Double>> input = new ArrayList<>();
		input.add(new Tuple5<Integer, Integer, Double, Double, Double>(43, 1, 1.0, -30.072, -51.118));
		input.add(new Tuple5<Integer, Integer, Double, Double, Double>(43, 1, 3.5, -30.068, -51.144));
		input.add(new Tuple5<Integer, Integer, Double, Double, Double>(43, 1, 7.5, -30.037, -51.203));
		DataSource<Tuple5<Integer, Integer, Double, Double, Double>> phoneData = env.fromCollection(input);
//		DataSource<Tuple5<Integer, Integer, Double, Double, Double>> phoneData = env
//			.readCsvFile("../data/phoneData.csv")
//			.fieldDelimiter(",")
//			.types(Integer.class, Integer.class, Double.class, Double.class, Double.class);
		
		// Map to each line a vector of coordinates)
		final DataSet<Tuple2<Integer, LabeledVector>> trainingDS = phoneData.sortPartition(0, Order.ASCENDING) // Order by linha_id
//		DataSet<LabeledVector> trainingDS = phoneData.sortPartition(0, Order.ASCENDING) // Order by linha_id
			.sortPartition(1, Order.ASCENDING)		// Order by phone_id
			.sortPartition(2, Order.ASCENDING)		// Order by time
			.groupBy(0, 1)							// Group by (linha_id,phone_id)
			.reduceGroup(new ReducerMatchCoordinates(mapLinesCoordinates));
		
		// Get the distinct number of lines
		DataSet<Tuple1<Integer>> lines = trainingDS.distinct(0)
			.project(0);
		
		// Collect the data set, iterate over the lines and apply ML
		List<Tuple2<Integer, WeightVector>> trainedVectors = new ArrayList<>();
		for (final Tuple1<Integer> lineNumber : lines.collect()) {
			
			DataSet<LabeledVector> trainingDSOneLine = 
				trainingDS.filter(new FilterFunction<Tuple2<Integer,LabeledVector>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public boolean filter(Tuple2<Integer, LabeledVector> datapoint) throws Exception {
						return datapoint.f0.equals(lineNumber.f0);
					}
				})
				.map(new MapFunction<Tuple2<Integer,LabeledVector>, LabeledVector>() {
					private static final long serialVersionUID = 1L;
					@Override
					public LabeledVector map(Tuple2<Integer, LabeledVector> datapoint)
							throws Exception {
						return datapoint.f1;
					}
				});
			
			MultipleLinearRegression mlr = Trainer.trainMLR(trainingDSOneLine);
			
			org.apache.flink.ml.math.Vector v0 = new org.apache.flink.ml.math.DenseVector(new double[]{0.0});
			org.apache.flink.ml.math.Vector v1 = new org.apache.flink.ml.math.DenseVector(new double[]{1.0});
			org.apache.flink.ml.math.Vector v2 = new org.apache.flink.ml.math.DenseVector(new double[]{2.0});
			org.apache.flink.ml.math.Vector v3 = new org.apache.flink.ml.math.DenseVector(new double[]{3.0});
			DataSet<org.apache.flink.ml.math.Vector> test = env.fromElements(v0, v1, v2, v3);
			Trainer.predictMLR(mlr, test);
			
			trainedVectors.add(new Tuple2<Integer, WeightVector>(
					lineNumber.f0,
					mlr.weightsOption().get().collect().head()
			));
		}
		
		env.fromCollection(trainedVectors).print();
		// execute program
		env.execute("Flink Java API Skeleton");
	}
}
