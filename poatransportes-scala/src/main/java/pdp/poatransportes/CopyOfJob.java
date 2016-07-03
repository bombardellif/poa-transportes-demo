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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.common.WeightVector;
import org.apache.flink.ml.regression.MultipleLinearRegression;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.io.jdbc.*;

import java.sql.Connection;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;

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




import com.github.fommil.netlib.BLAS;
public class CopyOfJob {

	@SuppressWarnings("unchecked")
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
		//DataSet<Tuple3<Integer, Double, Double>> dbData = env.createInput(
		DataSet<Tuple3<String, String, String>> dbData = env.createInput(
			// create and configure input format
			JDBCInputFormat.buildJDBCInputFormat()
				//.setDrivername("org.postgresql.Driver")
				.setDrivername("com.mysql.jdbc.Driver")
				//.setDBUrl("jdbc:postgresql://localhost:5432/poa_transporte")
				.setDBUrl("jdbc:mysql://localhost/trabalho")
				//.setUsername("bombardellif")
				//.setPassword("1234")
				.setUsername("root")
				.setPassword("root")
				.setQuery("SELECT linha_id, latitude, longitude FROM coordenadas WHERE linha_id = 127621")
				.finish(),
			// specify type information for DataSet
			//new TupleTypeInfo(Tuple3.class, INT_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO)
			new TupleTypeInfo(Tuple3.class, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
		);
		
		final Map<Integer, Vector<double[]>> mapLinesCoordinates = new HashMap<>();
		
		List<Tuple2<Integer,Vector<double[]>>> listOfLines = dbData.groupBy(0)
			.reduceGroup(new GroupReduceFunction<Tuple3<String, String, String>, Tuple2<Integer,Vector<double[]>>>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void reduce(
						Iterable<Tuple3<String, String, String>> values,
						Collector<Tuple2<Integer, Vector<double[]>>> out)
						throws Exception {
					
					Vector<double[]> route = new Vector<>();
					boolean first = true;
					int line = 0;
					for (Tuple3<String, String, String> coordinate : values) {
						route.add(new double[]{Double.parseDouble(coordinate.f1), Double.parseDouble(coordinate.f2)});
						if (first)
							line = Integer.parseInt(coordinate.f0);
					}
					out.collect(new Tuple2<>(line, route));
				}
			})
			.collect();
		for (Tuple2<Integer,Vector<double[]>> line : listOfLines) {
			mapLinesCoordinates.put(line.f0, line.f1);
		}
		
		// Read data input from phones (batch)
		DataSource<Tuple5<Integer, Integer, Double, Double, Double>> phoneData = env
			.readCsvFile("../data/phoneData-small.csv")
			.fieldDelimiter(",")
			.types(Integer.class, Integer.class, Double.class, Double.class, Double.class);
		
		// Map to each line a vector of coordinates)
		final DataSet<Tuple2<Integer, LabeledVector>> trainingDS = phoneData.sortPartition(1, Order.ASCENDING) // Order by linha_id
//		DataSet<LabeledVector> trainingDS = phoneData.sortPartition(0, Order.ASCENDING) // Order by linha_id
			.sortPartition(0, Order.ASCENDING)		// Order by phone_id
			.sortPartition(2, Order.ASCENDING)		// Order by time
			.groupBy(1, 0)							// Group by (linha_id,phone_id)
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
//			trainingDSOneLine.print();
			
			WeightVector weight = Trainer.trainMLR(trainingDSOneLine);
			trainedVectors.add(new Tuple2<Integer, WeightVector>(
					lineNumber.f0,
					weight
			));
			
//			org.apache.flink.ml.math.Vector v0 = new org.apache.flink.ml.math.DenseVector(new double[]{0.0});
//			org.apache.flink.ml.math.Vector v1 = new org.apache.flink.ml.math.DenseVector(new double[]{273.0});
//			org.apache.flink.ml.math.Vector v2 = new org.apache.flink.ml.math.DenseVector(new double[]{300.0});
//			org.apache.flink.ml.math.Vector v3 = new org.apache.flink.ml.math.DenseVector(new double[]{450.0});
//			DataSet<org.apache.flink.ml.math.Vector> test = env.fromElements(v0, v1, v2, v3);
//			DataSet<WeightVector> weights = env.fromElements(weight);
//			Trainer.predictMLR(weights, test);
		}
//		trainingDS.print();
		env.fromCollection(trainedVectors).print();
		// execute program
		env.execute("Flink Java API Skeleton");
	}
}
