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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.common.WeightVector;
import org.apache.flink.ml.regression.MultipleLinearRegression;
import org.apache.flink.streaming.api.functions.windowing.delta.extractor.ArrayFromTuple;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.io.jdbc.*;

import scala.Console;

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
public class Job {

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
		DataSet<Tuple4<Integer, Double, Double, Long>> dbData = env.createInput(
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
				.setQuery(
					"SELECT C.linha_id, C.latitude, C.longitude, (C.id - F.minId) ordem " +
					"FROM coordenadas C " +
					"INNER JOIN (SELECT MIN(id) AS minId, linha_id FROM coordenadas GROUP BY linha_id) F ON (C.linha_id = F.linha_id) " +
					"WHERE C.linha_id IN (SELECT id FROM linhas where tem_paradas = 1) " +
					"ORDER BY C.linha_id, ordem ASC"
				)
				
				//.setQuery("SELECT linha_id, latitude, longitude, id FROM coordenadas WHERE linha_id IN (SELECT id FROM linhas where tem_paradas = 1) ORDER BY id ASC")
				
				//.setQuery("SELECT linha_id, latitude, longitude FROM coordenadas WHERE linha_id = 127621 ORDER BY id ASC")
				.finish(),
			// specify type information for DataSet
			new TupleTypeInfo(Tuple4.class, INT_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)
		);
		
		final Map<Integer, Vector<double[]>> mapLinesCoordinates = new HashMap<>();
		
		List<Tuple3<Integer, Vector<double[]>, Vector<Integer>>> listOfLines = dbData
			.groupBy(0)
			.sortGroup(3, Order.ASCENDING)
			.reduceGroup(new GroupReduceFunction<Tuple4<Integer,Double,Double, Long>, Tuple2<Integer,Vector<double[]>>>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void reduce(
						Iterable<Tuple4<Integer, Double, Double, Long>> values,
						Collector<Tuple2<Integer, Vector<double[]>>> out)
						throws Exception {
					
					Vector<double[]> route = new Vector<>();
					boolean first = true;
					int line = 0;
					for (Tuple4<Integer, Double, Double, Long> coordinate : values) {
						route.add(Integer.parseInt(coordinate.f3.toString()), new double[] {coordinate.f1, coordinate.f2});
						if (first)
							line = coordinate.f0;
					}
					out.collect(new Tuple2<>(line, route));
				}
			})
			.map(new MapFunction<Tuple2<Integer, Vector<double[]>>, Tuple3<Integer, Vector<double[]>, Vector<Integer>>>() {
				private static final long serialVersionUID = 1L;
				
				@Override
				public Tuple3<Integer, Vector<double[]>, Vector<Integer>> map(Tuple2<Integer, Vector<double[]>> in) throws Exception {
					Class.forName("com.mysql.jdbc.Driver").newInstance();
					Connection con = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/trabalho", "root", "root");
					
					java.sql.Statement st = con.createStatement();
					String sql = 
							"SELECT LP.ordem, P.lat, P.lon "
						+ 	"FROM linhas_paradas LP "
						+	"INNER JOIN paradas P ON (P.id = LP.parada_id) "
						+ 	"WHERE LP.linha_id = " + String.valueOf(in.f0) + " "
						+ 	"ORDER BY ordem ASC";
					java.sql.ResultSet rs = st.executeQuery(sql);
					Vector<Integer> stops = new Vector<Integer>(rs.getFetchSize());
					while(rs.next()) {
						int ordem = rs.getInt("ordem");
						double[] coord = new double[]{rs.getDouble("lat"), rs.getDouble("lon")};
						double nearestDistance = 10000000.0;
						int nearest = 0;
						int gettingFar = 0;
						for(int i = 0; i < in.f1.size(); i++) {
							double[] _coord = in.f1.get(i);
							double dist = Math.sqrt(Math.pow(coord[0] - _coord[0], 2) + Math.pow(coord[1] - _coord[1], 2));
							if(dist < nearestDistance) {
								nearest = i;
								nearestDistance = dist;
								gettingFar = 0;
							} else {
								gettingFar++;
							}
							if(gettingFar > 10) {
								/* se teve 10 aumentos seguidos de distancia, admite que já encontrou */
								break;
							}
						}
						stops.add(ordem - 1, nearest);
					}
					con.close();
					Tuple3<Integer, Vector<double[]>, Vector<Integer>> out = new Tuple3<>(in.f0, in.f1, stops);
					return out;
				}
			})
			.collect();
		for (Tuple3<Integer,Vector<double[]>, Vector<Integer>> line : listOfLines) {
			mapLinesCoordinates.put(line.f0, line.f1);
		}
		// Read data input from phones (batch)
		DataSource<Tuple5<Integer, Integer, Double, Double, Double>> phoneData = env
			.readCsvFile("../data/phoneData-micro.csv")
			.fieldDelimiter(",")
			.types(Integer.class, Integer.class, Double.class, Double.class, Double.class);
		
		// Map to each line a vector of coordinates)
		final DataSet<Tuple2<Integer, LabeledVector>> trainingDS = phoneData.sortPartition(1, Order.ASCENDING) // Order by linha_id
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
			
			WeightVector weight = Trainer.trainMLR(trainingDSOneLine);
			trainedVectors.add(new Tuple2<Integer, WeightVector>(
					lineNumber.f0,
					weight
			));

		}
//		trainingDS.print();
		env
			.fromCollection(trainedVectors)
			/* produz uma lista com (linha_id, parada_id, parada nome, viagem, horario) */
			.flatMap(new BusLineFlatMap(listOfLines))
			.writeAsCsv("/Users/eduardo/Desktop/poa-transportes-demo/out.csv").setParallelism(1);
			//.map(new MapFunction<Tuple2<Integer, LabeledVector>, >)
			//.print();
		// execute program
		//env.execute("Flink Java API Skeleton");
	}
}
