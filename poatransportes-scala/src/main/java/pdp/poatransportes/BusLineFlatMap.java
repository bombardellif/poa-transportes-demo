package pdp.poatransportes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.common.WeightVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.util.Collector;

public class BusLineFlatMap implements FlatMapFunction<Tuple2<Integer, WeightVector>, Tuple5<Integer, Integer, String, Integer, Double>> {
	
	private static final long serialVersionUID = 1L;
	
	private HashMap<Integer, Vector<Integer>> lineStops;
	
	public BusLineFlatMap(List<Tuple3<Integer,Vector<double[]>, Vector<Integer>>> lines) {
		lineStops = new HashMap<>();
		for(Tuple3<Integer,Vector<double[]>, Vector<Integer>> line: lines) {
			lineStops.put(line.f0, line.f2);
		}
	}
	
	public void flatMap(Tuple2<Integer, WeightVector> data, Collector<Tuple5<Integer, Integer, String, Integer, Double>> out) throws Exception {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost/trabalho", "root", "root");
		
		Statement st = con.createStatement();
		String sql = "SELECT segundos FROM partidas WHERE linha_id = " + String.valueOf(data.f0) + " ORDER BY segundos ASC";
		ResultSet rs = st.executeQuery(sql);
		Vector<Integer> partidas = new Vector<>();
		while(rs.next()) {
			partidas.add(rs.getInt(1));
		}

		st = con.createStatement();
		sql = 
				"SELECT paradas.id, paradas.nome, linhas_paradas.ordem FROM paradas " +
				"inner join linhas_paradas on (paradas.id = linhas_paradas.parada_id) " +
				"where linha_id = " + String.valueOf(data.f0) + " " +
				"order by linhas_paradas.ordem asc";
		rs = st.executeQuery(sql);
		Vector<Integer> stops = this.lineStops.get(data.f0);
		int j = 0;
		while(rs.next()) {
			for(int i = 0; i < partidas.size(); i++) {
				Tuple5<Integer, Integer, String, Integer, Double> row = new Tuple5<>();
				row.f0 = data.f0;
				row.f1 = rs.getInt(1);
				row.f2 = rs.getString(2);
				row.f3 = i;
				DenseVector weight = (DenseVector) data.f1.weights();
				//DenseVector x = new DenseVector();
				double[] vecData = weight.data();
				//Vector weightVector = new WeightVector(, 0.0);
				row.f4 = Double.valueOf(partidas.get(i).toString()) + 
						Double.valueOf(lineStops.get(data.f0).get(j).toString()) * 
						vecData[0]; // calcular horario
				
				//WeightVector(DenseVector(Array(b(1))), b(0))
				
				out.collect(row);
			}
			j++;
		}
		
		con.close();
	}

}
