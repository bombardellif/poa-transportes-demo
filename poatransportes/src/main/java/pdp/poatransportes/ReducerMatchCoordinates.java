package pdp.poatransportes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class ReducerMatchCoordinates implements GroupReduceFunction<Tuple5<Integer, Integer, Double, Double, Double>, Tuple5<Integer,Integer,Double,Double,Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(
			Iterable<Tuple5<Integer, Integer, Double, Double, Double>> groupedData,
			Collector<Tuple5<Integer, Integer, Double, Double, Double>> fullData)
			throws Exception {
		
		for (Tuple5<Integer, Integer, Double, Double, Double> phoneData : groupedData) {
			fullData.collect(phoneData);
		}
	}

	
}
