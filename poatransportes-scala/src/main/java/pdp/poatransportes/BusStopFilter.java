package pdp.poatransportes;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class BusStopFilter implements FilterFunction<Tuple3<Integer, Integer, Integer>> {
	private int busLine;
	
	public BusStopFilter(int _busLine) {
		this.busLine = _busLine;
	}
	
	@Override
	public boolean filter(Tuple3<Integer, Integer, Integer> stop) {
		return stop.f0 == this.busLine;
	}
}
