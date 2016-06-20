package pdp.poatransportes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;

public class ReducerMatchCoordinates implements GroupReduceFunction<Tuple5<Integer, Integer, Double, Double, Double>, Tuple2<Integer, LabeledVector>> {
//public class ReducerMatchCoordinates implements GroupReduceFunction<Tuple5<Integer, Integer, Double, Double, Double>, LabeledVector> {

	private Map<Integer, Vector<double[]>> lineCoordinates;
	
	public ReducerMatchCoordinates(Map<Integer, Vector<double[]>> lineCoordinates) {
		this.lineCoordinates = lineCoordinates;
	}
	
	private double[][] pairwiseDistance(Vector<double[]> coord1, Vector<double[]> coord2) {
		double[][] result = new double[coord1.size()][coord2.size()];
		
		for (int i=0, n=coord1.size(); i<n; i++) {
			double[] q = coord1.get(i);
			
			for (int j=0; j<coord2.size(); j++) {
				double[] p = coord2.get(j);
				result[i][j] = Math.sqrt( Math.pow(q[0] - p[0], 2) + Math.pow(q[1] - p[1], 2));
				
				//System.out.printf("%f ", result[i][j]);
			}
		}
		
		return result;
	}
	
	public int[] matchCoordinates(double[][] distances) {
		int lines = distances.length,
			columns = distances[0].length;
		int[] result = new int[lines];
		
		double accCost = 0.0;
		int i = lines-1,
			j = columns-1,
			k = 0;
		while (i >= 0) {
			
			// find the argmin in the whole row
			double minWhole = distances[i][k];
			int argminWhole = 0;
			for (int col=k; col < columns; col++) {
				if (distances[i][col] <= minWhole) {
					minWhole = distances[i][col];
					argminWhole = col;
				}
			}
			double newCostWhole = accCost + distances[i][argminWhole];
			// find the argmin in the left part of the row
			double minLeft = distances[i][k];
			int argminLeft = 0;
			for (int col=k; col <= j; col++) {
				if (distances[i][col] <= minLeft) {
					minLeft = distances[i][col];
					argminLeft = col;
				}
			}
			double newCostLeft = accCost + distances[i][argminLeft];
			
//			if (newCostWhole < newCostLeft) {
//				// Start the search again
//				k = argminLeft;
//				i = lines-1;
//				j = lines-1;
//			} else {
				// Continue the search in the upper row
				result[i] = argminLeft;
				i -= 1;
				j = argminLeft;
//			}
		}
		return result;
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(
			Iterable<Tuple5<Integer, Integer, Double, Double, Double>> groupedData,
			Collector<Tuple2<Integer, LabeledVector>> fullData)
//			Collector<LabeledVector> fullData)
			throws Exception {
		
		Vector<double[]> travelCoordinates = new Vector<double[]>();
		Vector<double[]> thisLineCoordinates = null;
		
		List<Tuple3<Integer, Integer, Double>> inputs = new ArrayList<>(); 
		
		boolean first = true;
		for (Tuple5<Integer, Integer, Double, Double, Double> phoneData : groupedData) {
			if (first) {
				thisLineCoordinates = this.lineCoordinates.get(phoneData.f1);
				first = false;
			}
			
			travelCoordinates.add(new double[]{phoneData.f3, phoneData.f4});
			
			inputs.add(new Tuple3<Integer, Integer, Double>(phoneData.f0, phoneData.f1, phoneData.f2));
		}
		
		if (thisLineCoordinates != null) {
			// Calculate the distance between any pair of coordinates
			double[][] distanceMatrix = pairwiseDistance(travelCoordinates, thisLineCoordinates);
			
			// Match the phone coordinates with the line ones
			int[] match = matchCoordinates(distanceMatrix);
			
			// Estimate when the bus might have left the initial stop, if we don't know it yet
			double timeAtBegin;
			if (match[0] != 0) {
				int last = inputs.size()-1,
					xFirst = match[0],
					xLast = match[last];
				double yFirst = inputs.get(0).f2,
					yLast = inputs.get(last).f2;
				
				double a = (yLast - yFirst) / (xLast - xFirst);
				
				timeAtBegin = (yFirst + yLast - a*xFirst - a*xLast) / 2.0;
			} else {
				timeAtBegin = inputs.get(0).f2;
			}
			
			// Translate the data points downwards at the Cartesian plane
			for (Tuple3<Integer, Integer, Double> input : inputs) {
				input.f2 -= timeAtBegin;
			};
			
			// Add the matched coordinates in the resulting dataset, which is a dataset of labeled vector
			assert match.length == inputs.size();
			double centralizeTerm = match.length > 0 ? match[0] : 0;
			int i = 0;
			for (Tuple3<Integer, Integer, Double> input : inputs) {
				fullData.collect(new Tuple2<Integer, LabeledVector>(
						input.f1,
						new LabeledVector(input.f2, new DenseVector(new double[]{i}))));
//						new LabeledVector(input.f2, new DenseVector(new double[]{match[i] - centralizeTerm}))));
						
	//			fullData.collect(new LabeledVector(input.f2, new DenseVector(new double[]{match[i]})));
				
				i++;
			}
		}
	}

	
}
