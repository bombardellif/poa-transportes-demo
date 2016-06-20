package pdp.poatransportes

import org.apache.flink.api.scala._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.common.WeightVector

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

object Trainer {
  
  def trainMLR(javaDs: JavaDataSet[LabeledVector]): WeightVector = {
    
    val scalaDs = new DataSet[LabeledVector](javaDs)
    val labels:DataSet[Double] = scalaDs.map { x => x.label }
    val Y = labels.collect().toArray[Double]
    val vectors:DataSet[Array[Double]] = scalaDs.map { x => Array(x.vector(0)) }
    val X = vectors.collect().toArray[Array[Double]]
    
    val ols = new OLSMultipleLinearRegression()
    ols.newSampleData(Y, X)
    val b = ols.estimateRegressionParameters()
    
    WeightVector(DenseVector(Array(b(1))), b(0))
  }
  
  def predictMLR(weights: JavaDataSet[WeightVector], javaDs: JavaDataSet[Vector]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val mlr = MultipleLinearRegression()
    mlr.weightsOption = Option(new DataSet[WeightVector](weights))
    val scalaDs = new DataSet[Vector](javaDs)
    
    val prediction = mlr.predict(scalaDs)
    prediction.print()
  }
  
  def predictMLR(mlr: MultipleLinearRegression, javaDs: JavaDataSet[Vector]): Unit = {
    val scalaDs = new DataSet[Vector](javaDs)
    
    val prediction = mlr.predict(scalaDs)
    prediction.print()
  }
  
  def predictMLRValidate(mlr: MultipleLinearRegression, javaDs: JavaDataSet[LabeledVector]): Unit = {
    val scalaDs = new DataSet[LabeledVector](javaDs)
    
    val prediction = mlr.predict(scalaDs)
    prediction.print()
  }
}