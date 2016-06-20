package pdp.poatransportes

import org.apache.flink.api.scala._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.common.WeightVector

object Trainer {
  
  def trainMLR(javaDs: JavaDataSet[LabeledVector]): MultipleLinearRegression = {
    
    val mlr = MultipleLinearRegression()
      .setIterations(1)
      .setStepsize(0.5)
      .setConvergenceThreshold(0.001)
    
    val scalaDs = new DataSet[LabeledVector](javaDs)
    
    scalaDs.print()
    mlr.fit(scalaDs)
    val wv = mlr.weightsOption.get
    wv.print()
    
    mlr
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