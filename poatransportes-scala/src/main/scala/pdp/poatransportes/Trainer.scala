package pdp.poatransportes

import org.apache.flink.api.scala._
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.regression.MultipleLinearRegression

object Trainer {
  
  def trainMLR(javaDs: JavaDataSet[LabeledVector]): MultipleLinearRegression = {
    
    val mlr = MultipleLinearRegression()
    val scalaDs = new DataSet[LabeledVector](javaDs)
    
    mlr.fit(scalaDs)
    mlr
  }
  
  def predictMLR(mlr: MultipleLinearRegression, javaDs: JavaDataSet[Vector]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val scalaDs = new DataSet[Vector](javaDs)
    
    val prediction = mlr.predict(scalaDs)
    prediction.print()
  }
}