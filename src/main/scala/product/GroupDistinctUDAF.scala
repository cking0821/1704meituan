package product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by Administrator on 2017/8/11.
  *
  */
object GroupDistinctUDAF extends UserDefinedAggregateFunction{
  /**
    * 输入的数据类型
    * 1：beijing
    * @return
    */
  override def inputSchema: StructType = StructType(
    StructField("city_info",StringType,true) ::Nil
  )

  /**
    * 定义输出的数据类型
    * @return
    */
  override def dataType: DataType = StringType

  /**
    * 定义中间的缓存字段和数据类型
    * @return
    */
  override def bufferSchema: StructType =StructType(
    StructField("buffer_city_info",StringType,true) ::Nil
  )

  /**
    * 初始化缓存字段
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }

  /**
    * 局部汇总
    * 拼接字符串
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var last_str = buffer.getString(0)
    val current_str = input.getString(0)
    if(!last_str.contains(current_str)){
     if(last_str.equals("")){
       last_str+=current_str
     }else{
       last_str+=","+current_str
     }
    }
    buffer.update(0,last_str)  //1:beijing,2:shanghai
  }

  /**
    * 全局汇总
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var b1 = buffer1.getString(0)//1:beijing,2:shanghai
    val b2 = buffer2.getString(0)
    for( b <- b2.split(",")){
      if(!b1.contains(b)){
        if(b1.equals("")){
          b1=b1+b
        }else{
          b1=b1+","+b
        }
      }
    }
    buffer1.update(0,b1)
  }

  /**
    *返回最后的结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
     buffer.getString(0)
  }

  /**
    *输入和输出的数据类型是否一致
    * @return
    */
  override def deterministic: Boolean = true
}
