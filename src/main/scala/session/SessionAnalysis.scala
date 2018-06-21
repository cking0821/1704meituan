package session

import java.lang.Long

import com.alibaba.fastjson.JSONObject
import com.aura.constant.Constants
import com.aura.dao.factory.DAOFactory
import com.aura.utils.{ParamUtils, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/8/10.
  *
  * [taskID]
  *
  */
object SessionAnalysis {
  def main(args: Array[String]): Unit = {
    /**
      * 获取到了程序入口
      */
    val spark = SparkSession
      .builder()
      .appName(Constants.SPARK_APP_NAME_SESSION)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop1:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    /**
      * 获取task的信息
      */
    val taskID: Long = ParamUtils.getTaskIdFromArgs(args)
    val taskDAO = DAOFactory.getTaskDAO
    val task = taskDAO.findById(taskID)
    val taskParam: JSONObject = ParamUtils.getTaskParam(task)
    /**
      * 获取参数的：
      * 起始时间
      * 结束时间
      *
      */
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val age= ParamUtils.getParam(taskParam,Constants.PARAM_SEX)

    /**
      *
      * 步骤一：
      *     从hive里面去获取数据
      */
    val sql=
      s"""
        SELECT
           *
        FORM
          user_visit_action
        WHERE
          date >= ${startDate} and date <= ${endDate}
      """

    val df: DataFrame = spark.sql(sql)

    val sql2 =
      """
        select * from user
      """
     val userdf = spark.sql(sql2)

    val usreInfoRDD: RDD[(Long, Row)] = userdf.rdd.map(row => (row(0).toString.toLong,row))

    val actionRDD: RDD[Row] = df.rdd

    /**
      * 步骤二： 根据用户条件去筛选会话
      *
      */
    val user2log: RDD[(Long, Row)] = actionRDD.map(log => (log.toString().split(",")(1).toLong,log))


      if( age !=null){

      }

    val filterRDD= actionRDD
    ///////////////////////////////权当前面没什么事
    val sessionRDD: RDD[(Long, Iterable[Row])] = filterRDD.groupBy(row => row(2).toString.toLong)


    val s_1_3=spark.sparkContext.longAccumulator(Constants.TIME_PERIOD_1s_4s)
    val s_4_7=spark.sparkContext.longAccumulator(Constants.TIME_PERIOD_4s_7s)

    /**
      *
      * 步骤三：
      *    会话时长比例（针对过滤出来的回话）
      *    1-3s  3-7s
      *    总的会话的比例
      *1） 如何求出每个会话的时长：
      *   1-3 s  会话个数 /  总的会话
      *
      *   groupby (1,2,3,4)
      *
      *   会话步长比例:
      *
      */

    /**
      *
      * 获取，点击，下单，支付排名前十的品类
      *思路：
      * 1）TopN 首先比较的点击的次数，如果点击次数相同，那么比较下单的次数，依次类推
      *    也就是说我们需要的是二次排序
      * 2）我们日志记录里面
      *
     搜索的关键词
   click_category_id  用户点击的日志里面 只会有一个品类id
     用户点击的品类
   order_category_id   用户下单的这条日志记录里面，有可能有很多个品类id
     用户下单的品类id
   pay_ctegory_id
     支付的品类id       用户下单的日志记录里面，也有可能有多个品类id
      *
      * 3）首先我想获取点击，下单，支付的所有品类
      *    点击的日志里面所有id  并集
      *    下单的日志里面所有的id  并集
      *    支付的日志里面所有的id  并集
      *        去重
      *    这样的方式才能把我们所有的品类id才能获取到
      * 4）点击品类的 单词计数操作
      *    11  11  12 12
      *    11 2  品类id 11  出现 两次
      *    12 2  品类id 12  出现 两次
      *    分别对下单品类
      *
      *        对支付品类 也做类似的操作
      *
      *
      *  5）用所有品类的这个rdd与  点击品类的次数RDD   下单品类的次数RDD     支付品类的次数RDD
      *     依次进行join
      *
      *  6) 为什么？
      *
      *
      *
      *
      */
    /**
      * 1)获取所有的品类id
      */
    val allCategoryIDRDD: RDD[(Long, Long)] = getAllcategory(filterRDD).distinct()
    /**
      * 2) 分别获取 点击品类，下单品类，支付品类 的次数
      */
    //点击
    val clickCategoryIDAndCountRDD: RDD[(Long, Long)] = clickCategoryCountRDD(filterRDD)
    //下单
    val orderCategoryCountRDD = orderCategoryCountRDD(filterRDD)
    //支付
    val payCategoryCountRDD = payCategoryCountRDD(filterRDD)


    val categoryIDAndValue: RDD[(Long, String)] = joinCategoryAndData(allCategoryIDRDD,clickCategoryIDAndCountRDD,orderCategoryCountRDD,payCategoryCountRDD)

    /**
      * 求出结果TopN
      */
    sortByClickCountAndOrderCountAndpayCount(categoryIDAndValue)
  }

  /**
    * 获取所有的品类ID
    *   目的是为了跟别人进行join
    *   (categoryid,categoryid)
    * @param filterRDD
    */
  def getAllcategory( filterRDD:RDD[Row]):RDD[(Long,Long)]={
    val categorylist: ListBuffer[(Long, Long)] = new ListBuffer[(Long,Long)]

    filterRDD.flatMap( row =>{
      val clickCategoryStr = row(7)
      val orderCategoryStr = row(9)
      val payCategoryStr = row(11)
      if(clickCategoryStr != null){
        categorylist += Tuple2(clickCategoryStr.toString.toLong,clickCategoryStr.toString.toLong)
      }
      if(orderCategoryStr != null){
        val orderStrArray: Array[String] = orderCategoryStr.toString.split(",")
        for(categoryid <- orderStrArray){
          categorylist += Tuple2(categoryid.toLong,categoryid.toLong)
        }
      }
      if(payCategoryStr != null){
        val payStrArray = payCategoryStr.toString.split(",")
        for(payCategoryid <- payStrArray ){
          categorylist += Tuple2(payCategoryid.toLong,payCategoryid.toLong)
        }
      }
     categorylist.iterator
    } )


  }

  /**
    * 获取点击品类出现的次数
    * @param filterRDD
    * @return
    */
  def clickCategoryCountRDD(filterRDD:RDD[Row]):RDD[(Long,Long)]= {
    val categoryRDDAndOne: RDD[(Long, Long)] = filterRDD.filter(row => {
      val clickCategoryid = row.get(7)
      if (clickCategoryid != null) {
        true
      } else {
        false
      }
    }).map(row => {
      val clickCategoryID = row.getLong(7)
      (clickCategoryID, 1L)
    })

    categoryRDDAndOne.reduceByKey((_ + _))

    /**
      * 获取下单品类出现的次数
      *
      * @param filterRDD
      * @return
      */
    def orderCategoryCountRDD(filterRDD: RDD[Row]): RDD[(Long, Long)] = {
      val orderCategoryRDD: RDD[(Long, Long)] = filterRDD.filter(row => {
        val orderCategoryid = row.get(9)
        if (orderCategoryid != null) {
          true
        } else {
          false
        }
      }).flatMap(row => {
        val orderStr = row.getString(9)
        val orderStrArray = orderStr.split(",")
        import scala.collection.mutable.ListBuffer
        var orderCategoryList = new ListBuffer[(Long, Long)]
        for (orderCategoryid <- orderStrArray) {
          orderCategoryList += Tuple2(orderCategoryid.toLong, 1L)
        }
        orderCategoryList.iterator
      })

      orderCategoryRDD.reduceByKey((_ + _))


    }

    /**
      * 获取支付品类id 出现的次数
      *
      * @param filterRDD
      * @return
      */
    def payCategoryCountRDD(filterRDD: RDD[Row]): RDD[(Long, Long)] = {
      val payCategoryRDD: RDD[(Long, Long)] = filterRDD.filter(row => {
        val payCategoryid = row.get(11)
        if (payCategoryid != null) {
          true
        } else {
          false
        }
      }).flatMap(row => {
        val payStr = row.getString(11)
        val payStrArray = payStr.split(",")
        import scala.collection.mutable.ListBuffer
        var payCategoryList = new ListBuffer[(Long, Long)]
        for (payCategoryid <- payStrArray) {
          payCategoryList += Tuple2(payCategoryid.toLong, 1L)
        }
        payCategoryList.iterator
      })

      payCategoryRDD.reduceByKey((_ + _))


    }







  }


  /**
    *
    *
    * @param allCategoryIDRDD
    * @param clickCategoryIDAndCountRDD
    * @param orderCategoryIDAndCountRDD
    * @param payCategoryIDAndCountRDD
    * @return
    */
  def joinCategoryAndData(
                           allCategoryIDRDD: RDD[(Long, Long)],
                           clickCategoryIDAndCountRDD: RDD[(Long, Long)],
                           orderCategoryIDAndCountRDD: RDD[(Long, Long)],
                           payCategoryIDAndCountRDD: RDD[(Long, Long)]
                         ): RDD[(Long, String)] = {
    /**
      * (Long, (Long, Option[Long])
      * long: category id
      * long: category id
      * clickcount:
      */
    val tmpJoinRDD: RDD[(Long, (Long, Option[Long]))] = allCategoryIDRDD.leftOuterJoin(clickCategoryIDAndCountRDD)
    val clickTmpjoinRDD = tmpJoinRDD.map(tuple => {
      val categoryID = tuple._1
      val clickCategoryCount = tuple._2._2.getOrElse(0)
      //k=v | k=v | kv
      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryID + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCategoryCount
      (categoryID, value)
    })
    /**
      * (Long, (String, Option[Long]))
      * Long: categoryid
      * String: value
      * long: 下单次数的次数
      */
    val tmpJoinRDD2: RDD[(Long, (String, Option[Long]))] = clickTmpjoinRDD.leftOuterJoin(orderCategoryIDAndCountRDD)

    val orderTmpJoinRDD = tmpJoinRDD2.map(tuple => {
      val categoryID = tuple._1
      var value = tuple._2._1
      val orderCategoryCount = tuple._2._2.getOrElse(0)
      value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCategoryCount
      (categoryID, value)
    })

    val tmpJoinRDD3 = orderTmpJoinRDD.leftOuterJoin(payCategoryIDAndCountRDD)

    val payJoinRDD = tmpJoinRDD3.map(tuple => {
      val categoryID = tuple._1
      var value = tuple._2._1
      val payCategoryCount = tuple._2._2.getOrElse(0)
      value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCategoryCount
      (categoryID, value)
    })

    payJoinRDD


  }

  /**
    * 实现排序
    * @param categoryIDAndValue
    */
  def sortByClickCountAndOrderCountAndpayCount(categoryIDAndValue: RDD[(Long, String)]): Unit ={
    val sortRDD: RDD[(SortKey, String)] = categoryIDAndValue.map(tuple => {
      val categoreID = tuple._1
      val value = tuple._2
      //k=v|k=v
      //clickcount ordercount paycount
      val clickCount = StringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_CLICK_COUNT).toInt
      val orderCount = StringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_ORDER_COUNT).toInt
      val payCount = StringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_PAY_COUNT).toInt
      val sortKey = new SortKey(clickCount, orderCount, payCount)
      (sortKey, value)
    })
    sortRDD.sortByKey(false).take(10)

  }









}
