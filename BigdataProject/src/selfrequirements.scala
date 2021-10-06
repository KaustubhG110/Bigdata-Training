import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object selfrequirements extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "A1Mart application")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val customersdf = spark.read
    .format("csv")
    .option("path", "/user/itv001180/Project2/customers")
    .option("inferschema", true)
    .load()
    .selectExpr("_c0 as CustomerId", "_c1 as FirstName", "_c2 as LastName", "_c3 as EmailId",
      "_c4 as Password", "_c5 as Street", "_c6 as City", "_c7 as State", "_c8 as Zipcode")

  val ordersdf = spark.read
    .format("csv")
    .option("path", "/user/itv001180/Project2/orders")
    .option("inferschema", true)
    .load()
    .selectExpr("_c0 as OrderId", "_c1 as Timestamp", "_c2 as CustomerId", "_c3 as OrderStatus")

  //val inputcity = readLine("City Name: ")
  val inputcity = "Brownsville"
  val customerscity = customersdf.filter(customersdf("City") === inputcity)

  println("Number of orders based on zip code for particular city")

  val orderszipcode = ordersdf.join(customerscity, ordersdf("CustomerId") === customerscity("CustomerId"), "inner")
  orderszipcode.groupBy("Zipcode").count().show(false)

  println("Customer details whose orders are suspected to be fraud")
  val ordersfraud = ordersdf.filter(ordersdf("OrderStatus") === "SUSPECTED_FRAUD")
  customersdf.join(ordersfraud, customersdf("CustomerId") === ordersfraud("CustomerId"), "inner").show(false)

  println("Number of Orders based on Month for a particular city")
  val orderscity = ordersdf.join(customerscity, ordersdf("CustomerId") === customerscity("CustomerId"), "inner")
  val orderscityMonth = orderscity.withColumn("Month", split(col("Timestamp"), "-").getItem(1))
  orderscityMonth.groupBy("Month").count().orderBy("Month").show(false)

}