import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

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

  println("Number of orders based on zip code for particular city")

  //val inputcity = readLine("City Name: ")
  val inputcity = "Brownsville"
  val customerscity = customersdf.filter(customersdf("City") === inputcity)

  val orderszipcode = ordersdf.join(customerscity, ordersdf("CustomerId") === customerscity("CustomerId"), "inner")

  orderszipcode.groupBy("Zipcode").count().show(false)

}