import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object A1MartIssues extends App {

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

  val orderitemsdf = spark.read
    .format("csv")
    .option("path", "/user/itv001180/Project2/order_items")
    .option("inferschema", true)
    .load()
    .selectExpr("_c0 as ItemId", "_c1 as OrderId", "_c2 as ProductId", "_c3 as Quantity", "_c4 as Subtotal", "_c5 as ProductPrice")

  println("a. Retrieve all records for particular customer")
  //val customer = readLine("Customer Name(in <firstName> <LastName>): ")
  val customer = "Mary Jones"
  val cust = customer.split(" ")
  customersdf.filter(customersdf("FirstName") === cust(0) && customersdf("LastName") === cust(1)).show(false)

  println("b. List count of orders based on status and month")
  val ordersModifieda = ordersdf.withColumn("Month", split(col("Timestamp"), "-").getItem(1))
  ordersModifieda.orderBy("Month").groupBy("Month", "OrderStatus").count().show(false)

  println("c. List count of orders based on status and month for particular customer")
  //val customerID = readLine("Customer ID: ")
  val customerID = 7465
  val orderscustomerb = ordersdf.filter(ordersdf("CustomerId") === customerID)
  val ordersModifiedb = orderscustomerb.withColumn("Month", split(col("Timestamp"), "-").getItem(1))
  ordersModifiedb.orderBy("Month").groupBy("Month", "OrderStatus").count().show(false)

  println("d. List count of orders based on customer and status")
  ordersdf.orderBy("CustomerId").groupBy("CustomerId", "OrderStatus").count().show(100)

  println("e. Find the customers who have placed orders")
  val CustomerIdorderse = ordersdf.select(ordersdf("CustomerId")).distinct
  customersdf.join(CustomerIdorderse, customersdf("CustomerId") === CustomerIdorderse("CustomerId"), "inner")
    .show(false)

  println("f. Find the customers who have not placed orders yet")
  val CustomerIdordersf = ordersdf.select(ordersdf("CustomerId")).distinct
  customersdf.join(CustomerIdordersf, customersdf("CustomerId") === CustomerIdordersf("CustomerId"), "leftanti")
    .show(false)

  println("g1. Find top 5 customer with Highest number of orders")
  ordersdf.groupBy("CustomerId").count().orderBy(col("count").desc).limit(5).show(false)

  println("g2. Find top 5 customer with Highest sum of total orders")
  orderitemsdf.groupBy("OrderId").sum("Subtotal").orderBy(col("sum(Subtotal)").desc).limit(5).show(false)
  
  println("h. Find the customer who did not order in last 1 monthor for long time")
  val ordersModifiedh = ordersdf.withColumn("UnixTime", unix_timestamp(col("DateAndTime")))
  val maxDate = ordersModifiedh.agg(max("UnixTime")).take(1)(0).get(0)
  val maxDateLong: Long = maxDate.asInstanceOf[Number].longValue
  val threshold = maxDateLong - 2592000
  val ordersLastMonthh = ordersModifiedh.filter(col("UnixTime") > threshold)
  customersdf.join(ordersLastMonthh, customersdf("CustomerId") === ordersLastMonthh("CustomerId"),"leftanti")
    .show(false)
    
  println("i. Find the last order date for all customers")
  ordersdf.groupBy("CustomerId").agg(max("DateAndTime") as "LastOrderDate").show(false)
  
  println("j. Find open and close number of ordersfor a customer")
  //val custIDinput = readLine("Customer ID: ")
  val custIDinput = "7833"
  val ordersCustomerj = ordersdf.filter(ordersdf("CustomerId") === custIDinput)
  val orderscountj = ordersCustomerj.groupBy("OrderStatus").count()
  orderscountj.agg(sum(when(orderscountj("OrderStatus") === "PENDING_PAYMENT" || 
                            orderscountj("OrderStatus") === "ON_HOLD"         ||
                            orderscountj("OrderStatus") === "PAYMENT_REVIEW"  ||
                            orderscountj("OrderStatus") === "PROCESSING"      ||
                            orderscountj("OrderStatus") === "SUSPECTED_FRAUD" ||
                            orderscountj("OrderStatus") === "PENDING", orderscountj("count"))).as("OPENED"),
                   sum(when(orderscountj("OrderStatus") === "COMPLETE" ||
                            orderscountj("OrderStatus") === "CLOSED"   ||
                            orderscountj("OrderStatus") === "CANCELED", orderscountj("count"))).as("CLOSED"))
                  .show(false)
  
   println("k. Find number of customers in every state")
   customersdf.groupBy("State").count().show(false)
}
  
