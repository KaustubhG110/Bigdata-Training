import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object masking {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "A1Mart application")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val orderitemsdf = spark.read
    .format("csv")
    .option("path", "/user/itv001180/Project2/order_items")
    .option("inferschema", true)
    .load()
    .selectExpr("_c0 as ItemId", "_c1 as OrderId", "_c2 as ProductId", "_c3 as Quantity", "_c4 as Subtotal", "_c5 as ProductPrice")

  val maskeddf = orderitemsdf
    .withColumn("SubtotalMasked", regexp_replace(orderitemsdf("Subtotal"), "[0-9]+" + "." + "[0-9]+", "*****"))
    .drop("Subtotal").show
}