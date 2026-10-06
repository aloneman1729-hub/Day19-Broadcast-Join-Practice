package com.day19broadcast

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastJoinApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19-Broadcast-Join-Practice")
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ============================================================
    // 1. LARGE FACT DATAFRAME - TRANSACTIONS
    // ============================================================

    val transactions = spark.range(1, 1000001)
      .select(
        col("id").alias("transaction_id"),
        concat(
          lit("C"),
          lpad((col("id") % 10000).cast("string"), 5, "0")
        ).alias("customer_id"),
        concat(
          lit("B"),
          lpad(((col("id") % 4) + 1).cast("string"), 3, "0")
        ).alias("branch_id"),
        round(
          ((col("id") % 1000) + 100).cast("double"),
          2
        ).alias("amount")
      )

    println("\n========================================")
    println("LARGE FACT DATAFRAME")
    println("========================================")

    println(s"Transaction count: ${transactions.count()}")

    transactions.show(10, false)

    // ============================================================
    // 2. SMALL REFERENCE DATAFRAME - BRANCH MASTER
    // ============================================================

    val branchMaster = Seq(
      ("B001", "Hyderabad", "South"),
      ("B002", "Bangalore", "South"),
      ("B003", "Mumbai", "West"),
      ("B004", "Delhi", "North")
    ).toDF(
      "branch_id",
      "branch_city",
      "region"
    )

    println("\n========================================")
    println("SMALL REFERENCE DATAFRAME")
    println("========================================")

    println(s"Branch master count: ${branchMaster.count()}")

    branchMaster.show(false)

    // ============================================================
    // 3. NORMAL JOIN
    // ============================================================

    println("\n========================================")
    println("NORMAL JOIN - SHUFFLE SORT MERGE JOIN")
    println("========================================")

    val normalJoin = transactions
      .join(branchMaster, Seq("branch_id"), "inner")

    println(s"Normal join count: ${normalJoin.count()}")

    println("\nNormal Join Execution Plan:")
    normalJoin.explain()

    // ============================================================
    // 4. BROADCAST JOIN
    // ============================================================

    println("\n========================================")
    println("BROADCAST JOIN")
    println("========================================")

    val broadcastJoin = transactions
      .join(
        broadcast(branchMaster),
        Seq("branch_id"),
        "inner"
      )

    println(s"Broadcast join count: ${broadcastJoin.count()}")

    println("\nBroadcast Join Execution Plan:")
    broadcastJoin.explain()

    // ============================================================
    // 5. SAMPLE JOINED DATA
    // ============================================================

    println("\n========================================")
    println("SAMPLE JOINED DATA")
    println("========================================")

    broadcastJoin
      .select(
        "transaction_id",
        "customer_id",
        "branch_id",
        "branch_city",
        "region",
        "amount"
      )
      .show(10, false)

    // ============================================================
    // 6. REGION ANALYSIS
    // ============================================================

    println("\n========================================")
    println("TRANSACTION SUMMARY BY REGION")
    println("========================================")

    broadcastJoin
      .groupBy("region")
      .agg(
        count("*").alias("transaction_count"),
        round(sum("amount"), 2).alias("total_amount")
      )
      .orderBy(desc("total_amount"))
      .show(false)

    // ============================================================
    // 7. BRANCH ANALYSIS
    // ============================================================

    println("\n========================================")
    println("TRANSACTION SUMMARY BY BRANCH")
    println("========================================")

    broadcastJoin
      .groupBy("branch_id", "branch_city")
      .agg(
        count("*").alias("transaction_count"),
        round(sum("amount"), 2).alias("total_amount")
      )
      .orderBy(desc("total_amount"))
      .show(false)

    println("\n========================================")
    println("DAY 19 BROADCAST JOIN COMPLETED")
    println("========================================")

    spark.stop()
  }
}
