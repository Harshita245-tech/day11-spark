import org.apache.spark.{SparkConf, SparkContext}

object Day11BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    println("\n===== DAY 11 - BROADCAST AND ACCUMULATORS =====")

    val conf = new SparkConf()
      .setAppName("Day11BroadcastAccumulator")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // ============================================================
    // 1. BROADCAST VARIABLE
    // ============================================================

    println("\n===== 1. BROADCAST VARIABLE =====")

    val productPrices = Map(
      "Laptop" -> 75000,
      "Mouse" -> 1500,
      "Keyboard" -> 3000,
      "Monitor" -> 20000
    )

    val broadcastPrices = sc.broadcast(productPrices)

    println("Product Price Master Data:")
    broadcastPrices.value.foreach(println)

    println("\nBroadcast Explanation:")
    println("A broadcast variable is used to efficiently share read-only data with executors.")
    println("The data is sent to each executor instead of being repeatedly sent with tasks.")
    println("Broadcast is useful when the reference data is small and read-only.")

    // ============================================================
    // 2. USE BROADCAST WITH RDD PROCESSING
    // ============================================================

    println("\n===== 2. BROADCAST WITH RDD PROCESSING =====")

    val transactions = sc.parallelize(
      Seq(
        ("T001", "Laptop", 2),
        ("T002", "Mouse", 5),
        ("T003", "Keyboard", 3),
        ("T004", "Monitor", 1),
        ("T005", "Laptop", 1)
      ),
      4
    )

    val transactionValues = transactions.map {
      case (transactionId, product, quantity) =>
        val price = broadcastPrices.value.getOrElse(product, 0)
        val total = price * quantity
        (transactionId, product, quantity, price, total)
    }

    println("Transaction Details:")
    transactionValues.collect().foreach(println)

    // ============================================================
    // 3. ACCUMULATOR
    // ============================================================

    println("\n===== 3. ACCUMULATOR =====")

    val badRecordCounter = sc.longAccumulator("BadRecordCounter")

    val records = sc.parallelize(
      Seq(
        "T001,Laptop,2",
        "T002,Mouse,5",
        "INVALID_RECORD",
        "T003,Keyboard,3",
        "T004,Monitor",
        "T005,Laptop,1"
      ),
      4
    )

    val validRecords = records.flatMap { record =>

      val parts = record.split(",")

      if (parts.length == 3) {
        Some(record)
      } else {
        badRecordCounter.add(1)
        None
      }
    }

    println("Valid Records:")
    validRecords.collect().foreach(println)

    println(s"\nBad Records Count: ${badRecordCounter.value}")

    println("\nAccumulator Explanation:")
    println("An accumulator is used for aggregating values from executors back to the Driver.")
    println("Accumulators are commonly used for counters and monitoring information.")
    println("Workers can add values to an accumulator, but they do not read its final value during normal processing.")

    // ============================================================
    // 4. WHY NOT USE NORMAL DRIVER VARIABLES?
    // ============================================================

    println("\n===== 4. DRIVER VARIABLE VS ACCUMULATOR =====")

    println("Normal Driver variables are not suitable for distributed updates.")
    println("Each executor may have its own copy of normal variables.")
    println("Changes made on executors are not reliably reflected in the Driver variable.")
    println("Accumulators provide Spark-supported distributed aggregation for counters.")

    // ============================================================
    // 5. TRANSACTION VALIDATION SCENARIO
    // ============================================================

    println("\n===== 5. TRANSACTION VALIDATION SCENARIO =====")

    println("Scenario:")
    println("A transaction dataset must be validated against a small product master table.")

    val masterProducts = Map(
      "Laptop" -> 75000,
      "Mouse" -> 1500,
      "Keyboard" -> 3000,
      "Monitor" -> 20000
    )

    val broadcastMaster = sc.broadcast(masterProducts)

    val transactionData = sc.parallelize(
      Seq(
        ("TX001", "Laptop", 1),
        ("TX002", "Mouse", 2),
        ("TX003", "Keyboard", 4),
        ("TX004", "Tablet", 1),
        ("TX005", "Monitor", 2),
        ("TX006", "Mobile", 1)
      ),
      4
    )

    val invalidTransactionCounter =
      sc.longAccumulator("InvalidTransactionCounter")

    val validatedTransactions = transactionData.flatMap {
      case (transactionId, product, quantity) =>

        broadcastMaster.value.get(product) match {

          case Some(price) =>
            val total = price * quantity
            Some(
              (transactionId, product, quantity, price, total)
            )

          case None =>
            invalidTransactionCounter.add(1)
            None
        }
    }

    println("\nValid Transactions:")
    validatedTransactions.collect().foreach(println)

    println(
      s"\nInvalid Transactions: ${invalidTransactionCounter.value}"
    )

    println("\nValidation Explanation:")
    println("The small product master table is broadcast to the executors.")
    println("Each transaction is checked against the broadcast master table.")
    println("Transactions with known products are processed.")
    println("Transactions with unknown products are counted using an accumulator.")

    // ============================================================
    // 6. BROADCAST BENEFIT
    // ============================================================

    println("\n===== 6. BROADCAST BENEFIT =====")

    println("Broadcast avoids repeatedly sending the same small reference data with tasks.")
    println("It can reduce communication overhead when a small read-only dataset is used frequently.")
    println("Examples include product master data, country codes, tax rates, and configuration data.")

    // ============================================================
    // 7. ACCUMULATOR USE CASES
    // ============================================================

    println("\n===== 7. ACCUMULATOR USE CASES =====")

    println("Accumulators can be used for:")
    println("- Counting bad records")
    println("- Counting invalid transactions")
    println("- Counting missing values")
    println("- Monitoring processing statistics")

    // ============================================================
    // 8. BROADCAST VS ACCUMULATOR
    // ============================================================

    println("\n===== 8. BROADCAST VS ACCUMULATOR =====")

    println("Broadcast:")
    println("- Shares read-only data from Driver to executors.")
    println("- Used for small reference data.")
    println("- Executors read the broadcast value.")

    println("\nAccumulator:")
    println("- Aggregates values from executors to the Driver.")
    println("- Commonly used for counters.")
    println("- Executors add values to the accumulator.")

    // ============================================================
    // 9. FINAL SUMMARY
    // ============================================================

    println("\n===== 9. FINAL SUMMARY =====")

    println("1. Broadcast variables efficiently share small read-only data.")
    println("2. Broadcast data is available to executors.")
    println("3. Accumulators are used for distributed counters.")
    println("4. Executors can add values to accumulators.")
    println("5. The Driver can read the final accumulator value.")
    println("6. Normal Driver variables should not be used for distributed updates.")
    println("7. Broadcast can be combined with RDD processing.")
    println("8. Accumulators can count invalid or bad records.")
    println("9. Transaction validation can use broadcast master data and accumulators.")

    println("\n===== DAY 11 BROADCAST AND ACCUMULATORS COMPLETED =====")

    sc.stop()
  }
}
