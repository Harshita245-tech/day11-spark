# Day 11 - Broadcast Variables and Accumulators

## Objective

The objective of Day 11 is to understand **Broadcast Variables** and **Accumulators** in Apache Spark and their use in distributed data processing.

This assignment demonstrates:

* Broadcast Variables
* Sharing small, read-only reference data with executors
* Using broadcast data with RDD processing
* Accumulators
* Counting bad records using accumulators
* Difference between normal Driver variables and Accumulators
* Transaction validation using broadcast data
* Counting invalid transactions using an accumulator

---

## Technologies Used

* **Scala:** 2.12.18
* **Apache Spark:** 3.5.3
* **Spark Core**
* **SBT:** 1.12.11
* **Operating System:** Ubuntu/Linux

---

# 1. Broadcast Variable

A **Broadcast Variable** is used to efficiently share small, read-only data with executors.

Instead of sending the same reference data repeatedly with different tasks, Spark broadcasts the data so that executors can access a local copy.

### Example

```scala
val productPrices = Map(
  "Laptop" -> 75000,
  "Mouse" -> 1500,
  "Keyboard" -> 3000,
  "Monitor" -> 20000
)

val broadcastPrices = sc.broadcast(productPrices)
```

The broadcast value can be accessed using:

```scala
broadcastPrices.value
```

In this assignment, the product price master data is broadcast and used during transaction processing.

---

# 2. Broadcast with RDD Processing

A transaction RDD is created containing the transaction ID, product, and quantity.

```scala
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
```

The broadcast product-price map is used to calculate the total transaction value.

```scala
val transactionValues = transactions.map {
  case (transactionId, product, quantity) =>
    val price = broadcastPrices.value.getOrElse(product, 0)
    val total = price * quantity
    (transactionId, product, quantity, price, total)
}
```

### Output

```text
(T001,Laptop,2,75000,150000)
(T002,Mouse,5,1500,7500)
(T003,Keyboard,3,3000,9000)
(T004,Monitor,1,20000,20000)
(T005,Laptop,1,75000,75000)
```

This demonstrates how broadcast data can be accessed while processing an RDD.

---

# 3. Accumulator

An **Accumulator** is used to aggregate values from distributed tasks.

Accumulators are commonly used for counters and monitoring information.

A Long Accumulator is created using:

```scala
val badRecordCounter =
  sc.longAccumulator("BadRecordCounter")
```

Executors can add values using:

```scala
badRecordCounter.add(1)
```

The final value can be accessed by the Driver using:

```scala
badRecordCounter.value
```

---

# 4. Counting Bad Records

The application processes the following records:

```text
T001,Laptop,2
T002,Mouse,5
INVALID_RECORD
T003,Keyboard,3
T004,Monitor
T005,Laptop,1
```

A valid record contains three fields.

```scala
val parts = record.split(",")

if (parts.length == 3) {
  Some(record)
} else {
  badRecordCounter.add(1)
  None
}
```

### Output

```text
Valid Records:
T001,Laptop,2
T002,Mouse,5
T003,Keyboard,3
T005,Laptop,1

Bad Records Count: 2
```

The two invalid records are counted using the accumulator.

---

# 5. Driver Variables and Accumulators

A Spark application consists of a **Driver** and one or more **Executors**.

A normal variable created on the Driver is not a reliable mechanism for collecting updates made by distributed tasks because executors may work with their own copies of variables.

Accumulators are specifically provided by Spark for distributed aggregation, such as counters.

### Basic Flow

```text
             Driver
               |
       +-------+-------+
       |       |       |
       v       v       v
 Executor 1 Executor 2 Executor 3
       |       |       |
       +-------+-------+
               |
               v
         Accumulator
               |
               v
             Driver
```

Executors add values to the accumulator, and the Driver can read the final accumulated value.

---

# 6. Transaction Validation Scenario

The assignment demonstrates transaction validation using a small product master table.

### Product Master Data

```scala
val masterProducts = Map(
  "Laptop" -> 75000,
  "Mouse" -> 1500,
  "Keyboard" -> 3000,
  "Monitor" -> 20000
)
```

The master data is broadcast to the executors:

```scala
val broadcastMaster =
  sc.broadcast(masterProducts)
```

The transaction dataset contains:

```text
(TX001,Laptop,1)
(TX002,Mouse,2)
(TX003,Keyboard,4)
(TX004,Tablet,1)
(TX005,Monitor,2)
(TX006,Mobile,1)
```

Each transaction is checked against the broadcast product master table.

* If the product exists, the transaction is processed.
* If the product does not exist, the invalid transaction counter is increased.

---

# 7. Valid Transactions

The valid transactions are:

```text
(TX001,Laptop,1,75000,75000)
(TX002,Mouse,2,1500,3000)
(TX003,Keyboard,4,3000,12000)
(TX005,Monitor,2,20000,40000)
```

These products are available in the broadcast master table.

---

# 8. Invalid Transactions

The invalid products are:

```text
Tablet
Mobile
```

These products are not available in the master product table.

### Output

```text
Invalid Transactions: 2
```

The invalid transaction count is maintained using an accumulator.

---

# 9. Transaction Validation Flow

The complete processing flow is:

```text
Product Master Table
        |
        v
Broadcast Variable
        |
        v
    Executors
        |
        v
  Transaction RDD
        |
        +----------------------+
        |                      |
        v                      v
Product Exists          Product Missing
        |                      |
        v                      v
Calculate Total       Accumulator + 1
        |                      |
        +----------+-----------+
                   |
                   v
             Final Result
```

This combines a **Broadcast Variable** and an **Accumulator** in a single distributed processing scenario.

---

# 10. Benefits of Broadcast Variables

Broadcast Variables are useful when a small reference dataset is required by many tasks.

In this assignment, the product master data is shared with executors using a Broadcast Variable.

### Typical examples of broadcast data

* Product master data
* Country codes
* Tax rates
* Configuration data
* Lookup/reference tables

Broadcasting such data can reduce the repeated transfer of the same reference data with individual tasks.

---

# 11. Accumulator Use Cases

Accumulators can be used for:

* Counting bad records
* Counting invalid transactions
* Counting missing values
* Monitoring processing statistics
* Tracking processing events

In this assignment, accumulators are used to count invalid records and invalid transactions.

---

# 12. Broadcast Variable vs Accumulator

| Feature            | Broadcast Variable   | Accumulator             |
| ------------------ | -------------------- | ----------------------- |
| Purpose            | Share read-only data | Aggregate values        |
| Direction          | Driver → Executors   | Executors → Driver      |
| Main Use           | Small reference data | Counters and monitoring |
| Executor Operation | Read value           | Add value               |
| Example            | Product prices       | Invalid record count    |

Broadcast Variables and Accumulators serve different purposes in Spark.

**Broadcast Variables** are used to distribute read-only reference data, while **Accumulators** are used to aggregate values generated during distributed processing.

---

# 13. Application Output

The Spark application successfully demonstrated the required concepts.

```text
===== 1. BROADCAST VARIABLE =====

Product Price Master Data:
(Laptop,75000)
(Mouse,1500)
(Keyboard,3000)
(Monitor,20000)

===== 2. BROADCAST WITH RDD PROCESSING =====

(T001,Laptop,2,75000,150000)
(T002,Mouse,5,1500,7500)
(T003,Keyboard,3,3000,9000)
(T004,Monitor,1,20000,20000)
(T005,Laptop,1,75000,75000)

===== 3. ACCUMULATOR =====

Bad Records Count: 2

===== 5. TRANSACTION VALIDATION SCENARIO =====

Valid Transactions:
(TX001,Laptop,1,75000,75000)
(TX002,Mouse,2,1500,3000)
(TX003,Keyboard,4,3000,12000)
(TX005,Monitor,2,20000,40000)

Invalid Transactions: 2

===== DAY 11 BROADCAST AND ACCUMULATORS COMPLETED =====
```

### Execution Status

```text
[success]
```

The Spark application completed successfully.

---

# 14. Conclusion

Day 11 demonstrates the use of **Broadcast Variables** and **Accumulators** in Apache Spark.

Broadcast Variables are used to efficiently share small, read-only reference data with executors.

Accumulators are used to aggregate values such as counters from distributed tasks.

The transaction validation scenario combines both concepts by:

1. Broadcasting product master data to executors.
2. Processing transactions using an RDD.
3. Validating products against the broadcast data.
4. Calculating transaction values for valid products.
5. Using an accumulator to count invalid transactions.

The application was successfully implemented and executed using **Scala 2.12.18** and **Apache Spark 3.5.3**.

## Status

**DAY 11 - BROADCAST AND ACCUMULATORS COMPLETED**
