# **📖 ULTIMATE APACHE SPARK README**  
**From Zero to Distributed Computing Hero**  

---

## **🔍 1. What is Apache Spark?**  
### **Definition**  
Apache Spark is an **open-source, distributed computing system** for big data processing. It provides:  
- **In-memory processing** (100x faster than Hadoop MapReduce).  
- **Unified engine** for SQL, streaming, machine learning, and graph processing.  
- **Fault tolerance** with Resilient Distributed Datasets (RDDs).  

### **Key Features**  
| Feature               | Benefit                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| **Spark SQL**         | Run SQL queries on structured data.                                    |
| **Spark Streaming**   | Process real-time data streams (Kafka, TCP sockets).                   |
| **MLlib**            | Built-in machine learning library (classification, regression).        |
| **GraphX**           | Graph processing (PageRank, social networks).                          |

### **Spark vs. Hadoop**  
| **Aspect**       | **Spark**                          | **Hadoop MapReduce**               |
|------------------|------------------------------------|------------------------------------|
| **Speed**        | In-memory (Fast)                  | Disk-based (Slower)               |
| **Ease of Use**  | High-level APIs (Python/SQL)      | Low-level Java APIs               |
| **Use Cases**    | Real-time, iterative processing   | Batch processing                 |

---

## **🛠 2. Installation & Setup**  
### **Step 1: Install Prerequisites**  
- **Java 8/11**:  
  ```bash
  sudo apt install openjdk-11-jdk  # Ubuntu
  brew install openjdk@11          # macOS
  ```
- **Python 3.x** (for PySpark):  
  ```bash
  sudo apt install python3-pip     # Ubuntu
  ```

### **Step 2: Download Spark**  
```bash
wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xvf spark-3.5.0-bin-hadoop3.tgz
cd spark-3.5.0-bin-hadoop3
```

### **Step 3: Start Spark Shell**  
- **Python (PySpark)**:  
  ```bash
  ./bin/pyspark
  ```
- **Scala**:  
  ```bash
  ./bin/spark-shell
  ```

---

## **📊 3. Basic Usage**  
### **Task 1: Read a CSV File**  
```python
# PySpark
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show(5)
```

### **Task 2: Filter and Aggregate**  
```python
from pyspark.sql.functions import col, avg

# Filter rows where age > 25
filtered_df = df.filter(col("age") > 25)

# Average salary by department
avg_salary = df.groupBy("department").agg(avg("salary").alias("avg_salary"))
avg_salary.show()
```

### **Task 3: Write Output**  
```python
df.write.parquet("output.parquet")  # Columnar storage
```

---

## **⚡ 4. Intermediate Skills**  
### **Spark SQL**  
```python
# Register DataFrame as a SQL table
df.createOrReplaceTempView("employees")

# Run SQL query
result = spark.sql("SELECT * FROM employees WHERE salary > 50000")
result.show()
```

### **Caching for Performance**  
```python
df.cache()  # Persist in memory
```

### **Joins**  
```python
joined_df = df1.join(df2, on="id", how="inner")
```

---

## **🚀 5. Advanced Techniques**  
### **Spark Streaming (Real-Time Data)**  
```python
from pyspark.streaming import StreamingContext

# Initialize streaming context (batch interval = 1 second)
ssc = StreamingContext(spark.sparkContext, 1)

# Read from TCP socket
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
word_counts = words.countByValue()
word_counts.pprint()

ssc.start()             # Start computation
ssc.awaitTermination()  # Wait for termination
```

### **Machine Learning with MLlib**  
```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

# Prepare features
assembler = VectorAssembler(inputCols=["age", "salary"], outputCol="features")
data = assembler.transform(df)

# Train model
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(data)
```

### **Optimizing Performance**  
1. **Partitioning**:  
   ```python
   df.repartition(100)  # Increase parallelism
   ```
2. **Broadcast Variables**:  
   ```python
   small_df = spark.createDataFrame([...])
   broadcast_df = spark.sparkContext.broadcast(small_df.collect())
   ```

---

## **📚 6. Learning Resources**  
### **Free**  
- [Spark Documentation](https://spark.apache.org/docs/latest/)  
- [Databricks Academy](https://academy.databricks.com/) (Free courses)  

### **Paid**  
- **Book**: *Learning Spark, 2nd Edition* (O’Reilly).  
- **Course**: [Spark Specialization on Coursera](https://www.coursera.org/specializations/big-data).  

---

## **❓ FAQ**  
**Q: How to monitor Spark jobs?**  
→ Access the **Spark UI** at `http://localhost:4040`.  

**Q: Spark vs. Pandas?**  
→ Use **Pandas for small data** (single machine), **Spark for big data** (distributed).  

**Q: How to handle skewed data?**  
→ Use `salting` or `repartition`:  
```python
df = df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 100).cast("int")))
```

---

## **🎯 Final Tips**  
✅ **Avoid `collect()`** – It brings all data to the driver (OOM risk).  
✅ **Use `.parquet()`** – Optimized for columnar storage.  
✅ **Tune `spark.executor.memory`** in `spark-defaults.conf`.  

---
