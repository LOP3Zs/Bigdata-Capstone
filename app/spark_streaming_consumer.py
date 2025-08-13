from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, date_format, avg, stddev, lag, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql.functions import unix_timestamp

# --- 1. Spark session ---
spark = SparkSession.builder \
    .appName("KafkaToCassandra_StreamWithIndicators") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                  "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. Schema Kafka message ---
schema = StructType([
    StructField("symbol", StringType()),
    StructField("interval", StringType()),
    StructField("datetime", StringType()),  # ISO format
    StructField("epoch_ms", LongType()),
    StructField("open", FloatType()),
    StructField("high", FloatType()),
    StructField("low", FloatType()),
    StructField("close", FloatType()),
    StructField("volume", FloatType()),
])

# --- 3. Read from Kafka ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "crypto_candles") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

ohlcv_df = parsed_df \
    .withColumn("timestamp", to_timestamp("datetime")) \
    .withColumn("day", to_date("timestamp")) \
    .withColumn("time", unix_timestamp("timestamp").cast("long")) \
    .drop("datetime", "epoch_ms", "interval", "timestamp")

# --- 4. Helper functions ---
def fetch_history(spark, symbol, current_day, current_time_long, window_minutes=200):
    """Fetch historical data from Cassandra for technical indicator calculations"""
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="ohlcv_1m", keyspace="market_data") \
        .load() \
        .filter(
            (col("symbol") == symbol) &
            (col("day") == current_day) &
            (col("time") <= current_time_long)
        ) \
        .select("symbol", "day", "time", "open", "high", "low", "close", "volume") \
        .orderBy("day", "time") \
        .limit(window_minutes)

def calculate_indicators(df):
    """Calculate technical indicators for the dataframe"""
    # Moving Averages
    w_sizes = [5, 10, 20, 50, 100, 200]
    for w in w_sizes:
        df = df.withColumn(f"ma{w}",
            avg("close").over(Window.partitionBy("symbol").orderBy("day", "time").rowsBetween(-w + 1, 0))
        )

    # Bollinger Bands (20 period)
    bb_win = Window.partitionBy("symbol").orderBy("day", "time").rowsBetween(-20 + 1, 0)
    df = df.withColumn("bb_middle", avg("close").over(bb_win))
    df = df.withColumn("bb_stddev", stddev("close").over(bb_win))
    df = df.withColumn("bb_upper", col("bb_middle") + 2 * col("bb_stddev"))
    df = df.withColumn("bb_lower", col("bb_middle") - 2 * col("bb_stddev"))

    # MACD (12, 26, 9)
    win12 = Window.partitionBy("symbol").orderBy("day", "time").rowsBetween(-12 + 1, 0)
    win26 = Window.partitionBy("symbol").orderBy("day", "time").rowsBetween(-26 + 1, 0)
    win9  = Window.partitionBy("symbol").orderBy("day", "time").rowsBetween(-9 + 1, 0)

    df = df.withColumn("macd_line", avg("close").over(win12) - avg("close").over(win26))
    df = df.withColumn("macd_signal", avg("macd_line").over(win9))
    df = df.withColumn("macd_histogram", col("macd_line") - col("macd_signal"))

    # RSI(14)
    lag_close = lag("close", 1).over(Window.partitionBy("symbol").orderBy("day", "time"))
    delta = col("close") - lag_close
    gain = when(delta > 0, delta).otherwise(0.0)
    loss = when(delta < 0, -delta).otherwise(0.0)
    win14 = Window.partitionBy("symbol").orderBy("day", "time").rowsBetween(-14 + 1, 0)
    avg_gain = avg(gain).over(win14)
    avg_loss = avg(loss).over(win14)
    df = df.withColumn("rsi14", when(avg_loss == 0, 100.0).otherwise(100 - (100 / (1 + (avg_gain / avg_loss)))))

    # OBV (On Balance Volume)
    prev_close = lag("close", 1).over(Window.partitionBy("symbol").orderBy("day", "time"))
    obv_delta = when(col("close") > prev_close, col("volume")) \
        .when(col("close") < prev_close, -col("volume")).otherwise(0)
    df = df.withColumn("obv", spark_sum(obv_delta).over(Window.partitionBy("symbol").orderBy("day", "time")
                                                        .rowsBetween(Window.unboundedPreceding, 0)))
    
    # Add null columns for indicators not calculated yet (matching Cassandra schema)
    df = df.withColumn("adx", lit(None).cast(FloatType())) \
           .withColumn("plus_di", lit(None).cast(FloatType())) \
           .withColumn("minus_di", lit(None).cast(FloatType())) \
           .withColumn("stoch_k", lit(None).cast(FloatType())) \
           .withColumn("stoch_d", lit(None).cast(FloatType())) \
           .withColumn("cci", lit(None).cast(FloatType())) \
           .withColumn("atr", lit(None).cast(FloatType())) \
           .withColumn("parabolic_sar", lit(None).cast(FloatType())) \
           .withColumn("volume_ma20", lit(None).cast(FloatType())) \
           .withColumn("mfi", lit(None).cast(FloatType())) \
           .withColumn("pivot", lit(None).cast(FloatType())) \
           .withColumn("support1", lit(None).cast(FloatType())) \
           .withColumn("support2", lit(None).cast(FloatType())) \
           .withColumn("support3", lit(None).cast(FloatType())) \
           .withColumn("resistance1", lit(None).cast(FloatType())) \
           .withColumn("resistance2", lit(None).cast(FloatType())) \
           .withColumn("resistance3", lit(None).cast(FloatType()))

    return df

def process_batch(batch_df, batch_id):
    """Process each batch of streaming data"""
    if batch_df.count() == 0:
        return

    symbols = [row['symbol'] for row in batch_df.select("symbol").distinct().collect()]
    
    for symbol in symbols:
        symbol_batch = batch_df.filter(col("symbol") == symbol)
        
        # Get the latest row to determine current day and time
        latest_row = symbol_batch.orderBy(col("day").desc(), col("time").desc()).first()
        if not latest_row:
            continue
        
        # Fetch historical data from Cassandra for indicator calculation
        history_df = fetch_history(spark, symbol, latest_row["day"], latest_row["time"])
        
        # Normalize data types for union compatibility
        # History data: time is long, volume is long
        history_df_normalized = history_df.select(
            col("symbol"),
            col("day"),
            col("time"),  # already long from Cassandra
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume").cast("float")  # Cast to float for calculation consistency
        )
        
        # Current batch: time is long, volume is float
        symbol_batch_normalized = symbol_batch.select(
            col("symbol"),
            col("day"),
            col("time"),  # already long from transformation above
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume")  # already float
        )
        
        # Union current batch with historical data
        full_df = history_df_normalized.union(symbol_batch_normalized)
        
        # Calculate technical indicators
        enriched_df = calculate_indicators(full_df)
        
        # Filter to only include the current batch rows (not historical data)
        current_batch_rows = enriched_df.join(
            symbol_batch.select("symbol", "day", "time"), 
            ["symbol", "day", "time"], 
            "inner"
        )
        
        # Convert time back to proper format for Cassandra and cast volume to bigint
        final_df = current_batch_rows.select(
            col("symbol"),
            col("day"),
            col("time"),  # Keep as long since Cassandra expects long
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume").cast("bigint"),  # Cast to bigint for Cassandra
            # Technical indicators
            col("ma5"), col("ma10"), col("ma20"), col("ma50"), col("ma100"), col("ma200"),
            col("bb_upper"), col("bb_middle"), col("bb_lower"),
            col("macd_line"), col("macd_signal"), col("macd_histogram"),
            col("rsi14"),
            col("obv"),
            col("adx"), col("plus_di"), col("minus_di"),
            col("stoch_k"), col("stoch_d"),
            col("cci"), col("atr"), col("parabolic_sar"),
            col("volume_ma20"), col("mfi"),
            col("pivot"), col("support1"), col("support2"), col("support3"),
            col("resistance1"), col("resistance2"), col("resistance3")
        )
        
        # Write to Cassandra
        final_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="ohlcv_1m", keyspace="market_data") \
            .mode("append") \
            .save()

        print(f"Processed batch {batch_id} for symbol {symbol}")

# --- 5. Start streaming with foreachBatch ---
query = ohlcv_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .start()

query.awaitTermination()