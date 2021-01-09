## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

It either increase or decrease the throughput and latency. For example by increasing the value of maxOffsetsPerTrigger, it will increase the throughput because of lesser number of network calls but at the same time increases the latency.




## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

spark.default.parallelism
spark.streaming.kafka.maxRatePerPartition
spark.sql.shuffle.partitions

spark.default.parallelism -> defaults to the number of all cores on all machines. The more the core the more we get throughput, provided data is partetioned properly.

spark.streaming.kafka.maxRatePerPartition -> It is the maximum rate (in messages per second) at which each Kafka partition will be read by this direct API.

spark.sql.shuffle.partitions -> configures the number of partitions that are used when shuffling data for joins or aggregations. Normally this  is being used when we have a memory congestion and we see the error: spark error:java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE
