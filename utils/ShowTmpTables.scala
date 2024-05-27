// Show the tables in the tmp directory

println("Bronze table:")
val bronzeTablePath = "/tmp/scd/bronze/spotify_chart_data"
val df = spark.read.format("delta").load(bronzeTablePath)
df.show(truncate=false)

println("Silver table:")
val silverTablePath = "/tmp/scd/silver/spotify_chart_data"
val df = spark.read.format("delta").load(silverTablePath)
df.show(truncate=false)

println("Gold tables:")
println("Gold table Chart Data:")
val goldTablePath = "/tmp/scd/gold/spotify_chart_data"
val df = spark.read.format("delta").load(goldTablePath)
df.show(truncate=false)