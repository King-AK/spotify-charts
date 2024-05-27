# /bin/bash


# Build the JAR
gradle clean build

# clean up the tmp scd directory
rm -rf /tmp/scd

# Run the pipeline locally using the test data and tmp directories
# Bronze
echo "Running BronzeSCDTableBuilder"
/opt/spark/bin/spark-submit \
--packages io.delta:delta-spark_2.12:3.2.0 \
--class "com.kingak.sc.service.bronzeIngestion.BronzeSCDTableBuilder" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
app/build/libs/spotify-charts-0.0.1.jar \
--input "app/src/test/resources/RawSpotifyChartData" \
--output "/tmp/scd/bronze/spotify_chart_data" \
--checkpoint "/tmp/scd/checkpoint/bronze-scd-checkpoint"
echo "BronzeSCDTableBuilder completed"

# Silver
echo "Running SilverSCDTableBuilder"
/opt/spark/bin/spark-submit \
--packages io.delta:delta-spark_2.12:3.2.0 \
--class "com.kingak.sc.service.silverIngestion.SilverSCDTableBuilder" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
app/build/libs/spotify-charts-0.0.1.jar \
--input "/tmp/scd/bronze/spotify_chart_data" \
--output "/tmp/scd/silver/spotify_chart_data" \
--checkpoint "/tmp/scd/checkpoint/silver-scd-checkpoint"
echo "SilverSCDTableBuilder completed"

# Gold
echo "Running GoldSCDTableBuilder"
/opt/spark/bin/spark-submit \
--packages io.delta:delta-spark_2.12:3.2.0 \
--class "com.kingak.sc.service.goldIngestion.GoldSCDTableBuilder" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
app/build/libs/spotify-charts-0.0.1.jar \
--input "/tmp/scd/silver/spotify_chart_data" \
--chart-data-output "/tmp/scd/gold/spotify_chart_data" \
--artist-data-output "/tmp/scd/gold/spotify_artist_data" \
--song-data-output "/tmp/scd/gold/spotify_song_data" \
--relationship-data-output "/tmp/scd/gold/spotify_relationship_data" \
--checkpoint "/tmp/scd/checkpoint/gold-scd-checkpoint"
echo "GoldSCDTableBuilder completed"


## Show the results
#/opt/bin/spark/bin/spark-shell \
#--packages io.delta:delta-spark_2.12:3.1.0 \
#--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#-I utils/ShowTmpTables.scala



