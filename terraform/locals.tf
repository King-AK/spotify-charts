locals {
  # ABFSS root path
  abfss_root_path = format("abfss://%s@%s.dfs.core.windows.net", var.poc_container_name, var.poc_storage_account)

  # Artifact paths
  artifact_root         = "artifacts"
  product_jar_blob_path = format("%s/spotify-charts.jar", local.artifact_root)
  product_jar_blob_url  = format("%s/%s", local.abfss_root_path, local.product_jar_blob_path)

  # Checkpoint paths
  checkpoint_root        = "checkpoints"
  bronze_checkpoint_path = format("%s/bronze-scd-checkpoint", local.checkpoint_root)
  silver_checkpoint_path = format("%s/silver-scd-checkpoint", local.checkpoint_root)
  gold_checkpoint_path   = format("%s/gold-scd-checkpoint", local.checkpoint_root)

  # Landing paths
  landing_root     = "landing"
  scd_landing_path = format("%s/spotify-csv-batches", local.landing_root)

  # Delta DB paths
  delta_root                        = "delta"
  bronze_delta_db_path              = format("%s/bronze_scd", local.delta_root)
  silver_delta_db_path              = format("%s/silver_scd", local.delta_root)
  gold_delta_db_path                = format("%s/gold_scd", local.delta_root)
  # Delta Table paths
  bronze_scd_table_path             = format("%s/spotify_chart_data", local.bronze_delta_db_path)
  silver_scd_table_path             = format("%s/spotify_chart_data", local.silver_delta_db_path)
  gold_scd_chart_table_path         = format("%s/spotify_chart_data", local.gold_delta_db_path)
  gold_scd_artist_table_path        = format("%s/spotify_chart_artist_data", local.gold_delta_db_path)
  gold_scd_song_table_path          = format("%s/spotify_chart_song_data", local.gold_delta_db_path)
  gold_scd_relationships_table_path = format("%s/spotify_chart_relationships", local.gold_delta_db_path)

}