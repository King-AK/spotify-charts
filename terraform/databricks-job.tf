data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_job" "spotify_chart_pipeline" {
  name        = "spotify_chart_pipeline"
  description = "Streams Spotify charts data through Data Lake"

  job_cluster {
    job_cluster_key = "spotify_chart_pipeline_cluster"
    new_cluster {
      num_workers   = 1
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = data.databricks_node_type.smallest.id

      spark_conf = {
        format("fs.azure.account.key.%s.dfs.core.windows.net", var.poc_storage_account) = format("{{secrets/%s/poc-storage-account-key}}", databricks_secret_scope.poc_secret_scope.name)
        }
    }
  }

  task {
    task_key        = "bronzeIngestion"
    job_cluster_key = "spotify_chart_pipeline_cluster"
    spark_jar_task {
      main_class_name = "com.kingak.sc.service.bronzeIngestion.BronzeSCDTableBuilder"
      parameters      = [
        "--input", format("%s/%s", local.abfss_root_path, local.scd_landing_path),
        "--output", format("%s/%s", local.abfss_root_path, local.bronze_scd_table_path),
        "--checkpoint", format("%s/%s", local.abfss_root_path, local.bronze_checkpoint_path)
      ]
    }
    library {
      jar = local.product_jar_blob_url
    }
  }

  task {
    task_key        = "silverIngestion"
    job_cluster_key = "spotify_chart_pipeline_cluster"
    spark_jar_task {
      main_class_name = "com.kingak.sc.service.silverIngestion.SilverSCDTableBuilder"
      parameters      = [
        "--input", format("%s/%s", local.abfss_root_path, local.bronze_scd_table_path),
        "--output", format("%s/%s", local.abfss_root_path, local.silver_scd_table_path),
        "--checkpoint", format("%s/%s", local.abfss_root_path, local.silver_checkpoint_path)
      ]
    }
    library {
      jar = local.product_jar_blob_url
    }
    depends_on {
      task_key = "bronzeIngestion"
    }
  }

  task {
    task_key        = "goldIngestion"
    job_cluster_key = "spotify_chart_pipeline_cluster"
    spark_jar_task {
      main_class_name = "com.kingak.sc.service.goldIngestion.GoldSCDTableBuilder"
      parameters      = [
        "--input", format("%s/%s", local.abfss_root_path, local.silver_scd_table_path),
        "--chart-data-output", format("%s/%s", local.abfss_root_path, local.gold_scd_chart_table_path),
        "--artist-data-output", format("%s/%s", local.abfss_root_path, local.gold_scd_artist_table_path),
        "--song-data-output", format("%s/%s", local.abfss_root_path, local.gold_scd_song_table_path),
        "--relationship-data-output", format("%s/%s", local.abfss_root_path, local.gold_scd_relationships_table_path),
        "--checkpoint", format("%s/%s", local.abfss_root_path, local.gold_checkpoint_path)
      ]
    }
    library {
      jar = local.product_jar_blob_url
    }
    depends_on {
      task_key = "silverIngestion"
    }
  }
}