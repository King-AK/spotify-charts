package com.kingak.sc.model

case class SpotifyChartData( // TODO rename to RawSpotifyChartData, and make a new case class SpotifyChartData
    id: String,
    title: String,
    rank: Int,
    date: java.time.LocalDate,
    artist: String,
    url: String,
    region: String,
    chart: String, // TODO enum
    trend: String, // TODO enum
    streams: Long,
    track_id: String,
    album: String,
    popularity: Double,
    duration_ms: Long,
    explicit: Boolean,
    release_date: java.time.LocalDate,
    available_markets: String, // TODO seq[String]
    af_danceability: Double,
    af_energy: Double,
    af_key: Int,
    af_loudness: Double,
    af_mode: Int,
    af_speechiness: Double,
    af_acousticness: Double,
    af_instrumentalness: Double,
    af_liveness: Double,
    af_valence: Double,
    af_tempo: Double,
    af_time_signature: Int
)
