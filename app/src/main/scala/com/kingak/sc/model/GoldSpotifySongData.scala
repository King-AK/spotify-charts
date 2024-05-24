package com.kingak.sc.model
package com.kingak.sc.model

case class GoldSpotifySongData(
    songUUID: String,
    title: String,
    artist: String,
    url: String,
    track_id: String,
    album: String,
    duration_ms: Long,
    explicit: Boolean,
    release_date: java.time.LocalDate,
    available_markets: Seq[String],
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
