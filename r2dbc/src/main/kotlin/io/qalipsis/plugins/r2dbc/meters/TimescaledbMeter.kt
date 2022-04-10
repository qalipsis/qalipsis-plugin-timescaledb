package io.qalipsis.plugins.r2dbc.meters

import java.time.Instant

/**
 * Representation of meter to save into TimescaleDB.
 *
 * @author Palina Bril
 */
data class TimescaledbMeter(
    val id: Long? = null,
    val timestamp: Instant,
    val type: String,
    val count: Double? = null,
    val value: Double? = null,
    val sum: Double? = null,
    val mean: Double? = null,
    val activeTasks: Int? = null,
    val duration: Double? = null,
    val max: Double? = null,
    val name: String,
    val tags: String,
    val other: String? = null,
)