package io.qalipsis.plugins.r2dbc.meters

import java.math.BigDecimal
import java.time.Instant

/**
 * Representation of meter to save into TimescaleDB.
 *
 * @author Palina Bril
 */
data class TimescaledbMeter(
    val timestamp: Instant,
    val type: String,
    val count: BigDecimal? = null,
    val value: BigDecimal? = null,
    val sum: BigDecimal? = null,
    val mean: BigDecimal? = null,
    val activeTasks: Int? = null,
    val duration: BigDecimal? = null,
    val max: BigDecimal? = null,
    val name: String,
    val tags: String,
    val other: String? = null,
)