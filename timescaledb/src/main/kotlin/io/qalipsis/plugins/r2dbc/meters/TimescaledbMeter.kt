package io.qalipsis.plugins.r2dbc.meters

import java.math.BigDecimal
import java.sql.Timestamp

/**
 * Representation of meter to save into TimescaleDB.
 *
 * @author Palina Bril
 */
internal data class TimescaledbMeter(
    val name: String,
    val tags: String?,
    val timestamp: Timestamp,
    val type: String,
    val count: BigDecimal? = null,
    val value: BigDecimal? = null,
    val sum: BigDecimal? = null,
    val mean: BigDecimal? = null,
    val activeTasks: Int? = null,
    val duration: BigDecimal? = null,
    val max: BigDecimal? = null,
    val other: String? = null,
)