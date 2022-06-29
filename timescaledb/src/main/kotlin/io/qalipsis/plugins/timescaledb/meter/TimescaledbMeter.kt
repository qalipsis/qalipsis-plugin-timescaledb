package io.qalipsis.plugins.timescaledb.meter

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
    val tenant: String? = null,
    val campaign: String? = null,
    val scenario: String? = null,
    val count: BigDecimal? = null,
    val value: BigDecimal? = null,
    val sum: BigDecimal? = null,
    val mean: BigDecimal? = null,
    val activeTasks: Int? = null,
    val duration: BigDecimal? = null,
    val unit: String? = null,
    val max: BigDecimal? = null,
    val other: String? = null,
)