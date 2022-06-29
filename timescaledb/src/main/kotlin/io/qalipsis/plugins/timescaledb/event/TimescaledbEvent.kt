package io.qalipsis.plugins.timescaledb.event

import java.math.BigDecimal
import java.sql.Timestamp

/**
 * Representation of event to save into TimescaleDB.
 *
 * @author Gabriel Moraes
 */
data class TimescaledbEvent(
    val id: Long? = null,
    val timestamp: Timestamp,
    val level: String,
    val name: String,
    val tenant: String? = null,
    val campaign: String? = null,
    val scenario: String? = null,
    val tags: String? = null,
    val message: String? = null,
    val stackTrace: String? = null,
    val error: String? = null,
    val date: Timestamp? = null,
    val boolean: Boolean = false,
    val number: BigDecimal? = null,
    val durationNano: BigDecimal? = null,
    val geoPoint: String? = null,
    val value: String? = null
)
