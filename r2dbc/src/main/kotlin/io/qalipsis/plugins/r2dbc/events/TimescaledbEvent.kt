package io.qalipsis.plugins.r2dbc.events

import java.math.BigDecimal
import java.time.Instant

/**
 * Representation of event to save into TimescaleDB.
 *
 * @author Gabriel Moraes
 */
internal data class TimescaledbEvent(
    val id: Long? = null,
    val timestamp: Instant,
    val level: String,
    val tags: String,
    val name: String,
    val message: String? = null,
    val stackTrace: String? = null,
    val error: String? = null,
    val instant: Instant? = null,
    val boolean: Boolean = false,
    val number: BigDecimal? = null,
    val value: String? = null
)
