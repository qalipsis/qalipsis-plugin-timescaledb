package io.qalipsis.plugins.r2dbc.events

import com.fasterxml.jackson.databind.ObjectMapper
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventConverter
import io.qalipsis.api.events.EventGeoPoint
import jakarta.inject.Singleton
import java.io.IOException
import java.io.PrintWriter
import java.io.StringWriter
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * Implementation of [EventConverter] to generate a [TimescaledbEvent].
 *
 * @author Gabriel Moraes
 */
@Singleton
internal class TimescaledbEventConverter(
    private val objectMapper: ObjectMapper
) : EventConverter<TimescaledbEvent> {

    /**
     * Generates a [TimescaledbEvent] representation of an event.
     *
     * Any type can be used for value, but [Boolean]s, [Number]s, [String]s, [java.time.temporal.Temporal]s, and [Throwable]s are interpreted.
     */
    override fun convert(event: Event): TimescaledbEvent {
        val tags = if (event.tags.isNotEmpty()) {
            event.tags.joinToString(",", prefix = "{", postfix = "}") { (key, value) -> """"$key":"$value"""" }
        } else {
            null
        }
        val timescaledbEvent = TimescaledbEvent(
            timestamp = Timestamp.from(event.timestamp),
            level = event.level.toString().lowercase(),
            name = event.name,
            tags = tags
        )
        return event.value?.let { addValue(it, timescaledbEvent) } ?: timescaledbEvent
    }

    private fun addValue(value: Any, timescaledbEvent: TimescaledbEvent): TimescaledbEvent {
        return when (value) {
            is String -> {
                timescaledbEvent.copy(message = value)
            }
            is Boolean -> {
                timescaledbEvent.copy(boolean = value)
            }
            is Number -> {
                timescaledbEvent.copy(number = BigDecimal(value.toDouble()))
            }
            is Instant -> {
                timescaledbEvent.copy(date = Timestamp.from(value))
            }
            is ZonedDateTime -> {
                timescaledbEvent.copy(date = Timestamp.from(value.toInstant()))
            }
            is LocalDateTime -> {
                timescaledbEvent.copy(date = Timestamp.from(value.atZone(ZoneId.systemDefault()).toInstant()))
            }
            is Duration -> {
                timescaledbEvent.copy(duration = value.toNanos().toBigDecimal())
            }
            is EventGeoPoint -> {
                timescaledbEvent.copy(geoPoint = """{"type": "Point","coordinates":[${value.latitude},${value.longitude}]}""")
            }
            is Throwable -> {
                timescaledbEvent.copy(error = value.message, stackTrace = stackTraceToString(value))
            }
            else -> {
                timescaledbEvent.copy(value = objectMapper.writeValueAsString(value))
            }
        }
    }

    /**
     * Converts the stack trace of a [Throwable] into a [String].
     */
    private fun stackTraceToString(throwable: Throwable): String {
        try {
            StringWriter().use { sw ->
                PrintWriter(sw).use { pw ->
                    throwable.printStackTrace(pw)
                    return sw.toString()
                }
            }
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

}