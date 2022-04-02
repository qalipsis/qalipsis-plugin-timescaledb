package io.qalipsis.plugins.r2dbc.events

import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventConverter
import io.qalipsis.api.events.EventGeoPoint
import io.qalipsis.api.events.EventRange
import jakarta.inject.Singleton
import java.io.IOException
import java.io.PrintWriter
import java.io.StringWriter
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

/**
 * Implementation of [EventConverter] to generate a [TimescaledbEvent].
 *
 * @author Gabriel Moraes
 */
@Singleton
class TimescaledbEventConverter : EventConverter<TimescaledbEvent> {

    /**
     * Generates a [TimescaledbEvent] representation of an event.
     *
     * Any type can be used for value, but [Boolean]s, [Number]s, [String]s, [java.time.temporal.Temporal]s, and [Throwable]s are interpreted.
     */
    override fun convert(event: Event): TimescaledbEvent {
        val timestamp = event.timestamp
        val level = event.level.toString().lowercase()
        val tags = event.tags.joinToString(",") { tag ->
            """"${tag.key}":"${tag.value}""""
        }
        val name = event.name

        val timescaledbEvent = TimescaledbEvent(timestamp = timestamp, level = level, tags = tags, name = name)

        return event.value?.let { addValue(it, timescaledbEvent) } ?: timescaledbEvent
    }

    private fun addValue(value: Any, timescaledbEvent: TimescaledbEvent): TimescaledbEvent {
        return when {
            value is String -> {
               timescaledbEvent.copy(message = value)
            }
            value is Boolean -> {
                timescaledbEvent.copy(boolean = value)
            }
            value is Number -> {
                timescaledbEvent.copy(number = BigDecimal(value.toDouble()))
            }
            value is Instant -> {
                timescaledbEvent.copy(date = value)
            }
            value is ZonedDateTime -> {
                timescaledbEvent.copy(date = value.toInstant())
            }
            value is LocalDateTime -> {
                timescaledbEvent.copy(date = value.atZone(ZoneId.systemDefault()).toInstant())
            }
            value is Throwable -> {
                timescaledbEvent.copy(error = value.message, stackTrace = stackTraceToString(value))
            }
            else -> {
                timescaledbEvent.copy(value = value.toString())
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

    companion object {

        @JvmStatic
        private val TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT
    }

}