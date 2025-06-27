/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.timescaledb.event

import com.fasterxml.jackson.databind.ObjectMapper
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventConverter
import io.qalipsis.api.events.EventGeoPoint
import jakarta.inject.Singleton
import org.apache.commons.text.StringEscapeUtils
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
        var tenant: String? = null
        var campaign: String? = null
        var scenario: String? = null

        val filteredTags = event.tags.mapNotNull { (key, value) ->
            when (key) {
                "tenant" -> {
                    tenant = value
                    null
                }
                "campaign" -> {
                    campaign = value
                    null
                }
                "scenario" -> {
                    scenario = value
                    null
                }
                else -> {
                    """"${StringEscapeUtils.escapeJson(key).lowercase()}":"${StringEscapeUtils.escapeJson(value)}""""
                }
            }
        }
        val tags = if (filteredTags.isNotEmpty()) {
            filteredTags.joinToString(",", prefix = "{", postfix = "}")
        } else {
            null
        }
        val timescaledbEvent = TimescaledbEvent(
            timestamp = Timestamp.from(event.timestamp),
            level = event.level.toString().lowercase(),
            name = event.name,
            tenant = tenant,
            campaign = campaign,
            scenario = scenario,
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
                timescaledbEvent.copy(durationNano = value.toNanos().toBigDecimal())
            }
            is EventGeoPoint -> {
                timescaledbEvent.copy(geoPoint = """{"type": "Point","coordinates":[${value.latitude},${value.longitude}]}""")
            }

            is Throwable -> {
                timescaledbEvent.copy(error = value.message, stackTrace = stackTraceToString(value))
            }

            is Iterable<*> -> {
                var event = timescaledbEvent
                value.filterNotNull().forEach { event = addValue(it, event) }
                event
            }

            is Array<*> -> {
                var event = timescaledbEvent
                value.filterNotNull().forEach { event = addValue(it, event) }
                event
            }

            else -> {
                timescaledbEvent.copy(value = objectMapper.writeValueAsString(value))
            }
        }
    }

    /**
     * Converts the stack trace of a [Throwable] into a [String].
     */
    @KTestable
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