/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.timescaledb.event

import com.fasterxml.jackson.databind.ObjectMapper
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.util.StringEscapeUtils
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