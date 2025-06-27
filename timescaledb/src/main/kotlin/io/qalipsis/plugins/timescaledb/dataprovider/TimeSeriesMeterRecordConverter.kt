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

package io.qalipsis.plugins.timescaledb.dataprovider

import com.fasterxml.jackson.databind.ObjectMapper
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.report.TimeSeriesMeter
import io.qalipsis.api.report.TimeSeriesRecord
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import jakarta.inject.Singleton
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

/**
 * Converter from SQL result of time-series records to [io.qalipsis.api.report.TimeSeriesMeter].
 *
 * @author Eric Jessé
 */
@Suppress("UNCHECKED_CAST")
@Singleton
internal class TimeSeriesMeterRecordConverter(
    private val objectMapper: ObjectMapper
) : TimeSeriesRecordConverter {

    override fun convert(row: Row, metadata: RowMetadata): TimeSeriesRecord {
        return when (val type = row.get("type", String::class.java)!!) {
            MeterType.TIMER.value.lowercase() -> convertTimer(type, row)
            else -> convertNonTimer(type, row)
        }
    }

    private fun convertTimer(type: String, row: Row): TimeSeriesMeter {
        return TimeSeriesMeter(
            name = row.get("name", String::class.java)!!,
            type = type,
            timestamp = row.get("timestamp", Instant::class.java)!!,
            campaign = row.get("campaign", String::class.java),
            scenario = row.get("scenario", String::class.java),
            tags = row.get("tags", String::class.java)
                ?.let { objectMapper.readValue(it, Map::class.java) } as Map<String, String>?,
            count = row.get("count", BigDecimal::class.java)?.toLong(),
            sumDuration = row.get("sum", BigDecimal::class.java)?.toLong()?.times(1_000)?.let(Duration::ofNanos),
            maxDuration = row.get("max", BigDecimal::class.java)?.toLong()?.times(1_000)?.let(Duration::ofNanos),
            meanDuration = row.get("mean", BigDecimal::class.java)?.toLong()?.times(1_000)?.let(Duration::ofNanos),
            other = (row.get("other", String::class.java)
                ?.let { objectMapper.readValue(it, Map::class.java) } as Map<String, String>?)
                ?.mapValues { BigDecimal(it.value) }
        )
    }

    private fun convertNonTimer(type: String, row: Row): TimeSeriesMeter {
        return TimeSeriesMeter(
            name = row.get("name", String::class.java)!!,
            type = type,
            timestamp = row.get("timestamp", Instant::class.java)!!,
            campaign = row.get("campaign", String::class.java),
            scenario = row.get("scenario", String::class.java),
            tags = row.get("tags", String::class.java)
                ?.let { objectMapper.readValue(it, Map::class.java) } as Map<String, String>?,
            count = row.get("count", BigDecimal::class.java)?.toLong(),
            sum = row.get("sum", BigDecimal::class.java),
            mean = row.get("mean", BigDecimal::class.java),
            max = row.get("max", BigDecimal::class.java),
            value = row.get("value", BigDecimal::class.java),
            other = (row.get("other", String::class.java)
                ?.let { objectMapper.readValue(it, Map::class.java) } as Map<String, String>?)
                ?.mapValues { BigDecimal(it.value) }
        )
    }
}