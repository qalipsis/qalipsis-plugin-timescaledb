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