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
import io.qalipsis.api.report.TimeSeriesEvent
import io.qalipsis.api.report.TimeSeriesRecord
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import jakarta.inject.Singleton
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

/**
 * Converter from SQL result of time-series records to [io.qalipsis.api.report.TimeSeriesEvent].
 *
 * @author Eric Jessé
 */
@Singleton
internal class TimeSeriesEventRecordConverter(
    private val objectMapper: ObjectMapper
) : TimeSeriesRecordConverter {

    override fun convert(row: Row, metadata: RowMetadata): TimeSeriesRecord {
        return TimeSeriesEvent(
            name = row.get("name", String::class.java)!!,
            level = row.get("level", String::class.java)!!,
            timestamp = row.get("timestamp", Instant::class.java)!!,
            campaign = row.get("campaign", String::class.java),
            scenario = row.get("scenario", String::class.java),
            tags = row.get("tags", String::class.java)
                ?.let { objectMapper.readValue(it, Map::class.java) } as Map<String, String>?,
            message = row.get("message", String::class.java),
            stackTrace = row.get("stack_trace", String::class.java),
            error = row.get("error", String::class.java),
            date = row.get("date", Instant::class.java),
            boolean = row.get("boolean") as? Boolean,
            number = row.get("number", BigDecimal::class.java),
            duration = row.get("duration_nano", BigDecimal::class.java)?.toLong()?.let(Duration::ofNanos),
        )
    }
}