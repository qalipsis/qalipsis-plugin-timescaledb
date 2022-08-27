package io.qalipsis.plugins.timescaledb.dataprovider

import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.Meter
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
            "${Meter.Type.TIMER}".lowercase() -> convertTimer(type, row)
            "${Meter.Type.LONG_TASK_TIMER}".lowercase() -> convertTimer(type, row)
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
            sumDuration = row.get("sum", BigDecimal::class.java)?.toLong()?.let(Duration::ofNanos),
            meanDuration = row.get("mean", BigDecimal::class.java)?.toLong()?.let(Duration::ofNanos),
            maxDuration = row.get("max", BigDecimal::class.java)?.toLong()?.let(Duration::ofNanos),

            activeTasks = row.get("active_tasks", BigDecimal::class.java)?.toInt(),
            duration = row.get("duration", BigDecimal::class.java)?.toLong()?.let(Duration::ofNanos),
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