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

package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.Measurement
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.api.meters.UnsupportedMeterException
import org.apache.commons.text.StringEscapeUtils
import java.math.BigDecimal
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

/**
 * Converter for QALIPSIS meters for TimescaleDB.
 *
 * @author Eric Jessé
 */
internal class TimescaledbMeterConverter {

    fun convert(
        meterSnapshots: Collection<MeterSnapshot>
    ): List<TimescaledbMeter> {
        return meterSnapshots.map { snapshot ->
            val snapshotMeter = snapshot.meterId
            var tenant: String? = null
            var campaign: String? = null
            var scenario: String? = null

            val filteredTags = snapshotMeter.tags.mapNotNull { tag ->
                when (tag.key) {
                    "tenant" -> {
                        tenant = tag.value
                        null
                    }

                    "campaign" -> {
                        campaign = tag.value
                        null
                    }

                    "scenario" -> {
                        scenario = tag.value
                        null
                    }

                    else -> {
                        """"${
                            StringEscapeUtils.escapeJson(tag.key).lowercase()
                        }":"${StringEscapeUtils.escapeJson(tag.value)}""""
                    }
                }
            }
            val serializedTags = if (filteredTags.isNotEmpty()) {
                filteredTags.sorted().joinToString(",", prefix = "{", postfix = "}")
            } else {
                null
            }

            val timescaledbMeter = TimescaledbMeter(
                timestamp = Timestamp.from(snapshot.timestamp),
                type = snapshot.meterId.type.value,
                name = snapshot.meterId.meterName,
                tags = serializedTags,
                tenant = tenant,
                campaign = campaign,
                scenario = scenario,
            )
            when (snapshot.meterId.type) {
                MeterType.GAUGE -> convertGauge(snapshot.measurements, timescaledbMeter)
                MeterType.COUNTER -> convertCounter(snapshot.measurements, timescaledbMeter)
                MeterType.TIMER -> convertTimer(snapshot.measurements, timescaledbMeter)
                MeterType.DISTRIBUTION_SUMMARY -> convertSummary(snapshot.measurements, timescaledbMeter)
                MeterType.RATE -> convertRate(snapshot.measurements, timescaledbMeter)
                MeterType.THROUGHPUT -> convertThroughput(snapshot.measurements, timescaledbMeter)
                else -> throw UnsupportedMeterException("Meter ${snapshotMeter.meterName} not supported")
            }
        }
    }

    /**
     * Timescaledb converter for Counter.
     */
    private fun convertCounter(
        measurements: Collection<Measurement>,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        measurements.forEach {
            if (java.lang.Double.isFinite(it.value)) {
                return timescaledbMeter.copy(count = BigDecimal(it.value))
            }
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for Gauge.
     */
    private fun convertGauge(
        measurements: Collection<Measurement>,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        measurements.forEach {
            if (java.lang.Double.isFinite(it.value)) {
                return timescaledbMeter.copy(value = BigDecimal(it.value))
            }
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for Timer.
     */
    private fun convertTimer(
        measurements: Collection<Measurement>,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        val statToValue = mutableMapOf<String, Double>()
        val other = mutableListOf<String>()
        measurements.forEach { measurement ->
            val key = measurement.statistic.value
            val value = BigDecimal(measurement.value).toString()
            when (measurement) {
                is DistributionMeasurementMetric -> {
                    val entry =
                        """"${
                            StringEscapeUtils.escapeJson("${key}_${measurement.observationPoint}").lowercase()
                        }":"${BigDecimal(value)}""""
                    other.add(entry)
                }

                else -> statToValue[key] = measurement.value
            }
        }
        return timescaledbMeter.copy(
            count = BigDecimal(statToValue[Statistic.COUNT.value] ?: 0.0),
            sum = BigDecimal(statToValue[Statistic.TOTAL_TIME.value] ?: 0.0),
            mean = BigDecimal(statToValue[Statistic.MEAN.value] ?: 0.0),
            max = BigDecimal(statToValue[Statistic.MAX.value] ?: 0.0),
            unit = "${TimeUnit.MICROSECONDS}",
            other = other.takeIf { it.isNotEmpty() }?.sorted()?.joinToString(",", prefix = "{", postfix = "}")
        )
    }

    /**
     * Timescaledb serializer for DistributionSummary.
     */
    private fun convertSummary(
        measurements: Collection<Measurement>,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        val statToValue = mutableMapOf<String, Double>()
        val other = mutableListOf<String>()
        measurements.forEach { measurement ->
            val key = measurement.statistic.value
            val value = BigDecimal(measurement.value).toString()
            when (measurement) {
                is DistributionMeasurementMetric -> {
                    val entry =
                        """"${
                            StringEscapeUtils.escapeJson("${key}_${measurement.observationPoint}").lowercase()
                        }":"$value""""
                    other.add(entry)
                }

                else -> statToValue[key] = measurement.value
            }
        }
        return timescaledbMeter.copy(
            count = BigDecimal(statToValue[Statistic.COUNT.value] ?: 0.0),
            sum = BigDecimal(statToValue[Statistic.TOTAL.value] ?: 0.0),
            mean = BigDecimal(statToValue[Statistic.MEAN.value] ?: 0.0),
            max = BigDecimal(statToValue[Statistic.MAX.value] ?: 0.0),
            other = other.takeIf { it.isNotEmpty() }?.sorted()?.joinToString(",", prefix = "{", postfix = "}")
        )
    }

    /**
     * Timescaledb converter for Rate.
     */
    private fun convertRate(
        measurements: Collection<Measurement>,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        measurements.forEach {
            if (java.lang.Double.isFinite(it.value)) {
                return timescaledbMeter.copy(value = BigDecimal(it.value))
            }
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb serializer for Throughput.
     */
    private fun convertThroughput(
        measurements: Collection<Measurement>,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        val statToValue = mutableMapOf<String, Double>()
        val other = mutableListOf<String>()
        measurements.forEach { measurement ->
            val key = measurement.statistic.value
            val value = BigDecimal(measurement.value).toString()
            when (measurement) {
                is DistributionMeasurementMetric -> {
                    val entry =
                        """"${
                            StringEscapeUtils.escapeJson("${key}_${measurement.observationPoint}").lowercase()
                        }":"$value""""
                    other.add(entry)
                }

                else -> statToValue[key] = measurement.value
            }
        }
        return timescaledbMeter.copy(
            value = BigDecimal(statToValue[Statistic.VALUE.value] ?: 0.0),
            sum = BigDecimal(statToValue[Statistic.TOTAL.value] ?: 0.0),
            mean = BigDecimal(statToValue[Statistic.MEAN.value] ?: 0.0),
            max = BigDecimal(statToValue[Statistic.MAX.value] ?: 0.0),
            other = other.takeIf { it.isNotEmpty() }?.sorted()?.joinToString(",", prefix = "{", postfix = "}")
        )
    }

}