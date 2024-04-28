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

package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.DistributionSummary
import io.qalipsis.api.meters.Gauge
import io.qalipsis.api.meters.Measurement
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.meters.UnsupportedMeterException
import org.apache.commons.text.StringEscapeUtils
import java.math.BigDecimal
import java.sql.Timestamp
import java.util.concurrent.TimeUnit.NANOSECONDS

/**
 * Converter for QALIPSIS meters for TimescaleDB.
 *
 * @author Eric Jessé
 */
internal class TimescaledbMeterConverter {

    fun convert(
        meterSnapshots: Collection<MeterSnapshot<*>>
    ): List<TimescaledbMeter> {
        return meterSnapshots.map { snapshot ->
            val snapshotMeter = snapshot.meter.id
            var tenant: String? = null
            val campaign: String = snapshotMeter.campaignKey.lowercase()
            val scenario: String = snapshotMeter.scenarioName.lowercase()

            val filteredTags = snapshotMeter.tags.mapNotNull { tag ->
                when (tag.key) {
                    "tenant" -> {
                        tenant = tag.value
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
                filteredTags.joinToString(",", prefix = "{", postfix = "}")
            } else {
                null
            }

            val timescaledbMeter = TimescaledbMeter(
                timestamp = Timestamp.from(snapshot.timestamp),
                type = snapshot.meter.id.type.value,
                name = snapshot.meter.id.meterName,
                tags = serializedTags,
                tenant = tenant,
                campaign = campaign,
                scenario = scenario,
            )
            when (snapshot.meter) {
                is Gauge -> convertGauge(snapshot.measurements, timescaledbMeter)
                is Counter -> convertCounter(snapshot.measurements, timescaledbMeter)
                is Timer -> convertTimer(snapshot.measurements, timescaledbMeter)
                is DistributionSummary -> convertSummary(snapshot.measurements, timescaledbMeter)
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
            count = BigDecimal(statToValue["value"] ?: 0.0),
            sum = BigDecimal(statToValue["total_time"] ?: 0.0),
            mean = BigDecimal(statToValue["mean"] ?: 0.0),
            max = BigDecimal(statToValue["max"] ?: 0.0),
            unit = "$BASE_TIME_UNIT",
            other = other.takeIf { it.isNotEmpty() }?.joinToString(",", prefix = "{", postfix = "}")
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
            count = BigDecimal(statToValue["count"] ?: 0.0),
            sum = BigDecimal(statToValue["total"] ?: 0.0),
            mean = BigDecimal(statToValue["mean"] ?: 0.0),
            max = BigDecimal(statToValue["max"] ?: 0.0),
            unit = "$BASE_TIME_UNIT",
            other = other.takeIf { it.isNotEmpty() }?.joinToString(",", prefix = "{", postfix = "}")
        )
    }

    private companion object {

        val BASE_TIME_UNIT = NANOSECONDS

    }

}