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

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.FunctionTimer
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.util.StringEscapeUtils
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.NANOSECONDS

/**
 * Converter for Micrometer meters for TimescaleDB.
 *
 * @author Eric Jessé
 */
internal class TimescaledbMeterConverter {

    fun convert(
        meters: List<Meter>,
        instant: Instant,
        namingConvention: NamingConvention
    ): List<TimescaledbMeter> {
        val timestamp = Timestamp.from(instant)
        return meters.map { meter ->
            val tags = meter.id.getConventionTags(namingConvention)
            var tenant: String? = null
            var campaign: String? = null
            var scenario: String? = null

            val filteredTags = tags.mapNotNull { tag ->
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
                filteredTags.joinToString(",", prefix = "{", postfix = "}")
            } else {
                null
            }

            val timescaledbMeter = TimescaledbMeter(
                timestamp = timestamp,
                type = meter.id.type.toString().lowercase(),
                name = meter.id.getConventionName(namingConvention),
                tags = serializedTags,
                tenant = tenant,
                campaign = campaign,
                scenario = scenario,
            )
            when (meter) {
                is TimeGauge -> convertTimeGauge(meter, timescaledbMeter)
                is Gauge -> convertGauge(meter, timescaledbMeter)
                is Counter -> convertCounter(meter, timescaledbMeter)
                is Timer -> convertTimer(meter, timescaledbMeter)
                is DistributionSummary -> convertSummary(meter, timescaledbMeter)
                is LongTaskTimer -> convertLongTaskTimer(meter, timescaledbMeter)
                is FunctionCounter -> convertFunctionCounter(meter, timescaledbMeter)
                is FunctionTimer -> convertFunctionTimer(meter, timescaledbMeter)
                else -> convertMeter(meter, timescaledbMeter)
            }
        }
    }

    /**
     * Timescaledb converter for Counter.
     */
    private fun convertCounter(counter: Counter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return convertCounter(counter.count(), timescaledbMeter)
    }

    /**
     * Timescaledb converter for FunctionCounter.
     */
    private fun convertFunctionCounter(counter: FunctionCounter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return convertCounter(counter.count(), timescaledbMeter)
    }

    /**
     * Timescaledb converter for Counter with value.
     */
    private fun convertCounter(value: Double, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(count = BigDecimal(value))
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for Gauge.
     */
    private fun convertGauge(gauge: Gauge, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val value = gauge.value()
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(value = BigDecimal(value))
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for TimeGauge.
     */
    private fun convertTimeGauge(
        gauge: TimeGauge,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        val value = gauge.value(BASE_TIME_UNIT)
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(value = BigDecimal(value), unit = "$BASE_TIME_UNIT")
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for FunctionTimer.
     */
    private fun convertFunctionTimer(
        timer: FunctionTimer,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        val sum = timer.totalTime(BASE_TIME_UNIT)
        val mean = timer.mean(BASE_TIME_UNIT)
        return timescaledbMeter.copy(
            count = BigDecimal(timer.count()),
            sum = BigDecimal(sum),
            mean = BigDecimal(mean),
            unit = "$BASE_TIME_UNIT"
        )
    }

    /**
     * Timescaledb converter for LongTaskTimer.
     */
    private fun convertLongTaskTimer(
        timer: LongTaskTimer,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        return timescaledbMeter.copy(
            activeTasks = timer.activeTasks(),
            duration = BigDecimal(timer.duration(NANOSECONDS)),
            unit = "$BASE_TIME_UNIT"
        )
    }

    /**
     * Timescaledb converter for Timer.
     */
    private fun convertTimer(
        timer: Timer,
        timescaledbMeter: TimescaledbMeter
    ): TimescaledbMeter {
        return timescaledbMeter.copy(
            count = BigDecimal(timer.count()),
            sum = BigDecimal(timer.totalTime(BASE_TIME_UNIT)),
            mean = BigDecimal(timer.mean(BASE_TIME_UNIT)),
            max = BigDecimal(timer.max(BASE_TIME_UNIT)),
            unit = "$BASE_TIME_UNIT"
        )
    }

    /**
     * Timescaledb serializer for DistributionSummary.
     */
    private fun convertSummary(summary: DistributionSummary, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val histogramSnapshot = summary.takeSnapshot()
        return timescaledbMeter.copy(
            count = BigDecimal(histogramSnapshot.count()),
            sum = BigDecimal(histogramSnapshot.total()),
            mean = BigDecimal(histogramSnapshot.mean()),
            max = BigDecimal(histogramSnapshot.max())
        )
    }

    /**
     * Timescaledb further converter for previous kinds of Meter
     */
    private fun convertMeter(meter: Meter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val measurements = meter.measure()
        val names = mutableListOf<String>()
        // Snapshot values should be used throughout this method as there are chances for values to be changed in-between.
        val values = mutableListOf<Double>()
        for (measurement in measurements) {
            val value = measurement.value
            if (!java.lang.Double.isFinite(value)) {
                continue
            }
            names.add(measurement.statistic.tagValueRepresentation)
            values.add(value)
        }
        return if (names.isEmpty()) {
            timescaledbMeter
        } else {
            val otherMeasurements = names.indices.joinToString(",", prefix = "{", postfix = "}") { index ->
                """"${names[index]}":"${values[index]}""""
            }
            timescaledbMeter.copy(other = otherMeasurements)
        }
    }

    private companion object {

        val BASE_TIME_UNIT = TimeUnit.NANOSECONDS

    }

}