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

import io.micrometer.core.instrument.Clock
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.naming.conventions.StringConvention
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.api.meters.MeterRegistryConfiguration
import io.qalipsis.api.meters.MeterRegistryFactory
import jakarta.inject.Singleton
import java.time.Duration
import java.util.Properties

/**
 * Configuration for the export of micrometer [io.micrometer.core.instrument.Meter] to TimescaleDB.
 *
 * @author Gabriel Moraes
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = TimescaledbMeterRegistryFactory.TIMESCALEDB_ENABLED, value = StringUtils.TRUE)
)
internal class TimescaledbMeterRegistryFactory(environment: Environment) : MeterRegistryFactory {

    private val properties = Properties()

    init {
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.RAW))
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.CAMEL_CASE))
    }

    @Singleton
    @Bean(preDestroy = "stop")
    fun timescaleRegistry(): TimescaledbMeterRegistry {
        return TimescaledbMeterRegistry(object : TimescaledbMeterConfig() {
            override fun prefix() = "timescaledb"
            override fun get(key: String): String? {
                return properties.getProperty(key)
            }
        }, TimescaledbMeterConverter(), Clock.SYSTEM)
    }

    override fun getRegistry(configuration: MeterRegistryConfiguration): TimescaledbMeterRegistry {
        return TimescaledbMeterRegistry(object : TimescaledbMeterConfig() {
            override fun prefix() = "timescaledb"
            override fun step(): Duration = configuration.step ?: super.step()
            override fun get(key: String): String? {
                return properties.getProperty(key)
            }
        }, TimescaledbMeterConverter(), Clock.SYSTEM)
    }

    companion object {

        private const val TIMESCALEDB_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.timescaledb"

        internal const val TIMESCALEDB_ENABLED = "$TIMESCALEDB_CONFIGURATION.enabled"
    }
}
