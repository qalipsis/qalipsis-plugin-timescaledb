package io.qalipsis.plugins.r2dbc.config

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.step.StepRegistryConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.naming.conventions.StringConvention
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.meters.MetersConfig
import io.qalipsis.plugins.r2dbc.meters.TimescaledbMeterConfig
import io.qalipsis.plugins.r2dbc.meters.TimescaledbMeterRegistry
import jakarta.inject.Singleton
import java.util.Properties

/**
 * Configuration for the export of micrometer [io.micrometer.core.instrument.Meter] to TimescaleDB.
 *
 * @author Gabriel Moraes
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = TimescaledbMeterRegistryFactory.TIMESCALEDB_ENABLED, notEquals = StringUtils.FALSE)
)
internal class TimescaledbMeterRegistryFactory {

    @Singleton
    fun timescaleRegistry(environment: Environment): TimescaledbMeterRegistry {
        val properties = Properties()
        properties.putAll(environment.getProperties(MetersConfig.CONFIGURATION, StringConvention.RAW))
        properties.putAll(environment.getProperties(MetersConfig.CONFIGURATION, StringConvention.CAMEL_CASE))

        return TimescaledbMeterRegistry(object : TimescaledbMeterConfig() {
            override fun prefix() = "timescaledb"
            override fun get(key: String): String? {
                return properties.getProperty(key)
            }

        }, Clock.SYSTEM)
    }

    companion object {

        private const val TIMESCALEDB_CONFIGURATION = "${MetersConfig.CONFIGURATION}.timescaledb"

        internal const val TIMESCALEDB_ENABLED = "$TIMESCALEDB_CONFIGURATION.enabled"
    }
}
