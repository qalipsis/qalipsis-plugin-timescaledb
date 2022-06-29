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
import jakarta.inject.Singleton
import java.util.Properties

/**
 * Configuration for the export of micrometer [io.micrometer.core.instrument.Meter] to TimescaleDB.
 *
 * @author Gabriel Moraes
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = TimescaledbMeterRegistryFactory.TIMESCALEDB_ENABLED, notEquals = StringUtils.FALSE)
)
internal class TimescaledbMeterRegistryFactory {

    @Singleton
    @Bean(preDestroy = "stop")
    fun timescaleRegistry(environment: Environment): TimescaledbMeterRegistry {
        val properties = Properties()
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.RAW))
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.CAMEL_CASE))

        return TimescaledbMeterRegistry(object : TimescaledbMeterConfig() {
            override fun prefix() = "timescaledb"
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
