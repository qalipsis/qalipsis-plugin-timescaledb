package io.qalipsis.plugins.r2dbc.config

import io.micrometer.core.instrument.Clock
import io.micrometer.elastic.ElasticConfig
import io.micrometer.elastic.ElasticMeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.naming.conventions.StringConvention
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.meters.MetersConfig
import jakarta.inject.Singleton
import java.util.Properties

/**
 * Configuration for the export of micrometer [io.micrometer.core.instrument.Meter] to Elasticsearch.
 *
 * @author Eric Jessé
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = ElasticsearchMeterRegistryFactory.ELASTICSEARCH_ENABLED, notEquals = StringUtils.FALSE)
)
internal class ElasticsearchMeterRegistryFactory {

    @Singleton
    fun elasticsearchRegistry(environment: Environment): ElasticMeterRegistry {
        val properties = Properties()
        properties.putAll(environment.getProperties(MetersConfig.CONFIGURATION, StringConvention.RAW))
        properties.putAll(environment.getProperties(MetersConfig.CONFIGURATION, StringConvention.CAMEL_CASE))

        return ElasticMeterRegistry(object : ElasticConfig {
            override fun prefix() = "elasticsearch"
            override fun get(key: String): String? {
                return properties.getProperty(key)
            }

        }, Clock.SYSTEM)
    }

    companion object {

        internal const val ELASTICSEARCH_CONFIGURATION = "${MetersConfig.CONFIGURATION}.elasticsearch"

        internal const val ELASTICSEARCH_ENABLED = "$ELASTICSEARCH_CONFIGURATION.enabled"
    }
}
