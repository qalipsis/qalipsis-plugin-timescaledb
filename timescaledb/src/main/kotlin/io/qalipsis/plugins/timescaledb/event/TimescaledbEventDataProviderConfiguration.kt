package io.qalipsis.plugins.timescaledb.event

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.bind.annotation.Bindable
import io.micronaut.core.util.StringUtils
import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import java.time.Duration
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.Positive

/**
 * Configuration for [TimescaledbEventDataProvider].
 *
 * @author Eric Jessé
 */
@Requires(property = "events.provider.timescaledb.enabled", value = StringUtils.TRUE, defaultValue = StringUtils.FALSE)
@ConfigurationProperties("events.provider.timescaledb")
interface TimescaledbEventDataProviderConfiguration : DataProviderConfiguration {

    @get:Bindable(defaultValue = "localhost")
    @get:NotBlank
    override val host: String

    @get:Bindable(defaultValue = "5432")
    @get:Positive
    override val port: Int

    @get:Bindable(defaultValue = "qalipsis")
    @get:NotBlank
    override val database: String

    @get:Bindable(defaultValue = "meters")
    @get:NotBlank
    override val schema: String

    @get:NotBlank
    override val username: String

    @get:NotBlank
    override val password: String

    @get:Bindable(defaultValue = "1")
    @get:Min(1)
    override val minSize: Int

    @get:Bindable(defaultValue = "4")
    @get:Min(1)
    override val maxSize: Int

    @get:Bindable(defaultValue = "PT1M")
    override val maxIdleTime: Duration
}
