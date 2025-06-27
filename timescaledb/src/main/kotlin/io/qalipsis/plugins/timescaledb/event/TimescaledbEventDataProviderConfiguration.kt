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

package io.qalipsis.plugins.timescaledb.event

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.bind.annotation.Bindable
import io.micronaut.core.util.StringUtils
import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import io.r2dbc.postgresql.client.SSLMode
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

    @get:Bindable(defaultValue = "false")
    override val enableSsl: Boolean

    @get:Bindable(defaultValue = "PREFER")
    override val sslMode: SSLMode

    override val sslRootCert: String?

    override val sslCert: String?

    override val sslKey: String?

    @get:Bindable(defaultValue = "1")
    @get:Min(1)
    override val minSize: Int

    @get:Bindable(defaultValue = "4")
    @get:Min(1)
    override val maxSize: Int

    @get:Bindable(defaultValue = "PT1M")
    override val maxIdleTime: Duration

    @get:Bindable(defaultValue = "true")
    override val initSchema: Boolean
}
