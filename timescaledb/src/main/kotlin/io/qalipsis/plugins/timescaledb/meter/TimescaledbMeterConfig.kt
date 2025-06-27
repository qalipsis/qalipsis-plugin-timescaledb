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

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import io.micronaut.core.util.StringUtils
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeterConfig.Companion.TIMESCALEDB_CONFIGURATION
import io.r2dbc.postgresql.client.SSLMode
import javax.validation.constraints.NotBlank
import javax.validation.constraints.Positive

/**
 *
 * Measurement configuration properties for Timescaledb.
 *
 * @author Francisca Eze
 */
@Introspected
@Requires(property = "$TIMESCALEDB_CONFIGURATION.enabled", value = StringUtils.TRUE)
@ConfigurationProperties(TIMESCALEDB_CONFIGURATION)
internal class TimescaledbMeterConfig {

    @get:NotBlank
    var prefix: String = "qalipsis"

    @get:NotBlank
    var host: String = "localhost"

    @get:Positive
    var port: Int = 5432

    @get:NotBlank
    var database: String = "qalipsis"

    /**
     * Name of the user for basic authentication when connecting to the database.
     */
    @get:NotBlank
    var username: String = "qalipsis_user"

    /**
     * Password of the user for basic authentication when connecting to the database.
     */
    @get:NotBlank
    var password: String = "qalipsis-pwd"

    @get:NotBlank
    var schema: String = "meters"

    /**
     * Specify whether Secure Sockets Layer (SSL) encryption should be enabled for communication.
     */
    var enableSsl: Boolean = false

    /**
     * Defines the mode of SSL (Secure Sockets Layer) usage for secure communication between a client and a server.
     */
    var sslMode: SSLMode = SSLMode.PREFER

    /**
     * Defines the root certificate used in SSL/TLS encryption for secure communication between a client and a server.
     */
    var sslRootCert: String? = null

    /**
     * Specifies the private key used in SSL encryption for secure communication between a client and a server.
     */
    var sslKey: String? = null

    /**
     * Specifies the SSL certificate used to establish a secure communication channel between a client and a server.
     */
    var sslCert: String? = null

    /**
     * The minimum number of idle connections in the pool to maintain.
     */
    @get:Positive
    var minIdleConnection: Int = 1

    /**
     * The maximum number of connections in the pool.
     */
    @get:Positive
    var maxPoolSize: Int = 1

    /**
     * For test purpose only.
     */
    var autostart: Boolean = true

    /**
     * For test purpose only.
     */
    var autoconnect: Boolean = true

    /**
     * Specifies whether the schema for the meters should be created or updated at startup.
     */
    var initSchema: Boolean = true

    /**
     * The name of the timestamp field. Default is: "timestamp"
     *
     * @return field name for timestamp
     */
    val timestampFieldName: String = "timestamp"

    companion object {
        const val TIMESCALEDB_CONFIGURATION = "meters.export.timescaledb"

        const val TIMESCALEDB_ENABLED = "${TIMESCALEDB_CONFIGURATION}.enabled"
    }
}
