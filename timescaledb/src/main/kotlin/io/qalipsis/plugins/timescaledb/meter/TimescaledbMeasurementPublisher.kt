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

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.plugins.timescaledb.dataprovider.setBigDecimalOrNull
import io.qalipsis.plugins.timescaledb.dataprovider.setStringOrNull
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseConfiguration
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseRunner
import org.postgresql.Driver

internal class TimescaledbMeasurementPublisher(
    private val config: TimescaledbMeterConfig,
    private val converter: TimescaledbMeterConverter
) : MeasurementPublisher {

    private lateinit var datasource: HikariDataSource

    private val sqlInsertStatement = String.format(SQL, "${config.schema}.meters")

    private var schemaInitialized = !config.initSchema

    init {
        if (config.autostart || config.autoconnect) {
            createOrUpdateSchemaIfRequired()

            val poolConfig = HikariConfig()
            poolConfig.isAutoCommit = true
            poolConfig.schema = config.schema
            poolConfig.username = config.username
            poolConfig.password = config.password
            poolConfig.driverClassName = Driver::class.java.canonicalName
            // See https://jdbc.postgresql.org/documentation/80/connect.html
            poolConfig.jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
            if (config.enableSsl) {
                poolConfig.dataSourceProperties["ssl"] = "true"
                poolConfig.dataSourceProperties["sslmode"] = config.sslMode.name
                config.sslRootCert?.let { poolConfig.dataSourceProperties["sslrootcert"] = it }
                config.sslCert?.let { poolConfig.dataSourceProperties["sslcert"] = it }
                config.sslKey?.let { poolConfig.dataSourceProperties["sslkey"] = it }
            }

            poolConfig.minimumIdle = config.minIdleConnection
            poolConfig.maximumPoolSize = config.maxPoolSize

            datasource = HikariDataSource(poolConfig)
        }

    }

    private fun createOrUpdateSchemaIfRequired() {
        if (!schemaInitialized) {
            LiquibaseRunner(
                LiquibaseConfiguration(
                    changeLog = "db/liquibase-meters-changelog.xml",
                    host = config.host,
                    port = config.port,
                    username = config.username,
                    password = config.password,
                    database = config.database,
                    defaultSchemaName = config.schema,
                    enableSsl = config.enableSsl,
                    sslMode = config.sslMode,
                    sslRootCert = config.sslRootCert,
                    sslKey = config.sslKey,
                    sslCert = config.sslCert
                )
            ).run()
            schemaInitialized = true
        }
    }

    override suspend fun stop() {
        super.stop()
        datasource.close()
        log.debug { "The meter registry publisher was stopped" }
    }

    override suspend fun publish(meters: Collection<MeterSnapshot>) {
        doPublish(converter.convert(meters))
    }

    @KTestable
    private fun doPublish(timescaledbMeters: List<TimescaledbMeter>) {
        if (log.isTraceEnabled) {
            log.trace {
                "Meters to publish: ${
                    timescaledbMeters.joinToString(
                        prefix = "\n\t",
                        separator = "\n\t"
                    ) { it.toString() }
                }"
            }
        } else {
            log.debug { "${timescaledbMeters.size} meters are to be published" }
        }
        tryAndLogOrNull(log) {
            val metersNames = mutableListOf<String>()
            datasource.connection.use { connection ->
                val results = connection.prepareStatement(sqlInsertStatement).use { statement ->
                    var bindIndex: Int
                    timescaledbMeters.forEach { meters ->
                        metersNames += meters.name
                        bindIndex = 1
                        statement.setString(bindIndex++, meters.name)
                        statement.setStringOrNull(bindIndex++, meters.tags)
                        statement.setTimestamp(bindIndex++, meters.timestamp)
                        statement.setStringOrNull(bindIndex++, meters.tenant)
                        statement.setStringOrNull(bindIndex++, meters.campaign)
                        statement.setStringOrNull(bindIndex++, meters.scenario)
                        statement.setString(bindIndex++, meters.type)
                        statement.setBigDecimalOrNull(bindIndex++, meters.count)
                        statement.setBigDecimalOrNull(bindIndex++, meters.value)
                        statement.setBigDecimalOrNull(bindIndex++, meters.sum)
                        statement.setBigDecimalOrNull(bindIndex++, meters.mean)
                        statement.setStringOrNull(bindIndex++, meters.unit)
                        statement.setBigDecimalOrNull(bindIndex++, meters.max)
                        statement.setStringOrNull(bindIndex++, meters.other)

                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
                if (log.isTraceEnabled) {
                    val updatedByMeterName =
                        metersNames.mapIndexed { index, meterName -> "${meterName}->${results[index]}" }.joinToString()
                    log.trace { "Result of the saving: $updatedByMeterName" }
                } else {
                    log.debug {
                        val updatedRows = results.count { it >= 1 }
                        "$updatedRows meters were successfully saved"
                    }
                }
            }
        }
    }

    private companion object {
        const val SQL =
            "INSERT into %s (name, tags, timestamp, tenant, campaign, scenario, type, count, value, sum, mean, unit, max, other) values (?, to_json(?::json), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, to_json(?::json))"

        val log = logger()
    }
}