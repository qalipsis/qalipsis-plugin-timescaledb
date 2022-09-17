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

package io.qalipsis.plugins.timescaledb.event

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.aerisconsulting.catadioptre.KTestable
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.dataprovider.setBigDecimalOrNull
import io.qalipsis.plugins.timescaledb.dataprovider.setBooleanOrNull
import io.qalipsis.plugins.timescaledb.dataprovider.setStringOrNull
import io.qalipsis.plugins.timescaledb.dataprovider.setTimestampOrNull
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseConfiguration
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseRunner
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import org.postgresql.Driver
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit


/**
 * Implementation of [io.qalipsis.api.events.EventsLogger] for TimescaleDB.
 *
 * @author Gabriel Moraes
 */
@Singleton
@Requires(beans = [TimescaledbEventsPublisherConfiguration::class])
internal class TimescaledbEventsPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) coroutineScope: CoroutineScope,
    private val configuration: TimescaledbEventsPublisherConfiguration,
    private val eventsConverter: TimescaledbEventConverter
) : AbstractBufferedEventsPublisher(
    configuration.minLevel,
    configuration.lingerPeriod,
    configuration.batchSize,
    coroutineScope
) {

    private lateinit var datasource: HikariDataSource

    private lateinit var sqlInsertStatement: String

    private lateinit var executors: ExecutorService

    private var schemaCreated = false

    override fun start() {
        createOrUpdateSchemaIfRequired()

        val poolConfig = HikariConfig()
        poolConfig.isAutoCommit = true
        poolConfig.schema = configuration.schema
        poolConfig.username = configuration.username
        poolConfig.password = configuration.password
        poolConfig.driverClassName = Driver::class.java.canonicalName
        // See https://jdbc.postgresql.org/documentation/80/connect.html
        poolConfig.jdbcUrl = "jdbc:postgresql://${configuration.host}:${configuration.port}/${configuration.database}"

        poolConfig.minimumIdle = configuration.publishers
        poolConfig.maximumPoolSize = configuration.publishers

        datasource = HikariDataSource(poolConfig)
        sqlInsertStatement = String.format(SQL, "${configuration.schema}.events")

        // We allow a unique executor to wait at the pool gate for the next connection to be released, therefore the "+1".
        executors = java.util.concurrent.Executors.newWorkStealingPool(configuration.publishers + 1)

        super.start()
    }

    /**
     * Creates or updates the DB schema when not yet done.
     */
    private fun createOrUpdateSchemaIfRequired() {
        if (!schemaCreated) {
            LiquibaseRunner(
                LiquibaseConfiguration(
                    changeLog = "db/liquibase-events-changelog.xml",
                    host = configuration.host,
                    port = configuration.port,
                    username = configuration.username,
                    password = configuration.password,
                    database = configuration.database,
                    defaultSchemaName = configuration.schema,
                )
            ).run()
            schemaCreated = true
        }
    }

    override suspend fun publish(values: List<Event>) {
        executors.submit {
            doPerformPublish(values)
        }
    }

    @KTestable
    private fun doPerformPublish(values: List<Event>) {
        log.debug { "Sending ${values.size} events to Timescaledb" }
        publishConvertedEvents(values.map { eventsConverter.convert(it) })
    }

    @KTestable
    private fun publishConvertedEvents(timescaledbEvents: List<TimescaledbEvent>) {
        try {
            log.debug { "${timescaledbEvents.size} events to be published" }
            log.trace { "Saved events: ${timescaledbEvents.joinToString("\n\t", prefix = "\n\t") { it.toString() }}" }
            datasource.connection.use { connection ->
                connection.beginRequest()
                val results = connection.prepareStatement(sqlInsertStatement).use { statement ->
                    var bindIndex: Int
                    timescaledbEvents.forEach { event ->
                        bindIndex = 1
                        statement.setTimestamp(bindIndex++, event.timestamp)
                        statement.setString(bindIndex++, event.level)
                        statement.setString(bindIndex++, event.name)
                        statement.setStringOrNull(bindIndex++, event.tenant)
                        statement.setStringOrNull(bindIndex++, event.campaign)
                        statement.setStringOrNull(bindIndex++, event.scenario)
                        statement.setStringOrNull(bindIndex++, event.tags)
                        statement.setStringOrNull(bindIndex++, event.message)
                        statement.setStringOrNull(bindIndex++, event.error)
                        statement.setStringOrNull(bindIndex++, event.stackTrace)
                        statement.setTimestampOrNull(bindIndex++, event.date)
                        statement.setBooleanOrNull(bindIndex++, event.boolean)
                        statement.setBigDecimalOrNull(bindIndex++, event.number)
                        statement.setBigDecimalOrNull(bindIndex++, event.durationNano)
                        statement.setStringOrNull(bindIndex++, event.geoPoint)
                        statement.setStringOrNull(bindIndex, event.value)
                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
                connection.endRequest()
                val updatedRows = results.count { it >= 1 }
                log.debug { "$updatedRows events were successfully published" }
            }
        } catch (e: Exception) {
            log.error(e) { e.message }
        }
    }

    override fun stop() {
        log.debug { "Stopping the events logger with ${buffer.size} events in the buffer" }
        super.stop()
        tryAndLogOrNull(log) {
            executors.shutdown()
            executors.awaitTermination(30, TimeUnit.SECONDS)
        }
        datasource.close()
        log.debug { "The events logger was stopped" }
    }

    companion object {

        const val SQL =
            "INSERT into %s (timestamp, level, name, tenant, campaign, scenario, tags, message, error, stack_trace, date, boolean, number, duration_nano, geo_point, value) values (?, ?, ?, ?, ?, ?, to_json(?::json), ?, ?, ?, ?, ?, ?, ?, to_json(?::json), to_json(?::json))"

        private val log = logger()
    }
}
