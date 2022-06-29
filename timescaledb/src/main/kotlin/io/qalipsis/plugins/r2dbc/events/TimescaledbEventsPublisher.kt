package io.qalipsis.plugins.r2dbc.events

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.aerisconsulting.catadioptre.KTestable
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import io.qalipsis.plugins.r2dbc.liquibase.LiquibaseConfiguration
import io.qalipsis.plugins.r2dbc.liquibase.LiquibaseRunner
import io.qalipsis.plugins.r2dbc.setBigDecimalOrNull
import io.qalipsis.plugins.r2dbc.setBooleanOrNull
import io.qalipsis.plugins.r2dbc.setStringOrNull
import io.qalipsis.plugins.r2dbc.setTimestampOrNull
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
@Requires(beans = [TimescaledbEventsConfiguration::class])
internal class TimescaledbEventsPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) coroutineScope: CoroutineScope,
    private val configuration: TimescaledbEventsConfiguration,
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

    override fun start() {
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
        executors = java.util.concurrent.Executors.newWorkStealingPool(configuration.publishers + 1)

        super.start()
    }

    override suspend fun publish(values: List<Event>) {
        executors.submit {
            doPerformPublish(values)
        }
    }

    @KTestable
    private fun doPerformPublish(values: List<Event>) {
        log.debug { "Sending ${values.size} events to Timescaledb" }
        val timescaledbEvents = values.map { eventsConverter.convert(it) }

        try {
            datasource.connection.use { connection ->
                val results = connection.prepareStatement(sqlInsertStatement).use { statement ->
                    var bindIndex: Int
                    timescaledbEvents.forEach { event ->
                        bindIndex = 1
                        statement.setTimestamp(bindIndex++, event.timestamp)
                        statement.setString(bindIndex++, event.level)
                        statement.setString(bindIndex++, event.name)
                        statement.setStringOrNull(bindIndex++, event.tags)
                        statement.setStringOrNull(bindIndex++, event.message)
                        statement.setStringOrNull(bindIndex++, event.error)
                        statement.setStringOrNull(bindIndex++, event.stackTrace)
                        statement.setTimestampOrNull(bindIndex++, event.date)
                        statement.setBooleanOrNull(bindIndex++, event.boolean)
                        statement.setBigDecimalOrNull(bindIndex++, event.number)
                        statement.setBigDecimalOrNull(bindIndex++, event.duration)
                        statement.setStringOrNull(bindIndex++, event.geoPoint)
                        statement.setStringOrNull(bindIndex, event.value)
                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
                val updatedRows = results.count { it > 1 }
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
            "INSERT into %s (timestamp, level, name, tags, message, error, stack_trace, date, boolean, number, duration, geo_point, value) values (?, ?, ?, to_json(?::json), ?, ?, ?, ?, ?, ?, ?, to_json(?::json), to_json(?::json))"

        private val log = logger()
    }
}
