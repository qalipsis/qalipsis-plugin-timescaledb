package io.qalipsis.plugins.timescaledb.liquibase

import io.micronaut.core.util.StringUtils
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.postgresql.Driver
import java.sql.Connection
import java.sql.DriverManager

/**
 * Class in charge to apply the schema changes to the database.
 *
 * @author Eric Jessé
 */
internal class LiquibaseRunner(
    private val configuration: LiquibaseConfiguration,
) {

    fun run() {
        val changeLog = configuration.changeLog
        DriverManager.getConnection(
            "jdbc:postgresql://${configuration.host}:${configuration.port}/${configuration.database}",
            configuration.username,
            configuration.password
        ).use { connection ->
            val liquibase = Liquibase(changeLog, ClassLoaderResourceAccessor(), createDatabase(connection))
            liquibase.update(Contexts(), LabelExpression())
        }
    }

    private fun createDatabase(connection: Connection): Database {
        val liquibaseConnection = JdbcConnection(connection)
        val database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(liquibaseConnection)
        val defaultSchema = configuration.defaultSchemaName
        if (StringUtils.isNotEmpty(defaultSchema)) {
            if (database.supportsSchemas()) {
                database.defaultSchemaName = defaultSchema
            } else if (database.supportsCatalogs()) {
                database.defaultCatalogName = defaultSchema
            }
        }
        val liquibaseSchema = configuration.liquibaseSchemaName
        if (StringUtils.isNotEmpty(liquibaseSchema)) {
            if (database.supportsSchemas()) {
                database.liquibaseSchemaName = liquibaseSchema
            } else if (database.supportsCatalogs()) {
                database.liquibaseCatalogName = liquibaseSchema
            }
        }

        return database
    }

    companion object {

        init {
            kotlin.runCatching {
                Driver.register()
            }
        }
    }

}