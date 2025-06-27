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
import java.util.Properties

/**
 * Class in charge to apply the schema changes to the database.
 *
 * @author Eric Jessé
 */
internal class LiquibaseRunner(
    private val configuration: LiquibaseConfiguration,
) {

    fun run() {
        // List of supported parameters: https://jdbc.postgresql.org/documentation/use/#connection-parameters
        val changeLog = configuration.changeLog
        val properties = Properties()
        properties["ApplicationName"] = "qalipsis-timescaledb-liquibase"
        properties["user"] = configuration.username
        properties["password"] = configuration.password
        if (configuration.enableSsl) {
            properties["ssl"] = "true"
            properties["sslmode"] = configuration.sslMode.name
            configuration.sslRootCert?.let { properties["sslrootcert"] = it }
            configuration.sslCert?.let { properties["sslcert"] = it }
            configuration.sslKey?.let { properties["sslkey"] = it }
        }

        DriverManager
            .getConnection(
                "jdbc:postgresql://${configuration.host}:${configuration.port}/${configuration.database}",
                properties
            )
            .use { connection ->
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