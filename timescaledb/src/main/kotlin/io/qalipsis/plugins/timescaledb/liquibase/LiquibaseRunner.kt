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