package io.qalipsis.plugins.r2dbc.liquibase

internal data class LiquibaseConfiguration(
    val changeLog: String,
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val database: String,
    val defaultSchemaName: String,
    val liquibaseSchemaName: String = defaultSchemaName
)