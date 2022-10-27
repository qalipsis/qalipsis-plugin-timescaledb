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

package io.qalipsis.plugins.timescaledb.dataprovider

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.validation.Validated
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.api.report.DataField
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.spi.Connection
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import reactor.core.publisher.Flux

@Validated
internal abstract class AbstractDataProvider(
    private val connectionPool: ConnectionPool,
    private val databaseTable: String,
    private val queryGenerator: AbstractQueryGenerator,
    private val objectMapper: ObjectMapper,
    excludedTags: Set<String> = emptySet()
) {

    private val excludedTagsArray = excludedTags.toTypedArray()

    suspend fun createQuery(tenant: String, query: QueryDescription): String {
        return objectMapper.writeValueAsString(queryGenerator.prepareQueries(tenant, query))
    }

    suspend fun listFields(tenant: String): Collection<DataField> {
        return queryGenerator.queryFields
    }

    suspend fun searchNames(tenant: String, filters: Collection<String>, size: Int): Collection<String> {
        val sql =
            StringBuilder("""SELECT DISTINCT "name" FROM $databaseTable WHERE "tenant" = $1 AND "campaign" IS NOT NULL""")
        if (filters.isNotEmpty()) {
            sql.append(""" AND "name" ILIKE any (array[$2])""")
        }
        sql.append(""" ORDER BY "name" LIMIT $size""")

        return Flux.usingWhen(
            connectionPool.create(),
            { connection ->
                Flux.from(connection.createStatement(sql.toString()).bind("$1", tenant).also {
                    if (filters.isNotEmpty()) {
                        it.bind("$2", filters.map(this::convertWildcards).toTypedArray())
                    }
                }.execute()).flatMap { result ->
                    result.map { row, _ -> row.get("name", String::class.java) }
                }
            },
            Connection::close
        ).asFlow().toList(mutableListOf<String>())
    }

    suspend fun searchTagsAndValues(
        tenant: String,
        filters: Collection<String>,
        size: Int
    ): Map<String, Collection<String>> {
        val sql =
            StringBuilder(
                """SELECT tags.key AS KEY, STRING_AGG(DISTINCT(tags.value), ',' ORDER BY tags.value) AS value FROM $databaseTable AS e, lateral jsonb_each_text(tags) AS tags 
                |WHERE "tenant" = $1 AND "campaign" IS NOT NULL AND tags.value <> ''""".trimMargin()
            )
        sql.append(""" AND tags.key <> all (array[$2])""")
        if (filters.isNotEmpty()) {
            sql.append(""" AND (tags.key ILIKE any (array[$3]) OR tags.value ILIKE any (array[$3]))""")
        }
        sql.append(""" GROUP BY tags.key ORDER BY tags.key LIMIT $size""")

        return Flux.usingWhen(
            connectionPool.create(),
            { connection ->
                Flux.from(
                    connection.createStatement(sql.toString())
                        .bind("$1", tenant).bind("$2", excludedTagsArray)
                        .also {
                            if (filters.isNotEmpty()) {
                                it.bind("$3", filters.map(this::convertWildcards).toTypedArray())
                            }
                        }.execute()
                ).flatMap { result ->
                    result.map { row, _ ->
                        row.get("key", String::class.java)!! to (row.get("value", String::class.java)?.split(',')
                            .orEmpty())
                    }
                }
            },
            Connection::close
        ).asFlow().toList(mutableListOf<Pair<String, List<String>>>()).toMap()
    }

    private fun convertWildcards(clause: String) = clause.replace('*', '%').replace('?', '_')

}