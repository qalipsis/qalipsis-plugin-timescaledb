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
    private val databaseSchema: String,
    private val databaseTable: String,
    private val queryGenerator: AbstractQueryGenerator,
    private val objectMapper: ObjectMapper,
    excludedTags: Set<String> = emptySet()
) {

    private val excludedTagsArray = excludedTags.toTypedArray()

    suspend fun createQuery(tenant: String?, query: QueryDescription): String {
        return objectMapper.writeValueAsString(queryGenerator.prepareQueries(tenant, query))
    }

    suspend fun listFields(tenant: String, name: String?): Collection<DataField> {
        return queryGenerator.queryFields
    }

    suspend fun searchNames(tenant: String, filters: Collection<String>, size: Int): Collection<String> {
        val sql =
            StringBuilder("""SELECT DISTINCT "name" FROM ${databaseSchema}.${databaseTable} WHERE "tenant" = $1 AND "campaign" IS NOT NULL""")
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
        name: String?,
        filters: Collection<String>,
        size: Int
    ): Map<String, Collection<String>> {
        val params = mutableMapOf<String, Any>()
        params["$1"] = tenant
        params["$2"] = excludedTagsArray
        val sql =
            StringBuilder(
                """SELECT tags.key AS KEY, STRING_AGG(DISTINCT(tags.value), ',' ORDER BY tags.value) AS value FROM ${databaseSchema}.${databaseTable} AS e, lateral jsonb_each_text(tags) AS tags 
                |WHERE "tenant" = $1 AND "campaign" IS NOT NULL AND tags.value <> ''""".trimMargin()
            )
        sql.append(""" AND tags.key <> all (array[$2])""")

        var nextBindingIndex = 3
        if (!name.isNullOrBlank()) {
            val binding = "\$${nextBindingIndex++}"
            params[binding] = name.trim()
            sql.append(""" AND name = $binding""")
        }
        if (filters.isNotEmpty()) {
            val binding = "\$${nextBindingIndex++}"
            params[binding] = filters.map(this::convertWildcards).toTypedArray()
            sql.append(""" AND (tags.key ILIKE any (array[$binding]) OR tags.value ILIKE any (array[$binding]))""")
        }
        sql.append(""" GROUP BY tags.key ORDER BY tags.key LIMIT $size""")

        return Flux.usingWhen(
            connectionPool.create(),
            { connection ->
                Flux.from(
                    connection.createStatement(sql.toString())
                        .also {
                            params.forEach { (binding, value) ->
                                it.bind(binding, value)
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