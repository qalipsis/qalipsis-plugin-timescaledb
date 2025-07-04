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

package io.qalipsis.plugins.timescaledb.event

import io.qalipsis.api.query.QueryAggregationOperator
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.plugins.timescaledb.dataprovider.SerializableBoundParameter

internal class PostgresEventQueryGenerator : AbstractEventQueryGenerator() {

    override fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: Map<String, SerializableBoundParameter>
    ): StringBuilder {
        // We create a time-bucket series to aggregate into them: https://www.postgresql.org/docs/current/functions-srf.html
        val schemaIdentifier = boundParameters[":schema"]!!.identifiers.first()
        val startIdentifier = boundParameters[":start"]!!.identifiers.first()
        val endIdentifier = boundParameters[":end"]!!.identifiers.first()
        val timeframeIdentifier = boundParameters[":timeframe"]!!.identifiers.first()
        val intervalSeries =
            "select CAST (start AS TIMESTAMP WITH TIME ZONE), start + interval '$timeframeIdentifier ms' - interval '1 ms' as end from generate_series($startIdentifier::timestamp, $endIdentifier::timestamp, interval '$timeframeIdentifier ms') AS start"
        val sql =
            StringBuilder("""with buckets as ($intervalSeries) SELECT buckets.start AS "bucket", """)

        val aggregation = when (query.aggregationOperation) {
            QueryAggregationOperator.COUNT -> "COUNT(*)"
            QueryAggregationOperator.AVERAGE -> "AVG(events.${query.fieldName})"
            QueryAggregationOperator.MIN -> "MIN(events.${query.fieldName})"
            QueryAggregationOperator.MAX -> "MAX(events.${query.fieldName})"
            QueryAggregationOperator.SUM -> "SUM(events.${query.fieldName})"
            QueryAggregationOperator.STANDARD_DEVIATION -> "STDDEV(events.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_75 -> "percentile_disc(0.75) WITHIN GROUP (ORDER BY events.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_99 -> "percentile_disc(0.99) WITHIN GROUP (ORDER BY events.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_99_9 -> "percentile_disc(0.999) WITHIN GROUP (ORDER BY events.${query.fieldName})"
        }
        sql.append("$aggregation AS result, events.campaign AS campaign")
        sql.append(""" FROM ${schemaIdentifier}.events RIGHT JOIN buckets ON events.timestamp BETWEEN buckets.start AND buckets.end""")

        return sql
    }

    override fun appendGroupingForAggregation(sql: StringBuilder) {
        sql.append(" GROUP BY events.campaign, buckets.start, buckets.end")
    }

    override fun appendOrderingForAggregation(sql: StringBuilder) {
        sql.append(" ORDER BY events.campaign, buckets.start")
    }

}