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

internal class TimescaledbEventQueryGenerator : AbstractEventQueryGenerator() {

    override fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: Map<String, SerializableBoundParameter>
    ): StringBuilder {
        // We create a time-bucket series to aggregate into them: https://www.postgresql.org/docs/current/functions-srf.html
        // The interval value cannot be bound and must be replaced as a string.
        val schemaIdentifier = boundParameters[":schema"]!!.identifiers.first()
        val timeframeIdentifier = boundParameters[":timeframe"]!!.identifiers.first()
        val sql =
            StringBuilder("""SELECT public.time_bucket(interval '$timeframeIdentifier ms', events.timestamp) as bucket,""")
        val aggregation = when (query.aggregationOperation) {
            QueryAggregationOperator.COUNT -> " COUNT(*) "
            QueryAggregationOperator.AVERAGE -> """ AVG(events.${query.fieldName}) """
            QueryAggregationOperator.MIN -> """ MIN(events.${query.fieldName}) """
            QueryAggregationOperator.MAX -> """ MAX(events.${query.fieldName}) """
            QueryAggregationOperator.SUM -> """ SUM(events.${query.fieldName}) """
            QueryAggregationOperator.STANDARD_DEVIATION -> """ STDDEV(events.${query.fieldName}) """
            // uddsketch hyperfunction is better dedicated to median percentiles (close to 50%).
            QueryAggregationOperator.PERCENTILE_75 -> """ public.approx_percentile(0.75, public.uddsketch(100, 0.01, events.${query.fieldName})) """
            // tdigest hyperfunction is better dedicated to extreme percentiles (close to 100%).
            QueryAggregationOperator.PERCENTILE_99 -> """ public.approx_percentile(0.99, public.tdigest(100, events.${query.fieldName})) """
            QueryAggregationOperator.PERCENTILE_99_9 -> """ public.approx_percentile(0.999, public.tdigest(100, events.${query.fieldName})) """
        }
        sql.append(" $aggregation AS result, events.campaign AS campaign")
        sql.append(""" FROM ${schemaIdentifier}.events""")
        return sql
    }

    override fun appendGroupingForAggregation(sql: StringBuilder) {
        sql.append(" GROUP BY events.campaign, bucket")
    }

    override fun appendOrderingForAggregation(sql: StringBuilder) {
        sql.append(" ORDER BY events.campaign, bucket")
    }

}