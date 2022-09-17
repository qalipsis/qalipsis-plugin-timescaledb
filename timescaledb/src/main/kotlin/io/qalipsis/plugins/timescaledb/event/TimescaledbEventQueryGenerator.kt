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
        sql.append(""" FROM events""")
        return sql
    }

    override fun appendGroupingForAggregation(sql: StringBuilder) {
        sql.append(" GROUP BY events.campaign, bucket")
    }

    override fun appendOrderingForAggregation(sql: StringBuilder) {
        sql.append(" ORDER BY events.campaign, bucket")
    }

}