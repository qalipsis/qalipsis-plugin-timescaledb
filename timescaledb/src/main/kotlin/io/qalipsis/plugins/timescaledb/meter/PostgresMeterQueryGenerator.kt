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

package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.query.QueryAggregationOperator
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.plugins.timescaledb.dataprovider.SerializableBoundParameter

internal class PostgresMeterQueryGenerator : AbstractMeterQueryGenerator() {

    override fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: Map<String, SerializableBoundParameter>
    ): StringBuilder {
        // We create a time-bucket series to aggregate into them: https://www.postgresql.org/docs/current/functions-srf.html
        val startIdentifier = boundParameters[":start"]!!.identifiers.first()
        val endIdentifier = boundParameters[":end"]!!.identifiers.first()
        val timeframeIdentifier = boundParameters[":timeframe"]!!.identifiers.first()
        val intervalSeries =
            "select CAST (start AS TIMESTAMP WITH TIME ZONE), start + interval '$timeframeIdentifier ms' - interval '1 ms' as end from generate_series($startIdentifier::timestamp, $endIdentifier::timestamp, interval '$timeframeIdentifier ms') AS start"
        val sql =
            StringBuilder("""with buckets as ($intervalSeries) SELECT buckets.start AS "bucket", """)

        val aggregation = when (query.aggregationOperation) {
            QueryAggregationOperator.COUNT -> "COUNT(*)"
            QueryAggregationOperator.AVERAGE -> "AVG(meters.${query.fieldName})"
            QueryAggregationOperator.MIN -> "MIN(meters.${query.fieldName})"
            QueryAggregationOperator.MAX -> "MAX(meters.${query.fieldName})"
            QueryAggregationOperator.SUM -> "SUM(meters.${query.fieldName})"
            QueryAggregationOperator.STANDARD_DEVIATION -> "STDDEV(meters.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_75 -> "percentile_disc(0.75) WITHIN GROUP (ORDER BY meters.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_99 -> "percentile_disc(0.99) WITHIN GROUP (ORDER BY meters.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_99_9 -> "percentile_disc(0.999) WITHIN GROUP (ORDER BY meters.${query.fieldName})"
        }
        sql.append("$aggregation AS result, meters.campaign AS campaign")
        sql.append(""" FROM meters RIGHT JOIN buckets ON meters.timestamp BETWEEN buckets.start AND buckets.end""")

        return sql
    }

    override fun appendGroupingForAggregation(sql: StringBuilder) {
        sql.append(" GROUP BY meters.campaign, buckets.start, buckets.end")
    }

    override fun appendOrderingForAggregation(sql: StringBuilder) {
        sql.append(" ORDER BY meters.campaign, buckets.start")
    }

}