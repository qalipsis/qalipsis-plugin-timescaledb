package io.qalipsis.plugins.timescaledb.event

import io.qalipsis.api.report.query.QueryAggregationOperator
import io.qalipsis.api.report.query.QueryDescription
import io.qalipsis.plugins.timescaledb.dataprovider.BoundParameters

internal class PostgresEventQueryGenerator : AbstractEventQueryGenerator() {

    override fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: MutableMap<String, BoundParameters>
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
            QueryAggregationOperator.AVERAGE -> "AVG(events.${query.fieldName})"
            QueryAggregationOperator.MIN -> "MIN(events.${query.fieldName})"
            QueryAggregationOperator.MAX -> "MAX(events.${query.fieldName})"
            QueryAggregationOperator.SUM -> "SUM(events.${query.fieldName})"
            QueryAggregationOperator.STANDARD_DEVIATION -> "STDDEV(events.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_75 -> "percentile_disc(0.75) WITHIN GROUP (ORDER BY events.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_99 -> "percentile_disc(0.99) WITHIN GROUP (ORDER BY events.${query.fieldName})"
            QueryAggregationOperator.PERCENTILE_99_9 -> "percentile_disc(0.999) WITHIN GROUP (ORDER BY events.${query.fieldName})"
        }
        sql.append("$aggregation AS result")
        sql.append(""" FROM events RIGHT JOIN buckets ON events.timestamp BETWEEN buckets.start AND buckets.end""")

        return sql
    }

    override fun appendGroupingForAggregation(sql: StringBuilder) {
        sql.append(" GROUP BY events.campaign, buckets.start, buckets.end")
    }

    override fun appendOrderingForAggregation(sql: StringBuilder) {
        sql.append(" ORDER BY events.campaign, buckets.start")
    }

}