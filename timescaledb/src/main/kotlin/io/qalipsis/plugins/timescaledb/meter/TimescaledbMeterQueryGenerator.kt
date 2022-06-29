package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.report.query.QueryAggregationOperator
import io.qalipsis.api.report.query.QueryDescription
import io.qalipsis.plugins.timescaledb.dataprovider.BoundParameters

internal class TimescaledbMeterQueryGenerator : AbstractMeterQueryGenerator() {

    override fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: MutableMap<String, BoundParameters>
    ): StringBuilder {
        // We create a time-bucket series to aggregate into them: https://www.postgresql.org/docs/current/functions-srf.html
        // The interval value cannot be bound and must be replaced as a string.
        val timeframeIdentifier = boundParameters[":timeframe"]!!.identifiers.first()
        val sql =
            StringBuilder("""SELECT public.time_bucket(interval '$timeframeIdentifier ms', meters.timestamp) as bucket,""")
        val aggregation = when (query.aggregationOperation) {
            QueryAggregationOperator.COUNT -> " COUNT(*) "
            QueryAggregationOperator.AVERAGE -> """ AVG(meters.${query.fieldName}) """
            QueryAggregationOperator.MIN -> """ MIN(meters.${query.fieldName}) """
            QueryAggregationOperator.MAX -> """ MAX(meters.${query.fieldName}) """
            QueryAggregationOperator.SUM -> """ SUM(meters.${query.fieldName}) """
            QueryAggregationOperator.STANDARD_DEVIATION -> """ STDDEV(meters.${query.fieldName}) """
            // uddsketch hyperfunction is better dedicated to median percentiles (close to 50%).
            QueryAggregationOperator.PERCENTILE_75 -> """ public.approx_percentile(0.75, public.uddsketch(100, 0.01, meters.${query.fieldName})) """
            // tdigest hyperfunction is better dedicated to extreme percentiles (close to 100%).
            QueryAggregationOperator.PERCENTILE_99 -> """ public.approx_percentile(0.99, public.tdigest(100, meters.${query.fieldName})) """
            QueryAggregationOperator.PERCENTILE_99_9 -> """ public.approx_percentile(0.999, public.tdigest(100, meters.${query.fieldName})) """
        }
        sql.append(" $aggregation AS result")
        sql.append(""" FROM meters""")
        return sql
    }

    override fun appendGroupingForAggregation(sql: StringBuilder) {
        sql.append(" GROUP BY meters.campaign, bucket")
    }

    override fun appendOrderingForAggregation(sql: StringBuilder) {
        sql.append(" ORDER BY meters.campaign, bucket")
    }

}