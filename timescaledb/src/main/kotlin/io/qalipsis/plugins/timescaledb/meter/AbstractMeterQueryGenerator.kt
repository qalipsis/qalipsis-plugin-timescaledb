package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.report.DataField
import io.qalipsis.api.report.DataFieldType
import io.qalipsis.plugins.timescaledb.dataprovider.AbstractQueryGenerator
import io.qalipsis.plugins.timescaledb.dataprovider.DataType

internal abstract class AbstractMeterQueryGenerator : AbstractQueryGenerator(
    dataType = DataType.METER,
    databaseTable = "meters",
    queryFields = FIELDS,
    numericFields = FIELDS.filter { it.type == DataFieldType.NUMBER }.map { it.name }.toSet(),
    booleanFields = FIELDS.filter { it.type == DataFieldType.BOOLEAN }.map { it.name }.toSet(),
    jsonFields = emptySet()
) {

    companion object {
        /**
         * List of fields that can be used for aggregation and filters.
         */
        val FIELDS = listOf(
            DataField("count", DataFieldType.NUMBER),
            DataField("value", DataFieldType.NUMBER),
            DataField("sum", DataFieldType.NUMBER),
            DataField("mean", DataFieldType.NUMBER),
            DataField("active_tasks", DataFieldType.NUMBER),
            DataField("duration", DataFieldType.NUMBER),
            DataField("max", DataFieldType.NUMBER),
            DataField("other", DataFieldType.OBJECT)
        ).sortedBy { it.name }

    }

}