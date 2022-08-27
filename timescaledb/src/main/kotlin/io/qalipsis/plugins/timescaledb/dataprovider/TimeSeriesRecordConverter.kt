package io.qalipsis.plugins.timescaledb.dataprovider

import io.qalipsis.api.report.TimeSeriesRecord
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata

/**
 * Converter from SQL result of time-series records to [TimeSeriesRecord].
 *
 * @author Eric Jessé
 */
interface TimeSeriesRecordConverter {

    fun convert(row: Row, metadata: RowMetadata): TimeSeriesRecord

}