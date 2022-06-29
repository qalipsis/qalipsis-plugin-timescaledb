package io.qalipsis.plugins.r2dbc

import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types


internal fun PreparedStatement.setStringOrNull(index: Int, value: String?) {
    if (value != null) {
        setString(index, value)
    } else {
        setNull(index, Types.VARCHAR)
    }
}

internal fun PreparedStatement.setTimestampOrNull(index: Int, value: Timestamp?) {
    if (value != null) {
        setTimestamp(index, value)
    } else {
        setNull(index, Types.TIMESTAMP_WITH_TIMEZONE)
    }
}

internal fun PreparedStatement.setBooleanOrNull(index: Int, value: Boolean?) {
    if (value != null) {
        setBoolean(index, value)
    } else {
        setNull(index, Types.BOOLEAN)
    }
}

internal fun PreparedStatement.setBigDecimalOrNull(index: Int, value: BigDecimal?) {
    if (value != null) {
        setBigDecimal(index, value)
    } else {
        setNull(index, Types.DOUBLE)
    }
}


internal fun PreparedStatement.setIntOrNull(index: Int, value: Int?) {
    if (value != null) {
        setInt(index, value)
    } else {
        setNull(index, Types.INTEGER)
    }
}
