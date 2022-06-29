package io.qalipsis.plugins.timescaledb.dataprovider

import java.time.Duration

/**
 * Configuration for [AbstractDataProvider].
 *
 * @author Eric Jessé
 */

interface DataProviderConfiguration {

    val host: String

    val port: Int

    val database: String

    val schema: String

    val username: String

    val password: String

    val minSize: Int

    val maxSize: Int

    val maxIdleTime: Duration
}
