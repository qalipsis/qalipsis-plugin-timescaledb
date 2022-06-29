package io.qalipsis.plugins.timescaledb

import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.JdbcDatabaseContainerProvider
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.jdbc.ConnectionUrl
import org.testcontainers.utility.DockerImageName

internal class TimescaleDbContainerProvider : JdbcDatabaseContainerProvider() {

    override fun supports(databaseType: String): Boolean {
        return databaseType == "timescaledb"
    }

    override fun newInstance(): JdbcDatabaseContainer<*> {
        return newInstance(DEFAULT_TAG)
    }

    override fun newInstance(tag: String): JdbcDatabaseContainer<*> {
        val dockerImageName = DockerImageName.parse(IMAGE).withTag(tag)
            .asCompatibleSubstituteFor(PostgreSQLContainer.IMAGE)
        return TimescaleDbContainer<TimescaleDbContainer<*>>(dockerImageName)
    }

    fun newInstance(imageName: DockerImageName): JdbcDatabaseContainer<*> {
        return TimescaleDbContainer<TimescaleDbContainer<*>>(imageName.asCompatibleSubstituteFor(PostgreSQLContainer.IMAGE))
    }

    override fun newInstance(connectionUrl: ConnectionUrl?): JdbcDatabaseContainer<*> {
        return newInstanceFromConnectionUrl(connectionUrl, USER_PARAM, PASSWORD_PARAM)
    }

    companion object {

        val USER_PARAM = "user"

        val PASSWORD_PARAM = "password"

        val IMAGE = "timescale/timescaledb"

        val DEFAULT_TAG = "latest-pg14"

    }
}
