package io.qalipsis.plugins.timescaledb

import com.github.dockerjava.api.command.InspectContainerResponse
import io.qalipsis.api.logging.LoggerHelper.logger
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

internal class TimescaleDbContainer<SELF : TimescaleDbContainer<SELF>>(
    dockerImageName: DockerImageName
) : PostgreSQLContainer<SELF>(dockerImageName) {

    override fun getJdbcUrl(): String {
        val additionalUrlParams = constructUrlParameters("?", "&")
        return ("jdbc:postgresql://" + host + ":" + getMappedPort(POSTGRESQL_PORT)
                + "/" + databaseName + additionalUrlParams)
    }

    override fun containerIsStarting(containerInfo: InspectContainerResponse?) {
        super.containerIsStarting(containerInfo)
        followOutput(Slf4jLogConsumer(log).withSeparateOutputStreams())
    }

    private companion object {
        val log = logger()
    }
}