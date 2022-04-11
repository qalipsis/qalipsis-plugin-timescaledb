plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
}

description = "Qalipsis Plugins - R2DBC with native Drivers"

allOpen {
    annotations(
        "io.micronaut.aop.Around",
        "jakarta.inject.Singleton",
        "io.qalipsis.api.annotations.StepConverter",
        "io.qalipsis.api.annotations.StepDecorator",
        "io.qalipsis.api.annotations.PluginComponent",
        "io.qalipsis.api.annotations.Spec",
        "io.micronaut.validation.Validated"
    )
}

val micronautVersion: String by project
val kotlinCoroutinesVersion: String by project
val testContainersVersion: String by project
val r2dbcVersion: String by project

val postgresqlDriverVersion = "42.3.1"
val nettyVersion = "4.1.74.Final"

val catadioptreVersion: String by project

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

dependencies {
    compileOnly("io.aeris-consulting:catadioptre-annotations:${catadioptreVersion}")
    compileOnly(kotlin("stdlib"))
    compileOnly("io.micronaut:micronaut-runtime")
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")

    api("io.qalipsis:api-common:${project.version}")
    api("io.qalipsis:api-dsl:${project.version}")


    implementation(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    implementation("io.micronaut.liquibase:micronaut-liquibase")
    implementation("io.micronaut.sql:micronaut-jdbc-hikari")
    implementation("io.micronaut.micrometer:micronaut-micrometer-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:$kotlinCoroutinesVersion")
    implementation("io.r2dbc:r2dbc-postgresql:${r2dbcVersion}")
    implementation("io.r2dbc:r2dbc-pool:${r2dbcVersion}")
    implementation("org.postgresql:postgresql:$postgresqlDriverVersion")
    implementation("io.r2dbc:r2dbc-spi:${r2dbcVersion}")
    implementation(
        group = "io.netty",
        name = "netty-transport-native-epoll",
        version = nettyVersion,
        classifier = "linux-x86_64"
    )
    implementation(
        group = "io.netty",
        name = "netty-transport-native-kqueue",
        version = nettyVersion,
        classifier = "osx-x86_64"
    )

    kapt(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    kapt("io.qalipsis:api-processors:${project.version}")
    kapt("io.qalipsis:api-dsl:${project.version}")
    kapt("io.qalipsis:api-common:${project.version}")
    kapt("io.aeris-consulting:catadioptre-annotations:${catadioptreVersion}")

    testImplementation("io.qalipsis:test:${project.version}")
    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testImplementation("io.aeris-consulting:catadioptre-kotlin:${catadioptreVersion}")
    testImplementation(testFixtures("io.qalipsis:api-dsl:${project.version}"))
    testImplementation(testFixtures("io.qalipsis:api-common:${project.version}"))
    testImplementation("org.testcontainers:testcontainers:${testContainersVersion}")
    testImplementation("org.testcontainers:postgresql:${testContainersVersion}")

    kaptTest(platform("io.micronaut:micronaut-bom:${micronautVersion}"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:api-processors:${project.version}")
}


