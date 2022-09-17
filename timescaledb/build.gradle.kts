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

plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
}

description = "QALIPSIS plugin for TimescaleDB"

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

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

val r2dbcVersion="0.8.13.RELEASE"
val coreVersion: String by project

dependencies {
    implementation(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    compileOnly("io.aeris-consulting:catadioptre-annotations")
    compileOnly("io.micronaut:micronaut-runtime")

    api("io.qalipsis:api-common")
    api("io.qalipsis:api-dsl")

    implementation("org.liquibase:liquibase-core")
    implementation("io.micronaut.sql:micronaut-jdbc-hikari")
    implementation("io.micronaut.micrometer:micronaut-micrometer-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("io.r2dbc:r2dbc-postgresql:${r2dbcVersion}")
    implementation("io.r2dbc:r2dbc-pool:${r2dbcVersion}")
    implementation("org.postgresql:postgresql")
    implementation("io.r2dbc:r2dbc-spi:${r2dbcVersion}")
    implementation(
        group = "io.netty",
        name = "netty-transport-native-epoll",
        classifier = "linux-x86_64"
    )
    implementation(
        group = "io.netty",
        name = "netty-transport-native-kqueue",
        classifier = "osx-x86_64"
    )

    kapt(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    kapt("io.qalipsis:api-processors")
    kapt("io.qalipsis:api-dsl")
    kapt("io.qalipsis:api-common")
    kapt("io.aeris-consulting:catadioptre-annotations")

    testImplementation("org.jetbrains.kotlin:kotlin-reflect")
    testImplementation("io.qalipsis:test")
    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testImplementation("io.aeris-consulting:catadioptre-kotlin")
    testImplementation(testFixtures("io.qalipsis:api-dsl"))
    testImplementation(testFixtures("io.qalipsis:api-common"))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:postgresql")
    testRuntimeOnly("io.qalipsis:runtime")
    testRuntimeOnly("io.qalipsis:head")
    testRuntimeOnly("io.qalipsis:factory")


    kaptTest(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:api-processors")
}


