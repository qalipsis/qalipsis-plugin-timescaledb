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

import org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED
import org.gradle.api.tasks.testing.logging.TestLogEvent.STANDARD_ERROR

plugins {
    idea
    java
    kotlin("jvm") version "1.6.21"
    kotlin("kapt") version "1.6.21"
    kotlin("plugin.allopen") version "1.6.21"

    id("nebula.contacts") version "5.1.0"
    id("nebula.info") version "9.1.1"
    id("nebula.maven-publish") version "17.3.3"
    id("nebula.maven-scm") version "17.3.3"
    id("nebula.maven-manifest") version "17.3.3"
    id("nebula.maven-apache-license") version "17.3.3"
    id ("com.github.jk1.dependency-license-report") version "1.17"
    signing
}

licenseReport {
    renderers = arrayOf<com.github.jk1.license.render.ReportRenderer>(
        com.github.jk1.license.render.InventoryHtmlReportRenderer(
            "report.html",
            "QALIPSIS plugin for TimescaleDB"
        )
    )
    allowedLicensesFile = File("$projectDir/build-config/allowed-licenses.json")
    filters = arrayOf<com.github.jk1.license.filter.DependencyFilter>(com.github.jk1.license.filter.LicenseBundleNormalizer())
}

/**
 * Target version of the generated JVM bytecode.
 */
val target = JavaVersion.VERSION_11

configure<JavaPluginConvention> {
    description = "QALIPSIS plugin for TimescaleDB"

    sourceCompatibility = target
    targetCompatibility = target
}

tasks.withType<Wrapper> {
    distributionType = Wrapper.DistributionType.BIN
    gradleVersion = "6.8.1"
}

val testNumCpuCore: String? by project

allprojects {
    group = "io.qalipsis.plugin"
    version = File(rootDir, "project.version").readText().trim()

    apply(plugin = "java")
    apply(plugin = "nebula.contacts")
    apply(plugin = "nebula.info")
    apply(plugin = "nebula.maven-publish")
    apply(plugin = "nebula.maven-scm")
    apply(plugin = "nebula.maven-manifest")
    apply(plugin = "nebula.maven-developer")
    apply(plugin = "nebula.maven-apache-license")
    apply(plugin = "nebula.javadoc-jar")
    apply(plugin = "nebula.source-jar")
    apply(plugin = "signing")

    infoBroker {
        excludedManifestProperties = listOf(
            "Manifest-Version", "Module-Owner", "Module-Email", "Module-Source",
            "Built-OS", "Build-Host", "Build-Job", "Build-Host", "Build-Job", "Build-Number", "Build-Id", "Build-Url",
            "Built-Status"
        )
    }

    contacts {
        addPerson("eric.jesse@aeris-consulting.com", delegateClosureOf<nebula.plugin.contacts.Contact> {
            moniker = "Eric Jessé"
            github = "ericjesse"
            role("Owner")
        })
    }

    repositories {
        mavenLocal()
        mavenCentral()
        maven {
            name = "maven-central-snapshots"
            setUrl("https://oss.sonatype.org/content/repositories/snapshots")
        }
    }

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    signing {
        publishing.publications.forEach { sign(it) }
    }

    val ossrhUsername: String? by project
    val ossrhPassword: String? by project
    publishing {
        publications {
            filterIsInstance<MavenPublication>().forEach {
                it.artifactId = project.name
            }
        }
        repositories {
            mavenLocal()
            maven {
                val releasesRepoUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                val snapshotsRepoUrl = "https://oss.sonatype.org/content/repositories/snapshots/"
                name = "sonatype"
                url = uri(if (project.version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
                credentials {
                    username = ossrhUsername
                    password = ossrhPassword
                }
            }
        }
    }

    tasks {
        withType<Jar> {
            archiveBaseName.set("io-qalipsis-plugin-${project.name}")
        }

        withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
            kotlinOptions {
                jvmTarget = target.majorVersion
                javaParameters = true
                freeCompilerArgs += listOf(
                    "-Xuse-experimental=kotlinx.coroutines.ExperimentalCoroutinesApi",
                    "-Xuse-experimental=kotlinx.coroutines.ObsoleteCoroutinesApi",
                    "-Xallow-result-return-type",
                    "-Xemit-jvm-type-annotations"
                )
            }
        }

        val test = named<Test>("test") {
            ignoreFailures = System.getProperty("ignoreUnitTestFailures", "false").toBoolean()
            exclude("**/*IntegrationTest.*", "**/*IntegrationTest$*")
        }

        val integrationTest = register<Test>("integrationTest") {
            this.group = "verification"
            ignoreFailures = System.getProperty("ignoreIntegrationTestFailures", "false").toBoolean()
            include("**/*IntegrationTest*", "**/*IntegrationTest$*", "**/*IntegrationTest.**")
            exclude("**/*Scenario*.*")
        }

        val scenariosTest = register<Test>("scenariosTest") {
            this.group = "verification"
            ignoreFailures = System.getProperty("ignoreIntegrationTestFailures", "false").toBoolean()
            include("**/*Scenario*IntegrationTest.*")
        }

        named<Task>("check") {
            dependsOn(test.get(), integrationTest.get(), scenariosTest.get())
        }

        if (!project.file("src/main/kotlin").isDirectory) {
            project.logger.lifecycle("Disabling publish for ${project.name}")
            withType<AbstractPublishToMaven> {
                enabled = false
            }
        }

        withType<Test> {
            // Simulates the execution of the tests with a given number of CPUs.
            if (!testNumCpuCore.isNullOrBlank()) {
                project.logger.lifecycle("Running tests of ${project.name} with $testNumCpuCore cores")
                jvmArgs("-XX:ActiveProcessorCount=$testNumCpuCore")
            }
            useJUnitPlatform()
            testLogging {
                events(FAILED, STANDARD_ERROR)
                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL

                debug {
                    events(*org.gradle.api.tasks.testing.logging.TestLogEvent.values())
                    exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
                }

                info {
                    events(*org.gradle.api.tasks.testing.logging.TestLogEvent.values())
                    exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
                }
            }
        }

        artifacts {
            if (project.plugins.hasPlugin("java-test-fixtures")) {
                archives(findByName("testFixturesSources") as Jar)
                archives(findByName("testFixturesJavadoc") as Jar)
                archives(findByName("testFixturesJar") as Jar)
            }
        }
    }
}

val testTasks = subprojects.flatMap {
    val testTasks = mutableListOf<Test>()
    (it.tasks.findByName("test") as Test?)?.apply {
        testTasks.add(this)
    }
    (it.tasks.findByName("integrationTest") as Test?)?.apply {
        testTasks.add(this)
    }
    testTasks
}

tasks.register("testReport", TestReport::class) {
    this.group = "verification"
    destinationDir = file("${buildDir}/reports/tests")
    reportOn(*(testTasks.toTypedArray()))
}
