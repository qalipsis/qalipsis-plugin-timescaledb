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
import org.jetbrains.kotlin.gradle.plugin.extraProperties
import org.jreleaser.model.Active
import org.jreleaser.model.Signing
import org.jreleaser.model.api.deploy.maven.MavenCentralMavenDeployer

plugins {
    idea
    java
    kotlin("jvm") version "1.9.25"
    kotlin("kapt") version "1.9.25"
    kotlin("plugin.allopen") version "1.9.25"
    `maven-publish`
    id("org.jreleaser") version "1.18.0"
    id("com.github.jk1.dependency-license-report") version "2.9"
    id("com.palantir.git-version") version "3.0.0"
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

description = "QALIPSIS plugin for TimescaleDB"

tasks.withType<Wrapper> {
    distributionType = Wrapper.DistributionType.BIN
    gradleVersion = "8.14.1"
}

val testNumCpuCore: String? by project

jreleaser {
    gitRootSearch.set(true)

    release {
        // One least one enabled release provider is mandatory, so let's use Github and disable
        // all the options.
        github {
            skipRelease.set(true)
            skipTag.set(true)
            uploadAssets.set(Active.NEVER)
            token.set("dummy")
        }
    }

    val enableSign = !extraProperties.has("qalipsis.sign") || extraProperties.get("qalipsis.sign") != "false"
    if (enableSign) {
        signing {
            active.set(Active.ALWAYS)
            mode.set(Signing.Mode.MEMORY)
            armored = true
        }
    }

    deploy {
        maven {
            mavenCentral {
                register("qalipsis-releases") {
                    active.set(Active.RELEASE_PRERELEASE)
                    namespace.set("io.qalipsis")
                    applyMavenCentralRules.set(true)
                    stage.set(MavenCentralMavenDeployer.Stage.UPLOAD)
                    stagingRepository(layout.buildDirectory.dir("staging-deploy").get().asFile.path)
                }
            }
            nexus2 {
                register("qalipsis-snapshots") {
                    active.set(Active.SNAPSHOT)
                    // Here we are using our own repository, because the maven central snapshot repo
                    // is too often not available.
                    url.set("https://maven.qalipsis.com/repository/oss-snapshots/")
                    snapshotUrl.set("https://maven.qalipsis.com/repository/oss-snapshots/")
                    applyMavenCentralRules.set(true)
                    verifyPom.set(false)
                    snapshotSupported.set(true)
                    closeRepository.set(true)
                    releaseRepository.set(true)
                    stagingRepository(layout.buildDirectory.dir("staging-deploy").get().asFile.path)
                }
            }
        }
    }
}


allprojects {
    group = "io.qalipsis.plugin"
    version = File(rootDir, "project.version").readText().trim()

    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")
    apply(plugin = "com.palantir.git-version")

    repositories {
        mavenLocal()
        if (version.toString().endsWith("-SNAPSHOT")) {
            maven {
                name = "QALIPSIS OSS Snapshots"
                url = uri("https://maven.qalipsis.com/repository/oss-snapshots")
                content {
                    includeGroup("io.qalipsis")
                }
            }
        }
        mavenCentral()
    }

    kotlin {
        javaToolchains {
            jvmToolchain(11)
        }
    }

    java {
        withJavadocJar()
        withSourcesJar()
    }

    tasks {

        withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
            kotlinOptions {
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

        named<Task>("check") {
            dependsOn(test.get(), integrationTest.get())
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

    project.afterEvaluate {
        publishing {
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])
                    pom {

                        name.set(project.name)
                        description.set(project.description)

                        if (version.toString().endsWith("-SNAPSHOT")) {
                            this.withXml {
                                this.asNode().appendNode("distributionManagement").appendNode("repository").apply {
                                    this.appendNode("id", "qalipsis-oss-snapshots")
                                    this.appendNode("name", "QALIPSIS OSS Snapshots")
                                    this.appendNode("url", "https://maven.qalipsis.com/repository/oss-snapshots")
                                }
                            }
                        }
                        url.set("https://qalipsis.io")
                        licenses {
                            license {
                                name.set("GNU AFFERO GENERAL PUBLIC LICENSE, Version 3 (AGPL-3.0)")
                                url.set("http://opensource.org/licenses/AGPL-3.0")
                            }
                        }
                        developers {
                            developer {
                                id.set("ericjesse")
                                name.set("Eric Jessé")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://github.com/qalipsis/qalipsis-plugin-timescaledb.git")
                            url.set("https://github.com/qalipsis/qalipsis-plugin-timescaledb.git/")
                        }
                    }
                }
            }
        }
    }
}

val allTestTasks = subprojects.flatMap {
    val testTasks = mutableListOf<Test>()
    (it.tasks.findByName("test") as Test?)?.apply {
        testTasks.add(this)
    }
    (it.tasks.findByName("integrationTest") as Test?)?.apply {
        testTasks.add(this)
    }
    testTasks
}

tasks.register("aggregatedTestReport", TestReport::class) {
    group = "documentation"
    description = "Create an aggregated test report"

    destinationDirectory.set(project.layout.buildDirectory.dir("reports/tests"))
    testResults.from(*(allTestTasks.toTypedArray()))
    dependsOn.clear()
}
