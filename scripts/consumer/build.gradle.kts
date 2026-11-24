plugins {
    kotlin("jvm") version "2.4.20"
    kotlin("plugin.serialization") version "2.4.20"
    application
}

repositories {
    mavenLocal()
    mavenCentral()
}

val loadshiftVersion: String = providers.gradleProperty("loadshiftVersion").orElse(
    providers.fileContents(layout.projectDirectory.file("../../templates/library.module-template.yaml")).asText
        .map { Regex("""version: (\S+)""").find(it)!!.groupValues[1] },
).get()

val javaVersion: Int = providers.gradleProperty("javaVersion").orElse("17").get().toInt()

dependencies {
    for (module in listOf(
        "local", "camunda-7", "camunda-7-java-api", "camunda-8", "camunda-8-stream", "web", "sqlite", "otel", "micrometer",
    )) {
        implementation("io.github.it-atelier-gn:loadshift-$module:$loadshiftVersion")
    }
    implementation("org.camunda.bpm:camunda-engine:7.24.0")
    implementation("org.camunda.bpm:camunda-engine-plugin-spin:7.24.0")
    implementation("org.camunda.spin:camunda-spin-dataformat-json-jackson:7.24.0")
    runtimeOnly("com.h2database:h2:2.3.232")
}

kotlin { jvmToolchain(javaVersion) }

application {
    mainClass.set("MainKt")
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED").takeIf { javaVersion >= 22 }.orEmpty()
}
