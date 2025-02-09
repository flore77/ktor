val serialization_version: String by project.extra

description = "Ktor CBOR Content Negotiation via kotlinx.serialization support"

plugins {
    id("kotlinx-serialization")
}

kotlin.sourceSets {
    commonMain {
        dependencies {
            api(project(":ktor-shared:ktor-serialization:ktor-serialization-kotlinx"))
            api("org.jetbrains.kotlinx:kotlinx-serialization-cbor:$serialization_version")
        }
    }
    commonTest {
        dependencies {
            api("org.jetbrains.kotlinx:kotlinx-serialization-json:$serialization_version")
            api(project(":ktor-shared:ktor-serialization:ktor-serialization-kotlinx:ktor-serialization-kotlinx-tests"))
        }
    }
}
