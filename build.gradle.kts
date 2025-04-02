plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Iceberg dependencies
    implementation("org.apache.iceberg:iceberg-core:1.4.3")
    implementation("org.apache.iceberg:iceberg-parquet:1.4.3")
    implementation("org.apache.iceberg:iceberg-data:1.4.3")
    
    // Parquet dependencies
    implementation("org.apache.parquet:parquet-common:1.13.1")
    implementation("org.apache.parquet:parquet-column:1.13.1")
    implementation("org.apache.parquet:parquet-hadoop:1.13.1")
    implementation("org.apache.parquet:parquet-avro:1.13.1")
    
    // Hadoop dependencies
    implementation("org.apache.hadoop:hadoop-client:3.3.6")
    
    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-simple:2.0.9")
    
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("com.example.iceberg.IcebergDemoKt")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.example.iceberg.IcebergDemoKt"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
} 