rootProject.name = "rub"

// Composite build: Rub is engine 13 of the ecosystem — the observability engine. Including
// SmokeHouse's build transitively includes SuperBeefSort and CSRBT (nested composites);
// Gradle substitutes every published coordinate with the live sibling sources. Rub reads the
// store's public tail and vitals only — it never reaches inside any engine.
includeBuild("../SmokeHouse")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
