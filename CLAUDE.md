<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Official Apache Flink Kafka connector (Java, Maven multi-module). Parent POM
`org.apache.flink:flink-connector-parent` supplies most plugin config (checkstyle,
spotless, RAT) — much of the enforced style lives outside this repo.

Modules:
- `flink-connector-kafka` — core connector source
- `flink-sql-connector-kafka` — SQL/Table shaded jar
- `flink-connector-kafka-e2e-tests` — end-to-end test modules
- `flink-python` — PyFlink bindings
- `docs/` — doc site sources (content + content.zh)
- `tools/` — CI and Maven support, incl. `tools/maven/checkstyle.xml`

## Build & test

- Full build: `mvn clean package -DskipTests` (Java 11 to build; CI matrix tests JDK 11/17/21)
- Single test: `mvn -pl flink-connector-kafka -Dtest=ClassName test`
- Test naming: `*Test` = unit tests (Surefire), `*ITCase` = integration tests (Failsafe,
  often use Testcontainers — expect these to be slower and require Docker)
- Style/license: `mvn spotless:check` / `mvn spotless:apply`, `mvn rat:check` (ASF license
  header check, runs non-inherited per module)
- `maven-dependency-plugin` analyze runs at `verify` with `failOnWarning=true` — unused or
  undeclared dependencies fail the build, not just style/tests

## Conventions

- Commit/PR titles reference the Apache Flink JIRA ticket: `[FLINK-XXXXX] Description`.
  Use `[hotfix] Description` for minor non-ticketed changes. Optional tags like `[docs]`,
  `[tests]`, `[metrics]` follow the ticket ID, e.g. `[FLINK-38919][docs] ...`.
- No local `CONTRIBUTING.md` — see the general "how to contribute to Apache Flink" guide and
  issues.apache.org/jira/browse/FLINK for ticket tracking.
