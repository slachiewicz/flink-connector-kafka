---
name: verify-module
description: Run the full CI-parity Maven verification (checkstyle, spotless, RAT license check, dependency analyze, tests) scoped to the module(s) you changed in flink-connector-kafka, instead of a plain build/test that skips these checks. Use before considering work done or before opening a PR.
---

Determine which module(s) changed (e.g. `flink-connector-kafka`, `flink-sql-connector-kafka`,
a submodule under `flink-connector-kafka-e2e-tests`, or `flink-python`) from the files you edited.

Run, from the repo root:

```
mvn clean verify -DskipITs -pl <module> -am
```

- `-pl <module>` scopes the build to the changed module(s) (comma-separate if more than one).
- `-am` also builds required upstream modules so cross-module changes are validated together.
- `verify` (not just `test`) is required to trigger `checkstyle`, `spotless:check`, `rat:check`,
  and the `maven-dependency-plugin` analyze goal (`failOnWarning=true`) — a plain `mvn test`
  silently skips all of these and can pass locally while CI fails.
- `-DskipITs` skips `*ITCase` integration tests (Failsafe), which spin up Testcontainers/Docker
  and are slow. It does NOT skip checkstyle/spotless/rat/dependency-analyze — those still run.
  Drop `-DskipITs` (and make sure Docker is running) when you need full CI parity, e.g. right
  before opening a PR that touches integration-tested code paths.

If it fails on style, run `mvn spotless:apply -pl <module>` and re-verify. If it fails on RAT
(missing/incorrect ASF license header), add the standard Apache license header block matching
the other files in the module. If it fails on dependency analyze, add the missing dependency
explicitly or remove the now-unused one — don't suppress the check.

Report which checks passed/failed and any remaining failures verbatim; don't summarize away
build errors.
