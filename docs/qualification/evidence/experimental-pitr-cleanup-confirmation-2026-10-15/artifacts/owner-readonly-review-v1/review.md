# PITR cleanup candidate: focused read-only review

Reviewed fixed baseline `8dcaa48e…`, candidate `d6b0037366d7fb4adb38c21f0a4602de10c595c0ce625b65aa7fac114250387d`, and patch `0182c425…`. The baseline equals the final922 and adopted-main file bytes. This is an independent focused source/spec review, not a full two-axis skill review, compile result, Native qualification, or confirmation of the earlier missing stderr/cause.

No actionable finding in the requested scope.

| Requirement | Source evidence / assessment |
| --- | --- |
| Only own resources | The two stop calls use the tracked source/target names created from this test's fresh UUID (lines 212–218, 274, 386, 425). Final cleanup takes names only from those tracked vectors (77–101). No inventory-derived name is mutated. |
| Daemon errors cannot mean absence | Both inventory calls use `docker_ok`; nonzero status panics and invalid UTF-8 panics (142–150). Name filtering can include partial matches, but absence is decided using an exact output-line comparison (69, 98). An unsuccessful list cannot produce the cleanup marker. |
| Confirm actual removal | Explicit stop keeps ownership until a successful `container ls --all` excludes the exact name (47–73). Volumes remain tracked until successful removal and successful inventory exclusion (80–101). The marker is after all business assertions and `cleanup_and_confirm` (455–470). |
| Original budgets/resources | Each stop/confirmation shares one deadline: source grace10/30s, final target grace2/10s. Each volume removal/confirmation shares its original10s. There is no fresh per-poll timeout. The original100ms subprocess polling helper remains byte-identical, so this is its existing bounded polling behavior, not a hard real-time deadline guarantee. Pool/lease/WAL/restore resources and original PITR assertion body are exact. |
| Failure and Drop | A failing current stop/remove/list panics before dropping that name; remaining tracked names reach the byte-identical best-effort Drop. Resources successfully confirmed absent are no longer tracked. Drop still ignores ordinary nonzero deletion statuses and retains its original spawn/wait/timeout panic behavior, so this candidate cannot promise cleanup on every failure path. |
| Cause and qualification | Waiting for actual `--rm` removal addresses a possible race visible in the old ordering. The original failing `volume rm` stderr was not captured, so the real cause remains unproven. Only a future independently captured execution can qualify the candidate. |

The old PG16 WAL volume remains untouched. No compilation, database call, Docker command, candidate change, main change, or qualification transfer was performed by this review.
