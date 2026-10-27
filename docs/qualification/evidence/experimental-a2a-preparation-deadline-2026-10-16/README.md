# Fixed-source component evidence

Run `python3 -B verify.py --repository-root /path/to/repository` for pure offline verification. Add `--workspace --workspace-root /path/to/checkout` to bytecheck only the540 supplied inputs. No preserved runner is executed. Existing published references must remain available.

`artifact-index.json` maps every copied original to its original path and original raw SHA, plus actual stored bytes/SHA. Deterministic gzip keeps original log/patch/receipt content. `manifest.json` covers the evidence files; `seal.json` binds it. `report-bindings.json` also binds all three reports. The outer candidate manifest includes the complete delivery.

Only540 supplied compilation inputs are reconstructed: published subscription538 plus4 new/changed code payloads. The931-row source manifest is checked as a digest and against its compiled subset, not reconstructed wholesale. WholeGit, new adoption docs, compiler read closure and previous execution qualifications are excluded.

Original gate argv/cwd/runnerSHA/profile and exact logs remain in `original/`. The first stale-cache TCP attempt is not qualification for the new product. Source-freshness observations and actual recompiled fixture failures remain distinct. Original patches reconstruct each successive compiled-input map.

The package author wrote the tests. Independent reviews and actual-main adoption must be delivered separately; the pending state in this seal is historical and must not be retroactively edited. No Native/IdP/durableTask/fullCI/cold30m/stdio/R1 qualification is granted.
