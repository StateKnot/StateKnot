# Debug publication v2 — independent packaging increment

**Remaining findings: 0.** Fixed v2 candidate manifest `cc98e0d5…`, freeze `2d756f7b…`: 98 files / 594517 bytes. This review is limited to the lossless packaging delta from independently reviewed v1 (`29815d58…`); original v1 package/review and root's staged whitespace failure remain unchanged.

Exactly two original patch paths become `.patch.gz`. Each decodes to the original 918 bytes, SHA `27104f8f…`; deterministic compression reproduces the entire 396-byte gzip, SHA `04fa25bb…`, with mtime0 and no original filename header. No patch text or context whitespace was edited.

Ninety-two candidate files remain byte-identical. The only other changed files are artifact-index, evidence manifest, seal and machine JSON. Index changes retain raw identity and alter storage metadata only; seal updates its manifest hash. Machine JSON updates evidence hashes and adds an accurate packaging explanation. All remaining source, contract, execution, history, Native, resource and scope facts are unchanged. EN/CN reports and all verifiers remain byte-identical. Non-gzip text contains no trailing whitespace.

The final outer offline verifier returned0. No recorded product runner, Native/database/network operation or workspace check was rerun. V1's pre-adoption workspace rejection keeps its original time/scope; root's later adopted-source check is separate. No new runtime/CI/R1 qualification is created.

A first review-helper assertion expected machine JSON hash changes only and rejected the added packaging note. Its script/receipt is preserved under `tooling/`; explicit note validation closes that helper assumption without changing the candidate. This is not a product failure. This directory contains no raw unified-diff artifact.
