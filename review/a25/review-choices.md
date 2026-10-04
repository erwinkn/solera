# A25 review choices

Reviewed phase 1 at `dcfc0692be17731c8586a48e44fa251e6fa0828d`. Workspace HEAD was `1c6f9ef18167fbe1eb27948bac00386ecfaf09fb`. All reviewed files came from immutable Git objects. D117 records that choice; D118 records the review verdict.

| Choice | Alternative | Rationale |
|---|---|---|
| Review the submitted phase 1 snapshot | Chase W53's developing prototype | A25 is an independent review of the completed design. The coordinator can use findings while phase 2 proceeds. |
| Run small Python probes using committed module text | Rebuild native code or run the full benchmark | The confirmed executable defects are in the glob helper and analytical replay. No native two-view implementation exists at the reviewed revision. |
| Treat the algebra as viable, with required fixes | Reject two views outright, or accept the note as written | Small exhaustive histories support the ordinary net merge. Read-ahead, glob filtering, lifecycle naming and retention still have concrete counterexamples. |
| Require a bounded retention policy to preserve every active query boundary | Assume an oldest-position summary can replace all history | A pass ending at 7 cannot read a single net node spanning 4 through 15. A durable expiry/restart policy is another valid option, with its costs and semantics measured. This is a suggested alternative, not a policy decision for Erwin. |
| Keep generated names unique across index lives and reclamation | Rely only on engine epochs and deterministic bytes | A reset does not change the engine epoch, and changes the bytes for the same aligned commit range. This is a review recommendation, not an implementation. |
| Compare physical retention and all commit alignments | Use logical K runs or a 32-commit stride as a storage/fan-in bound | The base watermark participates in the floor. The stride systematically misses populated low levels. |
| Leave fixes and policy choices with the coordinator | Edit the experiment branch | The assignment is read-only. Review artifacts are in `/tmp/solera-a25-review`; no source, dependency, branch, or install changes were made. |

The implementation's decisions, alternatives and evidence are audited in the handoff's reuse/simplicity section. Codec speed was not remeasured. No platform, full-suite, object-store or phase 2 performance gate is claimed.
