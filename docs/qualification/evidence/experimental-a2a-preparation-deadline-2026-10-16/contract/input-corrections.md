# RPC method-name correction (source inventory error)

The reviewer authored the earlier contract/input inventory and mistakenly used four internal operation labels as JSON-RPC wire method strings. That source-derived inventory was not a runtime decoder result. The actual locked a2a-lf0.3.0 jsonrpc.rs methods constants are authoritative:

| Internal operation | Constant | Actual wire method |
| --- | --- | --- |
| CreatePushConfig | a2a::methods::CREATE_PUSH_CONFIG | CreateTaskPushNotificationConfig |
| GetPushConfig | a2a::methods::GET_PUSH_CONFIG | GetTaskPushNotificationConfig |
| ListPushConfigs | a2a::methods::LIST_PUSH_CONFIGS | ListTaskPushNotificationConfigs |
| DeletePushConfig | a2a::methods::DELETE_PUSH_CONFIG | DeleteTaskPushNotificationConfig |

This corrects the JSON_RPC_method field in immutable r1-next-implementation-audit-v3/entry-matrix.json and RPC method fields in immutable a2a-initial-authority-preflight-v1/valid-wire-candidates.json. The human operation labels, REST paths, decoded fields/hints and deadline/error/ownership contracts remain unchanged. Old seals and red receipts are preserved; neither is silently rewritten. The v2 test fixture also used the wrong strings and failed real positive controls, so its7/2 outcome cannot qualify all11 operations. Future final source/tests must use the actual locked constants and prove valid decoded control before timeout/refusal cases. This is an inventory/fixture mistake, not a production regression or an excuse to weaken the SDK.

The same reviewer wrote the originating contract but did not author production or these tests. The independent Spec axis here concerns the implemented source and actual evidence; it is not an independent endorsement of the reviewer's own design.
