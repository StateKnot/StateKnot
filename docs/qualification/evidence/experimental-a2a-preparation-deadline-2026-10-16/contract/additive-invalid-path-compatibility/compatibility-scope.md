# Additive compatibility decision

This owner-confirmed clarification supplements the immutable r1-next-implementation-audit-v3 contract; it does not rewrite its seal or any historical result. The implementation/test authors remain responsible for future actual execution.

“Original error maps” covers timely authorization/admission callbacks, original validation/capability errors, and the original SDK's individual buffer/syntax/ProtoJSON rejection on an otherwise valid supported task path. Normal supported requests retain the original request parts/headers/raw push bytes and the same configured size ceiling. It does not assert byte-identical error precedence for requests invalid in multiple independent ways.

The locked SDK originally extracts Path<String> before Json<Value> on REST push. The boundary's newly finite early buffer may therefore reject a raw invalid UTF-8 percent-encoded task path such as `%FF` with an accompanying pending or oversized body before the SDK's Path rejection. The owner accepts this change in compound-invalid rejection order. All such requests must still fail closed, obtain no backend authority, and dispatch no backend method. A future negative control should assert rejection and backend0, not a matching legacy status/message. No copy of the SDK Path parser, public API, permission change, background phase tracker, or extra body parser is justified.

SDK invalid synchronous decode paths still terminate through original errors without entering prepare/backend. They are not preemptible CPU work, and no hard real-time response deadline or universal malformed-input error precedence is claimed. Original authentication/backend budgets and limits remain separate and unchanged.
