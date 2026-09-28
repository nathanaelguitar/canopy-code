# Feishu question card controller Rust port

`feishu_question_controller.rs` ports the presentation, reservation, authenticated
claim, deferred response, expiry, settlement, run-cancellation, disposal, and
terminal-card projection state machine from
`packages/channels/feishu/src/question-card-controller.ts`. It reuses
`feishu_question_card.rs` for card JSON, action parsing, and answer validation.
Terminal card patches and their text fallbacks run through a per-request FIFO
queue, preserving cancellation then accepted-response ordering. A settlement
echo while `respond` is in flight is ignored; a response accepted after run
cancellation can update the projection, while a rejected response keeps the
run-cancelled projection. Callback-delivered cancellation cards are not patched
again.

The channel-base `ChannelUserInputRequestContext` and
`UserInputPresentationResult` are not ported yet. This module defines local
`FeishuQuestionContext`, settlement/response types, and async adapter callback
types; adapter integration must map the shared channel types to these local
types. Feishu SDK ingress and Card API implementations remain outside this
module. Asynchronous callbacks and timeout scheduling require an active Tokio
runtime. If a synchronous shutdown method runs without one, state cleanup still
closes the request and the controller reports that terminal projection could
not be scheduled.

Eighteen focused controller tests cover synchronous settlement during listener
registration, delivery failure and retry, same-scope rejection and independent
owners, captured chat correlation, submit/cancel callbacks, validation and
authentication failures, timeout ordering, run-wide cleanup, disposal and late
delivery, settlement echoes, accepted and rejected response races, and patch
fallbacks. The tests run in a standalone offline Cargo harness together with
the eight question-card helper tests. The controller is not exported from
`channels/mod.rs` by this slice.
