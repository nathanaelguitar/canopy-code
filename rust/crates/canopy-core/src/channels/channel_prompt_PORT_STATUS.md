# ChannelBase message prompt projection port

Implemented as canopy-core::channels::channel_prompt. It builds the
user-visible display projection and enriches model text with sanitized speaker
and mentioned-member attribution, quoted reply context, file-path attachment
references, metadata, and an inline image selected from the envelope or first
usable image attachment.

The caller supplies the resolved session scope and whether slash-command
parsing recognizes the current command. Session routing, authorization,
command execution, prompt dispatch, collected-message queues, lifecycle events,
and bridge invocation remain outside this helper. Rust strings preserve valid
Unicode scalars; JavaScript lone UTF-16 surrogates cannot be represented. Eight
focused crate tests cover speaker scopes, attribution suppression, mention and
quote sanitization, image selection, valid-path preservation, metadata order,
and the 8,000-code-point display cap.
