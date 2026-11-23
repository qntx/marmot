//! Typed application-message semantics: the send-side [`MessageIntent`]
//! vocabulary and the receive-side [`Semantics`] classification.
//!
//! Pure module: no storage, no I/O, no engine access. Wire shapes follow the
//! White Noise 0.17.0 ground truth recorded in
//! `vectors/wn/capture-0.17.0.json`.

use cgka_traits::app_event::{
    MARMOT_APP_EVENT_KIND_CHAT, MARMOT_APP_EVENT_KIND_DELETE, MARMOT_APP_EVENT_KIND_EDIT,
    MARMOT_APP_EVENT_KIND_REACTION, MARMOT_APP_EVENT_KIND_REMOVE,
};

use crate::types::{AccountId, EventId};

/// The kind-4891 admin-removal payload, byte-identical to White Noise.
const REMOVAL_CONTENT: &str = "{\"v\":1,\"action\":\"remove\"}";

/// Most reactions one `Unreact` may carry.
const MAX_UNREACT_IDS: usize = 100;
/// Most Unicode scalars one reaction emoji may carry.
const MAX_EMOJI_SCALARS: usize = 64;

/// A typed application message to send into a group.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MessageIntent {
    /// A chat message; `reply_to` adds the `e`/`q` reply pair. Kind 9.
    Chat {
        /// Message body; a reply's body must be non-empty after trim.
        content: String,
        /// The replied-to inner event id.
        reply_to: Option<EventId>,
    },
    /// A reaction on `target`. Kind 7, one `e` tag, content the emoji.
    Reaction {
        /// The reacted-to inner event id.
        target: EventId,
        /// The emoji: non-empty, unpadded, no control characters, at most
        /// 64 Unicode scalars.
        emoji: String,
    },
    /// Withdraw own reactions. Kind 5, one `e` tag per reaction id,
    /// 1..=100 ids, deduplicated in input order.
    Unreact {
        /// Own reaction event ids to withdraw.
        reactions: Vec<EventId>,
    },
    /// Replace `target`'s content. Kind 1009, one `e` tag.
    Edit {
        /// The message this edit replaces.
        target: EventId,
        /// The replacement body; must be non-empty after trim.
        content: String,
    },
    /// Delete `target`: kind 4891 (admin removal) when the sender holds
    /// admin, else kind 5 — the White Noise rule.
    Delete {
        /// The message to delete.
        target: EventId,
    },
    /// Admin removal of `target`. Always kind 4891; refuses with
    /// [`crate::SessionError::NotGroupAdmin`] unless the sender is a
    /// current group admin.
    Remove {
        /// The message to remove.
        target: EventId,
    },
}

/// The typed meaning of a received application message.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Semantics {
    /// A chat message; `reply_to` is the `q` target when valid, else the
    /// `e` target when valid, else none — a bad reference never drops the
    /// message.
    Chat {
        /// The replied-to inner event id.
        reply_to: Option<EventId>,
    },
    /// A reaction on `target` carrying `emoji`. A host keys reactions by
    /// `(target, sender, emoji)`, latest `(created_at, id)` wins.
    Reaction {
        /// The reacted-to inner event id.
        target: EventId,
        /// The reaction emoji.
        emoji: String,
    },
    /// An admin removal of `target` (kind 4891, granted moderation
    /// authority).
    Removal {
        /// The removed message's inner event id.
        target: EventId,
    },
    /// A kind-1009 edit the index authorized: `target` is a kind-9 message
    /// the sender authored; `content` replaces its body.
    Edit {
        /// The edited message's inner event id.
        target: EventId,
        /// The replacement body.
        content: String,
    },
    /// A kind-5 delete the index authorized; every listed target was
    /// authored by the sender (chats, reactions, edits). Not all targets
    /// are reactions — those are [`Semantics::Unreact`].
    Deletion {
        /// The honoured targets' inner event ids, in tag order.
        targets: Vec<EventId>,
    },
    /// A kind-5 delete the index authorized whose honoured targets are all
    /// reactions — a withdraw-own-reactions.
    Unreact {
        /// The honoured reaction ids, in tag order.
        reactions: Vec<EventId>,
    },
    /// Every other kind, delivered untyped.
    Other,
}

/// Why a received application event was dropped instead of delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropReason {
    /// The message's source-epoch retention elapsed before drain.
    Expired,
    /// Kind 4891 without granted moderation authority, or a kind 1009/5
    /// whose known target was authored by another account or has the wrong
    /// kind.
    Unauthorized,
    /// A kind 1009/5 whose target was still unknown when its hold expired
    /// or overflowed the hold queue.
    Unverifiable,
    /// Kind 7 with an invalid target or emoji; kind 4891 whose removal
    /// payload does not parse; a kind 1009 or 5 without a usable `e` tag.
    Malformed,
    /// Kinds 452–477 are reserved for the device-group draft's pairing
    /// traffic and never carry a chat payload.
    Reserved,
}

impl DropReason {
    /// The stable `snake_case` name of this reason.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Unauthorized => "unauthorized",
            Self::Unverifiable => "unverifiable",
            Self::Malformed => "malformed",
            Self::Reserved => "reserved",
        }
    }
}

/// A 32-byte id encoded as 64 lowercase hex characters; uppercase and
/// other lengths do not parse.
pub(crate) fn event_id_of_hex(raw: &str) -> Option<EventId> {
    if raw.len() != 64
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
    {
        return None;
    }
    let bytes: [u8; 32] = hex::decode(raw).ok()?.try_into().ok()?;
    Some(EventId(bytes))
}

/// The `e`-/`q`-tag id convention: `q` first, then `e`, each returning the
/// first valid id.
fn tagged_target(tags: &[Vec<String>], name: &str) -> Option<EventId> {
    tags.iter()
        .filter(|tag| tag.first().map(String::as_str) == Some(name))
        .find_map(|tag| tag.get(1).and_then(|raw| event_id_of_hex(raw)))
}

/// The emoji rule (MDK `validate_reaction_content`): non-empty, no
/// leading/trailing whitespace, no control characters, at most 64
/// Unicode scalars.
fn valid_emoji(emoji: &str) -> bool {
    !emoji.is_empty()
        && emoji.trim() == emoji
        && !emoji.chars().any(char::is_control)
        && emoji.chars().count() <= MAX_EMOJI_SCALARS
}

/// The classification a received inner event maps to at drain.
pub(crate) enum Classified {
    /// Deliver with this semantics.
    Deliver(Semantics),
    /// Drop with this reason.
    Drop(DropReason),
    /// A kind 1009/5 modifier — the session resolves it against the
    /// delivered index (deliver / hold / drop).
    Modifier(Modifier),
}

/// A kind-1009 edit or kind-5 delete parsed off the wire, awaiting
/// authorization against the delivered index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Modifier {
    /// Kind 1009: replace `target`'s content.
    Edit {
        /// The target's inner event id.
        target: EventId,
        /// The replacement body.
        content: String,
    },
    /// Kind 5: void `targets` — deduplicated `e`-tag ids in tag order.
    Delete {
        /// The targets' inner event ids.
        targets: Vec<EventId>,
    },
}

impl Modifier {
    /// The inner ids the modifier needs indexed before it can resolve.
    pub(crate) const fn targets(&self) -> &[EventId] {
        match self {
            Self::Edit { target, .. } => std::slice::from_ref(target),
            Self::Delete { targets } => targets.as_slice(),
        }
    }
}

/// What the delivered index knows about a would-be modifier target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetFacts {
    /// The indexed message's author.
    pub author: AccountId,
    /// The indexed message's kind.
    pub kind: u64,
}

/// The outcome of resolving a modifier against the delivered index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Resolution {
    /// A target is still unknown and the attempt was not final: hold the
    /// modifier.
    Hold,
    /// Authorized: deliver with this semantics.
    Deliver(Semantics),
    /// Refused: drop with this reason.
    Drop(DropReason),
}

/// Classify a decoded inner event. `grant` is `authority.moderation_grant`
/// (`None` when the engine carried no authority evidence); `expires_at` is
/// the source-epoch retention verdict; `now_secs` is the drain-time clock.
pub(crate) fn classify(
    kind: u64,
    tags: &[Vec<String>],
    content: &str,
    grant: Option<bool>,
    expires_at: Option<u64>,
    now_secs: u64,
) -> Classified {
    if expires_at.is_some_and(|expiry| expiry <= now_secs) {
        return Classified::Drop(DropReason::Expired);
    }
    // The device-group draft's pairing kinds never carry a chat payload.
    if (452..=477).contains(&kind) {
        return Classified::Drop(DropReason::Reserved);
    }
    match kind {
        MARMOT_APP_EVENT_KIND_CHAT => Classified::Deliver(Semantics::Chat {
            reply_to: tagged_target(tags, "q").or_else(|| tagged_target(tags, "e")),
        }),
        MARMOT_APP_EVENT_KIND_REACTION => {
            let target = tags
                .iter()
                .find(|tag| tag.first().map(String::as_str) == Some("e"))
                .and_then(|tag| tag.get(1))
                .and_then(|raw| event_id_of_hex(raw));
            match target {
                Some(target) if valid_emoji(content) => Classified::Deliver(Semantics::Reaction {
                    target,
                    emoji: content.to_owned(),
                }),
                _ => Classified::Drop(DropReason::Malformed),
            }
        }
        MARMOT_APP_EVENT_KIND_REMOVE => {
            if grant != Some(true) {
                return Classified::Drop(DropReason::Unauthorized);
            }
            cgka_traits::reporting::parse_removal(tags, content)
                .and_then(|raw| event_id_of_hex(&raw))
                .map_or(Classified::Drop(DropReason::Malformed), |target| {
                    Classified::Deliver(Semantics::Removal { target })
                })
        }
        MARMOT_APP_EVENT_KIND_EDIT => {
            let e_tags: Vec<&Vec<String>> = tags
                .iter()
                .filter(|tag| tag.first().map(String::as_str) == Some("e"))
                .collect();
            match e_tags.as_slice() {
                [tag] => tag.get(1).and_then(|raw| event_id_of_hex(raw)).map_or(
                    Classified::Drop(DropReason::Malformed),
                    |target| {
                        Classified::Modifier(Modifier::Edit {
                            target,
                            content: content.to_owned(),
                        })
                    },
                ),
                _ => Classified::Drop(DropReason::Malformed),
            }
        }
        MARMOT_APP_EVENT_KIND_DELETE => delete_targets(tags)
            .map_or(Classified::Drop(DropReason::Malformed), |targets| {
                Classified::Modifier(Modifier::Delete { targets })
            }),
        _ => Classified::Deliver(Semantics::Other),
    }
}

/// The most distinct targets a kind-5 delete may name; more is malformed.
const MAX_DELETE_TARGETS: usize = MAX_UNREACT_IDS;

/// Collect the distinct valid `e` targets of a kind-5 event in tag order.
/// `None` when no valid target is present or a `MAX_DELETE_TARGETS`+1th
/// distinct id appears — both malformed, and the bound caps the dedupe and
/// the held-modifier memory a single event can claim.
fn delete_targets(tags: &[Vec<String>]) -> Option<Vec<EventId>> {
    let mut targets: Vec<EventId> = Vec::new();
    for tag in tags
        .iter()
        .filter(|tag| tag.first().map(String::as_str) == Some("e"))
    {
        let Some(id) = tag.get(1).and_then(|raw| event_id_of_hex(raw)) else {
            continue;
        };
        if targets.contains(&id) {
            continue;
        }
        if targets.len() == MAX_DELETE_TARGETS {
            return None;
        }
        targets.push(id);
    }
    (!targets.is_empty()).then_some(targets)
}

/// Kinds a kind-5 delete may honour: chat, reaction, edit.
const DELETE_HONOURED_KINDS: [u64; 3] = [
    MARMOT_APP_EVENT_KIND_CHAT,
    MARMOT_APP_EVENT_KIND_REACTION,
    MARMOT_APP_EVENT_KIND_EDIT,
];

/// Resolve a modifier against the delivered index. `lookup` answers the
/// index's `(author, kind)` for a target's inner event id; `final_attempt`
/// is the hold-expiry / overflow / release pass where unknown targets no
/// longer wait.
pub(crate) fn resolve(
    modifier: &Modifier,
    sender: &AccountId,
    mut lookup: impl FnMut(&EventId) -> Option<TargetFacts>,
    final_attempt: bool,
) -> Resolution {
    match modifier {
        Modifier::Edit { target, content } => match lookup(target) {
            Some(facts) if facts.author == *sender && facts.kind == MARMOT_APP_EVENT_KIND_CHAT => {
                Resolution::Deliver(Semantics::Edit {
                    target: *target,
                    content: content.clone(),
                })
            }
            Some(_) => Resolution::Drop(DropReason::Unauthorized),
            None if final_attempt => Resolution::Drop(DropReason::Unverifiable),
            None => Resolution::Hold,
        },
        Modifier::Delete { targets } => {
            let mut honoured: Vec<(EventId, u64)> = Vec::with_capacity(targets.len());
            let mut known_failed = false;
            let mut any_unknown = false;
            for target in targets {
                match lookup(target) {
                    Some(facts)
                        if facts.author == *sender
                            && DELETE_HONOURED_KINDS.contains(&facts.kind) =>
                    {
                        honoured.push((*target, facts.kind));
                    }
                    Some(_) => known_failed = true,
                    None => any_unknown = true,
                }
            }
            if any_unknown && !final_attempt {
                return Resolution::Hold;
            }
            if honoured.is_empty() {
                return Resolution::Drop(if known_failed {
                    DropReason::Unauthorized
                } else {
                    DropReason::Unverifiable
                });
            }
            let honoured_ids: Vec<EventId> = honoured.iter().map(|(target, _)| *target).collect();
            if honoured
                .iter()
                .all(|(_, kind)| *kind == MARMOT_APP_EVENT_KIND_REACTION)
            {
                Resolution::Deliver(Semantics::Unreact {
                    reactions: honoured_ids,
                })
            } else {
                Resolution::Deliver(Semantics::Deletion {
                    targets: honoured_ids,
                })
            }
        }
    }
}

/// The `(kind, tags, content)` an intent encodes to — the wire shapes the
/// White Noise capture pins. `sender_is_admin` picks the kind-4891 vs
/// kind-5 `Delete` shape.
///
/// Errors are fixed field labels for [`crate::SessionError::InvalidInput`].
pub(crate) fn build_parts(
    intent: &MessageIntent,
    sender_is_admin: bool,
) -> Result<(u64, Vec<Vec<String>>, String), &'static str> {
    let hex = |id: &EventId| hex::encode(id.0);
    let target_tag = |id: &EventId| vec!["e".to_owned(), hex(id)];
    match intent {
        MessageIntent::Chat { content, reply_to } => {
            if reply_to.is_some() && content.trim().is_empty() {
                return Err("content");
            }
            let tags = reply_to.map_or_else(Vec::new, |target| {
                vec![target_tag(&target), vec!["q".to_owned(), hex(&target)]]
            });
            Ok((MARMOT_APP_EVENT_KIND_CHAT, tags, content.clone()))
        }
        MessageIntent::Reaction { target, emoji } => {
            if !valid_emoji(emoji) {
                return Err("emoji");
            }
            Ok((
                MARMOT_APP_EVENT_KIND_REACTION,
                vec![target_tag(target)],
                emoji.clone(),
            ))
        }
        MessageIntent::Unreact { reactions } => {
            let mut ids: Vec<EventId> = Vec::with_capacity(reactions.len());
            for id in reactions {
                if !ids.contains(id) {
                    ids.push(*id);
                }
            }
            if ids.is_empty() || ids.len() > MAX_UNREACT_IDS {
                return Err("reactions");
            }
            Ok((
                MARMOT_APP_EVENT_KIND_DELETE,
                ids.iter().map(target_tag).collect(),
                String::new(),
            ))
        }
        MessageIntent::Edit { target, content } => {
            if content.trim().is_empty() {
                return Err("content");
            }
            Ok((
                MARMOT_APP_EVENT_KIND_EDIT,
                vec![target_tag(target)],
                content.clone(),
            ))
        }
        MessageIntent::Delete { target } => {
            if sender_is_admin {
                Ok((
                    MARMOT_APP_EVENT_KIND_REMOVE,
                    vec![target_tag(target)],
                    REMOVAL_CONTENT.to_owned(),
                ))
            } else {
                Ok((
                    MARMOT_APP_EVENT_KIND_DELETE,
                    vec![target_tag(target)],
                    String::new(),
                ))
            }
        }
        MessageIntent::Remove { target } => Ok((
            MARMOT_APP_EVENT_KIND_REMOVE,
            vec![target_tag(target)],
            REMOVAL_CONTENT.to_owned(),
        )),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::shadow_unrelated,
    clippy::excessive_nesting,
    clippy::cognitive_complexity,
    clippy::redundant_clone,
    clippy::default_trait_access,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EventId {
        EventId([byte; 32])
    }

    fn hex_id(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    #[test]
    fn builders_match_wn_wire_shapes() {
        let (kind, tags, content) = build_parts(
            &MessageIntent::Chat {
                content: "r1".into(),
                reply_to: Some(id(0x01)),
            },
            false,
        )
        .unwrap();
        assert_eq!(kind, 9);
        assert_eq!(
            tags,
            vec![
                vec!["e".to_owned(), hex_id(0x01)],
                vec!["q".to_owned(), hex_id(0x01)]
            ]
        );
        assert_eq!(content, "r1");

        let (kind, tags, content) = build_parts(
            &MessageIntent::Reaction {
                target: id(0x02),
                emoji: "👍".into(),
            },
            false,
        )
        .unwrap();
        assert_eq!((kind, content), (7, "👍".to_owned()));
        assert_eq!(tags, vec![vec!["e".to_owned(), hex_id(0x02)]]);

        let (kind, tags, content) = build_parts(
            &MessageIntent::Unreact {
                reactions: vec![id(0x03), id(0x04), id(0x03)],
            },
            false,
        )
        .unwrap();
        assert_eq!((kind, content), (5, String::new()));
        assert_eq!(
            tags,
            vec![
                vec!["e".to_owned(), hex_id(0x03)],
                vec!["e".to_owned(), hex_id(0x04)]
            ]
        );

        let (kind, tags, content) = build_parts(
            &MessageIntent::Edit {
                target: id(0x05),
                content: "new".into(),
            },
            false,
        )
        .unwrap();
        assert_eq!((kind, content), (1009, "new".to_owned()));
        assert_eq!(tags, vec![vec!["e".to_owned(), hex_id(0x05)]]);

        // Non-admin Delete -> kind 5; admin -> kind 4891 with the exact
        // removal payload.
        let (kind, _, content) =
            build_parts(&MessageIntent::Delete { target: id(0x06) }, false).unwrap();
        assert_eq!((kind, content), (5, String::new()));
        let (kind, tags, content) =
            build_parts(&MessageIntent::Delete { target: id(0x06) }, true).unwrap();
        assert_eq!(
            (kind, content),
            (4891, "{\"v\":1,\"action\":\"remove\"}".into())
        );
        assert_eq!(tags, vec![vec!["e".to_owned(), hex_id(0x06)]]);
        let (kind, ..) = build_parts(&MessageIntent::Remove { target: id(0x07) }, false).unwrap();
        assert_eq!(kind, 4891);
    }

    #[test]
    fn validation_labels() {
        assert!(matches!(
            build_parts(
                &MessageIntent::Chat {
                    content: "  ".into(),
                    reply_to: Some(id(1)),
                },
                false,
            ),
            Err("content")
        ));
        // A plain (non-reply) chat may carry empty content.
        assert!(
            build_parts(
                &MessageIntent::Chat {
                    content: String::new(),
                    reply_to: None,
                },
                false,
            )
            .is_ok()
        );
        assert!(matches!(
            build_parts(
                &MessageIntent::Edit {
                    target: id(1),
                    content: " ".into(),
                },
                false,
            ),
            Err("content")
        ));
        for emoji in ["", " 👍", "👍 ", "a\nb"] {
            assert!(
                matches!(
                    build_parts(
                        &MessageIntent::Reaction {
                            target: id(1),
                            emoji: emoji.into(),
                        },
                        false,
                    ),
                    Err("emoji")
                ),
                "{emoji:?}"
            );
        }
        let sixty_five = "e".repeat(65);
        assert!(matches!(
            build_parts(
                &MessageIntent::Reaction {
                    target: id(1),
                    emoji: sixty_five,
                },
                false,
            ),
            Err("emoji")
        ));
        assert!(matches!(
            build_parts(&MessageIntent::Unreact { reactions: vec![] }, false),
            Err("reactions")
        ));
        let too_many: Vec<EventId> = (0u8..=100)
            .map(|n| {
                EventId([
                    n, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7,
                ])
            })
            .collect();
        assert!(matches!(
            build_parts(
                &MessageIntent::Unreact {
                    reactions: too_many,
                },
                false,
            ),
            Err("reactions")
        ));
    }

    #[test]
    fn classify_table() {
        let hex = hex_id(0x09);
        // Plain chat.
        assert!(matches!(
            classify(9, &[], "hi", None, None, 0),
            Classified::Deliver(Semantics::Chat { reply_to: None })
        ));
        // Reply: q wins; non-hex q falls back to e, then None — never a drop.
        let tags = vec![
            vec!["e".to_owned(), hex.clone()],
            vec!["q".to_owned(), "zz".to_owned()],
        ];
        assert!(matches!(
            classify(9, &tags, "r", None, None, 0),
            Classified::Deliver(Semantics::Chat { reply_to: Some(_) })
        ));
        let tags = vec![vec!["q".to_owned(), "zz".to_owned()]];
        assert!(matches!(
            classify(9, &tags, "r", None, None, 0),
            Classified::Deliver(Semantics::Chat { reply_to: None })
        ));
        // Reaction.
        let tags = vec![vec!["e".to_owned(), hex.clone()]];
        assert!(matches!(
            classify(7, &tags, "👍", None, None, 0),
            Classified::Deliver(Semantics::Reaction { .. })
        ));
        // Malformed reactions: uppercase/short target, empty/padded/control/
        // overlong emoji.
        for (tag_value, emoji) in [
            (hex_id(0xab).to_uppercase(), "👍".to_owned()),
            ("abcd".to_owned(), "👍".to_owned()),
            (hex.clone(), String::new()),
            (hex.clone(), " 👍".to_owned()),
            (hex.clone(), "a\u{7}b".to_owned()),
            (hex.clone(), "e".repeat(65)),
        ] {
            let tags = vec![vec!["e".to_owned(), tag_value.clone()]];
            assert!(
                matches!(
                    classify(7, &tags, &emoji, None, None, 0),
                    Classified::Drop(DropReason::Malformed)
                ),
                "{tag_value} {emoji:?}"
            );
        }
        // 4891: unauthorized without a grant, malformed on bad content,
        // Removal on the strict shape.
        let tags = vec![vec!["e".to_owned(), hex.clone()]];
        let body = "{\"v\":1,\"action\":\"remove\"}";
        for grant in [None, Some(false)] {
            assert!(matches!(
                classify(4891, &tags, body, grant, None, 0),
                Classified::Drop(DropReason::Unauthorized)
            ));
        }
        assert!(matches!(
            classify(4891, &tags, "{}", Some(true), None, 0),
            Classified::Drop(DropReason::Malformed)
        ));
        assert!(matches!(
            classify(4891, &tags, body, Some(true), None, 0),
            Classified::Deliver(Semantics::Removal { .. })
        ));
        // 5 -> a `Delete` modifier over deduped valid `e` ids; no usable
        // `e` tag -> malformed.
        let tags = vec![
            vec!["e".to_owned(), hex.clone()],
            vec!["e".to_owned(), hex.clone()],
            vec!["e".to_owned(), hex_id(0x0a)],
        ];
        assert!(matches!(
            classify(5, &tags, "", None, None, 0),
            Classified::Modifier(Modifier::Delete { ref targets })
                if *targets == vec![id(0x09), id(0x0a)]
        ));
        for tags in [
            vec![],
            vec![vec!["e".to_owned(), "zz".to_owned()]],
            vec![vec!["q".to_owned(), hex.clone()]],
        ] {
            assert!(matches!(
                classify(5, &tags, "", None, None, 0),
                Classified::Drop(DropReason::Malformed)
            ));
        }
        // 1009 -> an `Edit` modifier on exactly one valid `e` tag; zero or
        // two `e` tags, or an invalid id -> malformed.
        let tags = vec![vec!["e".to_owned(), hex.clone()]];
        assert!(matches!(
            classify(1009, &tags, "new", None, None, 0),
            Classified::Modifier(Modifier::Edit {
                ref target,
                ref content
            }) if *target == id(0x09) && content == "new"
        ));
        for tags in [
            vec![],
            vec![
                vec!["e".to_owned(), hex.clone()],
                vec!["e".to_owned(), hex_id(0x0a)],
            ],
            vec![vec!["e".to_owned(), "zz".to_owned()]],
        ] {
            assert!(matches!(
                classify(1009, &tags, "new", None, None, 0),
                Classified::Drop(DropReason::Malformed)
            ));
        }
        for kind in [447, 1200, 9_999] {
            assert!(matches!(
                classify(kind, &[], "", None, None, 0),
                Classified::Deliver(Semantics::Other)
            ));
        }
        // Expiry beats every classification.
        assert!(matches!(
            classify(9, &[], "hi", None, Some(10), 10),
            Classified::Drop(DropReason::Expired)
        ));
        assert!(matches!(
            classify(9, &[], "hi", None, Some(11), 10),
            Classified::Deliver(Semantics::Chat { reply_to: None })
        ));
    }

    /// Every inner app event the White Noise 0.17.0 capture observed
    /// (`vectors/wn/capture-0.17.0.json`) classifies exactly as the design
    /// table pins: kind 9 plain/reply -> `Chat`, kind 7 -> `Reaction`,
    /// kind 4891 (granted) -> `Removal`, kinds 5/1009 -> a `Modifier`
    /// (session-resolved), everything else (kind 447) -> `Other`.
    #[test]
    fn wn_fixture_events_classify() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../vectors/wn/capture-0.17.0.json"
        );
        let raw = std::fs::read_to_string(path).expect("fixture");
        let steps: serde_json::Value = serde_json::from_str(&raw).expect("json");
        let mut seen: std::collections::BTreeMap<u64, usize> = Default::default();
        for step in steps.as_array().expect("steps") {
            for message in step["observed"]["messages"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let kind = message["kind"].as_u64().expect("kind");
                *seen.entry(kind).or_default() += 1;
                let tags: Vec<Vec<String>> = message["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|tag| {
                        tag.as_array()
                            .into_iter()
                            .flatten()
                            .map(|item| item.as_str().expect("tag item").to_owned())
                            .collect()
                    })
                    .collect();
                let content = message["content"].as_str().expect("content");
                // 4891 arrives authorized (source-epoch admin) or dropped.
                let grant = (kind == 4891).then_some(true);
                match classify(kind, &tags, content, grant, None, 0) {
                    Classified::Deliver(Semantics::Chat { reply_to }) => {
                        assert_eq!(kind, 9, "only kind 9 is chat");
                        let expected = tags
                            .iter()
                            .find(|tag| tag.first().map(String::as_str) == Some("e"))
                            .and_then(|tag| tag.get(1))
                            .and_then(|raw| event_id_of_hex(raw));
                        assert_eq!(reply_to, expected, "reply target from the e/q pair");
                    }
                    Classified::Deliver(Semantics::Reaction { target, emoji }) => {
                        assert_eq!(kind, 7);
                        assert_eq!(emoji, content);
                        let tagged = tags[0].get(1).and_then(|raw| event_id_of_hex(raw));
                        assert_eq!(Some(target), tagged);
                    }
                    Classified::Deliver(Semantics::Removal { target }) => {
                        assert_eq!(kind, 4891);
                        let tagged = tags[0].get(1).and_then(|raw| event_id_of_hex(raw));
                        assert_eq!(Some(target), tagged);
                    }
                    Classified::Deliver(Semantics::Other) => {
                        assert!(!matches!(kind, 5 | 7 | 9 | 1009 | 4891), "kind {kind}");
                    }
                    Classified::Deliver(
                        Semantics::Edit { .. }
                        | Semantics::Deletion { .. }
                        | Semantics::Unreact { .. },
                    ) => panic!("modifier semantics come from resolve, not classify"),
                    Classified::Modifier(Modifier::Edit { target, .. }) => {
                        assert_eq!(kind, 1009);
                        let tagged = tags
                            .iter()
                            .find(|tag| tag.first().map(String::as_str) == Some("e"))
                            .and_then(|tag| tag.get(1))
                            .and_then(|raw| event_id_of_hex(raw));
                        assert_eq!(Some(target), tagged);
                    }
                    Classified::Modifier(Modifier::Delete { targets }) => {
                        assert_eq!(kind, 5);
                        assert!(!targets.is_empty(), "a delete modifier has targets");
                    }
                    Classified::Drop(reason) => {
                        assert!(
                            matches!(kind, 5 | 1009),
                            "fixture kind {kind} must not drop ({reason:?})"
                        );
                        assert_eq!(reason, DropReason::Malformed);
                    }
                }
            }
        }
        for kind in [5, 7, 9, 447, 1009, 4891] {
            assert!(seen.contains_key(&kind), "fixture covers kind {kind}");
        }
    }

    /// `resolve` against a stub index: edit/deletion/unreact authorization,
    /// wrong-kind and foreign-author refusals, holds while a target is
    /// unknown, and the honoured-subset rule at a final attempt.
    #[test]
    fn resolve_table() {
        let alice = AccountId([1; 32]);
        let bob = AccountId([2; 32]);
        let index: std::collections::HashMap<EventId, TargetFacts> = [
            (
                id(0x01),
                TargetFacts {
                    author: alice,
                    kind: 9,
                },
            ),
            (
                id(0x02),
                TargetFacts {
                    author: bob,
                    kind: 9,
                },
            ),
            (
                id(0x03),
                TargetFacts {
                    author: alice,
                    kind: 7,
                },
            ),
            (
                id(0x04),
                TargetFacts {
                    author: alice,
                    kind: 1009,
                },
            ),
            (
                id(0x05),
                TargetFacts {
                    author: alice,
                    kind: 1200,
                },
            ),
        ]
        .into_iter()
        .collect();
        let lookup = |target: &EventId| index.get(target).copied();
        let edit = |target| Modifier::Edit {
            target,
            content: "new".to_owned(),
        };
        let delete = |targets| Modifier::Delete { targets };

        // Edit: authorized own-chat target delivers the content.
        assert!(matches!(
            resolve(&edit(id(0x01)), &alice, lookup, false),
            Resolution::Deliver(Semantics::Edit { target, ref content })
                if target == id(0x01) && content == "new"
        ));
        // Foreign author and wrong-kind targets are unauthorized.
        for modifier in [
            edit(id(0x02)),
            edit(id(0x05)),
            edit(id(0x03)),
            edit(id(0x04)),
        ] {
            assert!(matches!(
                resolve(&modifier, &alice, lookup, false),
                Resolution::Drop(DropReason::Unauthorized)
            ));
        }
        // Unknown target: hold, then `unverifiable` at the final attempt.
        assert!(matches!(
            resolve(&edit(id(0xaa)), &alice, lookup, false),
            Resolution::Hold
        ));
        assert!(matches!(
            resolve(&edit(id(0xaa)), &alice, lookup, true),
            Resolution::Drop(DropReason::Unverifiable)
        ));

        // Delete: all-reaction honoured set -> Unreact; any non-reaction ->
        // Deletion; a held edit voids as an honoured kind-1009 target.
        assert!(matches!(
            resolve(&delete(vec![id(0x03)]), &alice, lookup, false),
            Resolution::Deliver(Semantics::Unreact { ref reactions })
                if *reactions == vec![id(0x03)]
        ));
        assert!(matches!(
            resolve(&delete(vec![id(0x01)]), &alice, lookup, false),
            Resolution::Deliver(Semantics::Deletion { ref targets })
                if *targets == vec![id(0x01)]
        ));
        assert!(matches!(
            resolve(&delete(vec![id(0x04)]), &alice, lookup, false),
            Resolution::Deliver(Semantics::Deletion { ref targets })
                if *targets == vec![id(0x04)]
        ));
        // Mixed honoured set (reaction + edit) is a Deletion in tag order.
        assert!(matches!(
            resolve(&delete(vec![id(0x03), id(0x04)]), &alice, lookup, false),
            Resolution::Deliver(Semantics::Deletion { ref targets })
                if *targets == vec![id(0x03), id(0x04)]
        ));
        // A foreign target alone fails -> unauthorized.
        assert!(matches!(
            resolve(&delete(vec![id(0x02)]), &alice, lookup, false),
            Resolution::Drop(DropReason::Unauthorized)
        ));
        // A foreign target mixed with honoured ones is dropped from the
        // honoured set — the honoured subset still delivers.
        assert!(matches!(
            resolve(&delete(vec![id(0x02), id(0x03)]), &alice, lookup, false),
            Resolution::Deliver(Semantics::Unreact { ref reactions })
                if *reactions == vec![id(0x03)]
        ));
        // An unknown target holds the whole modifier; at a final attempt
        // the honoured subset delivers and the unknown one is omitted.
        assert!(matches!(
            resolve(&delete(vec![id(0x03), id(0xaa)]), &alice, lookup, false),
            Resolution::Hold
        ));
        assert!(matches!(
            resolve(&delete(vec![id(0xaa), id(0x03)]), &alice, lookup, true),
            Resolution::Deliver(Semantics::Unreact { ref reactions })
                if *reactions == vec![id(0x03)]
        ));
        // Honoured empty at a final attempt: `unverifiable` when only
        // unknown, `unauthorized` when a known target failed.
        assert!(matches!(
            resolve(&delete(vec![id(0xaa)]), &alice, lookup, true),
            Resolution::Drop(DropReason::Unverifiable)
        ));
        assert!(matches!(
            resolve(&delete(vec![id(0xaa), id(0x02)]), &alice, lookup, true),
            Resolution::Drop(DropReason::Unauthorized)
        ));
    }

    #[test]
    fn delete_targets_are_bounded() {
        let tags = |count: usize, distinct: usize| {
            (0..count)
                .map(|i| {
                    vec![
                        "e".to_owned(),
                        hex_id(u8::try_from(i % distinct).expect("distinct fits u8")),
                    ]
                })
                .collect::<Vec<_>>()
        };
        // 101st distinct id -> malformed; exactly 100 -> modifier; 150
        // tags over only 100 distinct ids -> still a 100-target modifier.
        assert!(matches!(
            classify(5, &tags(101, 101), "", None, None, 0),
            Classified::Drop(DropReason::Malformed)
        ));
        assert!(matches!(
            classify(5, &tags(100, 100), "", None, None, 0),
            Classified::Modifier(Modifier::Delete { ref targets }) if targets.len() == 100
        ));
        assert!(matches!(
            classify(5, &tags(150, 100), "", None, None, 0),
            Classified::Modifier(Modifier::Delete { ref targets }) if targets.len() == 100
        ));
    }

    #[test]
    fn reserved_device_kinds_drop() {
        for kind in [452u64, 460, 477] {
            assert!(
                matches!(
                    classify(kind, &[], "", None, None, 0),
                    Classified::Drop(DropReason::Reserved)
                ),
                "kind {kind} drops reserved"
            );
        }
        for kind in [451u64, 478] {
            assert!(
                matches!(
                    classify(kind, &[], "", None, None, 0),
                    Classified::Deliver(Semantics::Other)
                ),
                "kind {kind} stays untyped"
            );
        }
    }
}
