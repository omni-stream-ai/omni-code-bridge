use std::collections::HashSet;

use crate::models::{ChatMessage, MessageRole};

const EQUIVALENT_ASSISTANT_MESSAGE_WINDOW_SECONDS: i64 = 10 * 60;
const EQUIVALENT_USER_MESSAGE_WINDOW_SECONDS: i64 = 10;

pub struct MessageProjection;

impl MessageProjection {
    pub fn from_sources(
        session_id: &str,
        provider_messages: Vec<ChatMessage>,
        bridge_messages: Vec<ChatMessage>,
    ) -> Vec<ChatMessage> {
        let provider_messages = normalize_session(provider_messages, session_id);
        let bridge_messages = normalize_session(bridge_messages, session_id);
        merge_canonical_messages(provider_messages, bridge_messages)
    }

    pub fn normalize_provider(session_id: &str, messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
        let mut messages = normalize_session(messages, session_id);
        sort_messages(&mut messages);
        messages
    }
}

fn normalize_session(messages: Vec<ChatMessage>, session_id: &str) -> Vec<ChatMessage> {
    messages
        .into_iter()
        .map(|mut message| {
            message.session_id = session_id.to_string();
            message
        })
        .collect()
}

fn merge_canonical_messages(
    provider_messages: Vec<ChatMessage>,
    bridge_messages: Vec<ChatMessage>,
) -> Vec<ChatMessage> {
    let mut merged = provider_messages;
    let bridge_context = bridge_messages.clone();
    let mut seen_ids = merged
        .iter()
        .map(|message| message.id.clone())
        .collect::<HashSet<_>>();

    for bridge_message in bridge_messages {
        if let Some(existing) = merged
            .iter_mut()
            .find(|message| message.id == bridge_message.id)
        {
            if should_replace_same_id(existing, &bridge_message) {
                *existing = bridge_message;
            }
            continue;
        }

        if let Some(existing_index) = merged.iter().position(|message| {
            messages_are_equivalent_in_context(message, &bridge_message, &merged, &bridge_context)
                || is_empty_assistant_placeholder_for_same_turn(
                    message,
                    &bridge_message,
                    &merged,
                    &bridge_context,
                )
                || is_final_assistant_reply_for_same_turn(
                    message,
                    &bridge_message,
                    &merged,
                    &bridge_context,
                )
        }) {
            let existing = &mut merged[existing_index];
            *existing = merge_equivalent_message(existing, &bridge_message);
            continue;
        }

        if seen_ids.insert(bridge_message.id.clone()) {
            merged.push(bridge_message);
        }
    }

    sort_messages(&mut merged);
    merged
}

fn is_empty_assistant_placeholder_for_same_turn(
    provider_message: &ChatMessage,
    bridge_message: &ChatMessage,
    provider_context: &[ChatMessage],
    bridge_context: &[ChatMessage],
) -> bool {
    if provider_message.role != MessageRole::Assistant
        || bridge_message.role != MessageRole::Assistant
        || provider_message.session_id != bridge_message.session_id
        || !bridge_message.content.trim().is_empty()
        || provider_message.content.trim().is_empty()
    {
        return false;
    }

    let Some(provider_user) = nearest_user_before(provider_context, provider_message) else {
        return false;
    };
    let Some(bridge_user) = nearest_user_before(bridge_context, bridge_message) else {
        return false;
    };
    provider_user.session_id == bridge_user.session_id
        && provider_user.content.trim() == bridge_user.content.trim()
}

fn is_final_assistant_reply_for_same_turn(
    provider_message: &ChatMessage,
    bridge_message: &ChatMessage,
    provider_context: &[ChatMessage],
    bridge_context: &[ChatMessage],
) -> bool {
    if provider_message.role != MessageRole::Assistant
        || bridge_message.role != MessageRole::Assistant
        || provider_message.session_id != bridge_message.session_id
        || provider_message.content.trim().is_empty()
        || bridge_message.content.trim().is_empty()
        || !is_last_assistant_in_turn(provider_context, provider_message)
        || !is_last_assistant_in_turn(bridge_context, bridge_message)
    {
        return false;
    }

    let Some(provider_user) = nearest_user_before(provider_context, provider_message) else {
        return false;
    };
    let Some(bridge_user) = nearest_user_before(bridge_context, bridge_message) else {
        return false;
    };
    provider_user.session_id == bridge_user.session_id
        && provider_user.content.trim() == bridge_user.content.trim()
}

fn is_last_assistant_in_turn(messages: &[ChatMessage], target: &ChatMessage) -> bool {
    let Some(index) = messages.iter().position(|message| message.id == target.id) else {
        return false;
    };
    messages[index + 1..]
        .iter()
        .all(|message| message.role == MessageRole::System || message.role == MessageRole::User)
}

fn should_replace_same_id(existing: &ChatMessage, incoming: &ChatMessage) -> bool {
    incoming.content.len() >= existing.content.len()
}

fn merge_equivalent_message(existing: &ChatMessage, incoming: &ChatMessage) -> ChatMessage {
    let content = if incoming.content.len() >= existing.content.len() {
        incoming.content.clone()
    } else {
        existing.content.clone()
    };
    ChatMessage {
        id: incoming.id.clone(),
        session_id: incoming.session_id.clone(),
        role: incoming.role.clone(),
        content,
        created_at: incoming.created_at,
    }
}

fn messages_are_equivalent(a: &ChatMessage, b: &ChatMessage) -> bool {
    if a.role != b.role {
        return false;
    }

    let a_content = a.content.trim();
    let b_content = b.content.trim();
    if a_content.is_empty() || b_content.is_empty() {
        return false;
    }
    if !contents_are_equivalent(&a.role, a_content, b_content) {
        return false;
    }

    let window_seconds = match a.role {
        MessageRole::User => EQUIVALENT_USER_MESSAGE_WINDOW_SECONDS,
        MessageRole::Assistant => EQUIVALENT_ASSISTANT_MESSAGE_WINDOW_SECONDS,
        MessageRole::System => return false,
    };

    a.created_at
        .signed_duration_since(b.created_at)
        .num_seconds()
        .abs()
        <= window_seconds
}

fn messages_are_equivalent_in_context(
    provider_message: &ChatMessage,
    bridge_message: &ChatMessage,
    provider_context: &[ChatMessage],
    bridge_context: &[ChatMessage],
) -> bool {
    if messages_are_equivalent(provider_message, bridge_message) {
        return true;
    }
    if provider_message.role != MessageRole::Assistant
        || bridge_message.role != MessageRole::Assistant
        || provider_message.session_id != bridge_message.session_id
    {
        return false;
    }
    let provider_content = provider_message.content.trim();
    let bridge_content = bridge_message.content.trim();
    if provider_content.is_empty()
        || bridge_content.is_empty()
        || !assistant_contents_have_prefix(provider_content, bridge_content)
    {
        return false;
    }

    let Some(provider_user) = nearest_user_before(provider_context, provider_message) else {
        return false;
    };
    let Some(bridge_user) = nearest_user_before(bridge_context, bridge_message) else {
        return false;
    };
    provider_user.session_id == bridge_user.session_id
        && provider_user.content.trim() == bridge_user.content.trim()
}

fn contents_are_equivalent(role: &MessageRole, a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    if !matches!(role, MessageRole::Assistant) {
        return false;
    }

    let shorter = if a.len() <= b.len() { a } else { b };
    let longer = if a.len() <= b.len() { b } else { a };
    if shorter.len() < 16 {
        return false;
    }
    longer.starts_with(shorter) && shorter.len() * 100 >= longer.len() * 35
}

fn assistant_contents_have_prefix(a: &str, b: &str) -> bool {
    let shorter = if a.len() <= b.len() { a } else { b };
    let longer = if a.len() <= b.len() { b } else { a };
    shorter.len() >= 16 && longer.starts_with(shorter)
}

fn nearest_user_before<'a>(
    messages: &'a [ChatMessage],
    target: &ChatMessage,
) -> Option<&'a ChatMessage> {
    let mut latest_user = None;
    for message in messages {
        if message.id == target.id {
            return latest_user;
        }
        if message.role == MessageRole::User {
            latest_user = Some(message);
        }
    }
    None
}

fn sort_messages(messages: &mut [ChatMessage]) {
    messages.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, Utc};

    use super::MessageProjection;
    use crate::models::{ChatMessage, MessageRole};

    #[test]
    fn projection_normalizes_provider_session_ids() {
        let now = Utc::now();
        let projected = MessageProjection::normalize_provider(
            "local-session",
            vec![message(
                "remote-user",
                "provider-thread",
                MessageRole::User,
                "hello",
                now,
            )],
        );

        assert_eq!(projected[0].session_id, "local-session");
    }

    #[test]
    fn projection_deduplicates_equivalent_assistant_messages() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![message(
                "remote-assistant",
                "provider-thread",
                MessageRole::Assistant,
                "same reply",
                now,
            )],
            vec![message(
                "bridge-assistant",
                "session",
                MessageRole::Assistant,
                "same reply",
                now + TimeDelta::seconds(3),
            )],
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, "bridge-assistant");
    }

    #[test]
    fn projection_deduplicates_equivalent_user_messages() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![message(
                "remote-user",
                "provider-thread",
                MessageRole::User,
                "continue",
                now,
            )],
            vec![message(
                "bridge-user",
                "session",
                MessageRole::User,
                "continue",
                now + TimeDelta::seconds(3),
            )],
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, "bridge-user");
        assert_eq!(projected[0].created_at, now + TimeDelta::seconds(3));
    }

    #[test]
    fn projection_keeps_same_content_outside_equivalence_window() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![message(
                "remote-assistant",
                "provider-thread",
                MessageRole::Assistant,
                "same reply",
                now,
            )],
            vec![message(
                "bridge-assistant",
                "session",
                MessageRole::Assistant,
                "same reply",
                now + TimeDelta::minutes(30),
            )],
        );

        assert_eq!(projected.len(), 2);
    }

    #[test]
    fn projection_deduplicates_prefix_assistant_messages() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![message(
                "remote-assistant",
                "provider-thread",
                MessageRole::Assistant,
                "I will inspect the workspace and then commit.",
                now,
            )],
            vec![message(
                "bridge-assistant",
                "session",
                MessageRole::Assistant,
                "I will inspect the workspace and then commit. Current branch is feat/desktop.",
                now + TimeDelta::seconds(3),
            )],
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, "bridge-assistant");
        assert_eq!(
            projected[0].content,
            "I will inspect the workspace and then commit. Current branch is feat/desktop."
        );
    }

    #[test]
    fn projection_merges_empty_bridge_reply_placeholder_with_provider_final_reply() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![
                message(
                    "remote-user",
                    "provider-thread",
                    MessageRole::User,
                    "inspect the workspace",
                    now,
                ),
                message(
                    "remote-assistant",
                    "provider-thread",
                    MessageRole::Assistant,
                    "I inspected the workspace and found two changed files.",
                    now + TimeDelta::seconds(2),
                ),
            ],
            vec![
                message(
                    "bridge-user",
                    "session",
                    MessageRole::User,
                    "inspect the workspace",
                    now + TimeDelta::seconds(1),
                ),
                message(
                    "bridge-assistant",
                    "session",
                    MessageRole::Assistant,
                    "",
                    now + TimeDelta::seconds(1),
                ),
            ],
        );

        assert_eq!(projected.len(), 2);
        let assistant = projected
            .iter()
            .find(|message| message.role == MessageRole::Assistant)
            .expect("assistant reply should exist");
        assert_eq!(assistant.id, "bridge-assistant");
        assert_eq!(
            assistant.content,
            "I inspected the workspace and found two changed files."
        );
    }

    #[test]
    fn projection_merges_non_prefix_bridge_stream_with_provider_final_reply() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![
                message(
                    "remote-user",
                    "provider-thread",
                    MessageRole::User,
                    "inspect the workspace",
                    now,
                ),
                message(
                    "remote-assistant",
                    "provider-thread",
                    MessageRole::Assistant,
                    "I found two changed files and one untracked directory.",
                    now + TimeDelta::seconds(2),
                ),
            ],
            vec![
                message(
                    "bridge-user",
                    "session",
                    MessageRole::User,
                    "inspect the workspace",
                    now + TimeDelta::seconds(1),
                ),
                message(
                    "bridge-assistant",
                    "session",
                    MessageRole::Assistant,
                    "Looking through the repository now.",
                    now + TimeDelta::seconds(1),
                ),
            ],
        );

        assert_eq!(projected.len(), 2);
        let assistant = projected
            .iter()
            .find(|message| message.role == MessageRole::Assistant)
            .expect("assistant reply should exist");
        assert_eq!(assistant.id, "bridge-assistant");
        assert_eq!(
            assistant.content,
            "I found two changed files and one untracked directory."
        );
    }

    #[test]
    fn projection_preserves_bridge_assistant_id_when_provider_content_is_longer() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![message(
                "remote-assistant",
                "provider-thread",
                MessageRole::Assistant,
                "I will inspect the workspace and then commit. Current branch is feat/desktop.",
                now + TimeDelta::seconds(3),
            )],
            vec![message(
                "bridge-assistant",
                "session",
                MessageRole::Assistant,
                "I will inspect the workspace and then commit.",
                now,
            )],
        );

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, "bridge-assistant");
        assert_eq!(
            projected[0].content,
            "I will inspect the workspace and then commit. Current branch is feat/desktop."
        );
        assert_eq!(projected[0].created_at, now);
    }

    #[test]
    fn projection_merges_same_turn_assistant_when_provider_timestamp_drifts() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![
                message(
                    "remote-user",
                    "provider-thread",
                    MessageRole::User,
                    "Resource ID 应该填什么",
                    now - TimeDelta::minutes(30),
                ),
                message(
                    "remote-assistant",
                    "provider-thread",
                    MessageRole::Assistant,
                    "I checked the resource id and found the latest setting.",
                    now - TimeDelta::minutes(30) + TimeDelta::seconds(3),
                ),
            ],
            vec![
                message(
                    "bridge-user",
                    "session",
                    MessageRole::User,
                    "Resource ID 应该填什么",
                    now,
                ),
                message(
                    "bridge-assistant",
                    "session",
                    MessageRole::Assistant,
                    "I checked the resource id",
                    now + TimeDelta::seconds(1),
                ),
            ],
        );

        let assistants = projected
            .iter()
            .filter(|message| message.role == MessageRole::Assistant)
            .collect::<Vec<_>>();
        assert_eq!(assistants.len(), 1);
        assert_eq!(assistants[0].id, "bridge-assistant");
        assert_eq!(assistants[0].created_at, now + TimeDelta::seconds(1));
        assert_eq!(
            assistants[0].content,
            "I checked the resource id and found the latest setting."
        );
    }

    #[test]
    fn projection_keeps_prefix_user_messages_distinct() {
        let now = Utc::now();
        let projected = MessageProjection::from_sources(
            "session",
            vec![message(
                "remote-user",
                "provider-thread",
                MessageRole::User,
                "please inspect the workspace",
                now,
            )],
            vec![message(
                "bridge-user",
                "session",
                MessageRole::User,
                "please inspect the workspace and push",
                now + TimeDelta::seconds(3),
            )],
        );

        assert_eq!(projected.len(), 2);
    }

    fn message(
        id: &str,
        session_id: &str,
        role: MessageRole,
        content: &str,
        created_at: chrono::DateTime<Utc>,
    ) -> ChatMessage {
        ChatMessage {
            id: id.to_string(),
            session_id: session_id.to_string(),
            role,
            content: content.to_string(),
            created_at,
        }
    }
}
