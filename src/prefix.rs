//! Prefix templating and payload fitting.
//!
//! Mesh payloads are tight, so a relayed message is a prefix identifying the
//! origin plus as much of the body as fits. On MeshCore this is not merely
//! cosmetic: channel messages carry no sender identity, so the prefix *is* the
//! only identity the receiving client will ever see.

/// Values available to a prefix template.
#[derive(Clone, Copy, Debug)]
pub struct Vars<'a> {
    /// Display name of the sender.
    pub name: &'a str,
    /// Configured network name.
    pub network: &'a str,
    /// Pre-rendered network marker, empty when naming the network would be
    /// redundant. Used by DM inboxes that listen to a single network.
    pub network_tag: &'a str,
    /// Channel index, or `None` for direct messages.
    pub channel: Option<u8>,
}

/// Expand `{placeholder}` occurrences in `template`.
///
/// Supported placeholders: `{name}`, `{network}`, `{network_tag}` and
/// `{channel}`. `{name}` and `{network}` accept a trailing byte budget, so
/// `{name5}` yields at most five bytes of the name. An unknown placeholder is
/// left verbatim rather than silently dropped, so typos are visible instead of
/// producing anonymous messages.
pub fn render(template: &str, vars: &Vars<'_>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        rest = &rest[start..];

        let Some(end) = rest.find('}') else {
            // Unterminated brace: emit the remainder as-is.
            break;
        };
        let token = &rest[1..end];
        match expand(token, vars) {
            Some(value) => out.push_str(&value),
            None => out.push_str(&rest[..=end]),
        }
        rest = &rest[end + 1..];
    }

    out.push_str(rest);
    out
}

fn expand(token: &str, vars: &Vars<'_>) -> Option<String> {
    let (key, budget) = split_budget(token);
    let value = match key {
        "name" => vars.name,
        "network" => vars.network,
        "network_tag" => vars.network_tag,
        "channel" => return Some(vars.channel.map_or(String::new(), |ch| ch.to_string())),
        _ => return None,
    };
    Some(match budget {
        Some(max) => truncate(value, max).to_owned(),
        None => value.to_owned(),
    })
}

/// Split a trailing byte budget off a placeholder name: `name5` -> `("name", 5)`.
fn split_budget(token: &str) -> (&str, Option<usize>) {
    let digits = token.len() - token.trim_end_matches(|c: char| c.is_ascii_digit()).len();
    if digits == 0 {
        return (token, None);
    }
    let (key, budget) = token.split_at(token.len() - digits);
    (key, budget.parse().ok())
}

/// Longest prefix of `text` that fits in `max_bytes` without splitting a
/// character.
pub fn truncate(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Join a prefix and a body into a payload of at most `max_bytes`, spending the
/// budget on the prefix first so the origin is never lost.
pub fn fit(prefix: &str, body: &str, max_bytes: usize) -> String {
    let prefix = truncate(prefix, max_bytes);
    let remaining = max_bytes - prefix.len();
    let mut out = String::with_capacity(prefix.len() + remaining);
    out.push_str(prefix);
    out.push_str(truncate(body, remaining));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> Vars<'static> {
        Vars {
            name: "Alice",
            network: "lora-casa",
            network_tag: "[lora-casa]",
            channel: Some(2),
        }
    }

    #[test]
    fn expands_the_documented_placeholders() {
        assert_eq!(
            render("[{name}/{network}]: ", &vars()),
            "[Alice/lora-casa]: "
        );
        assert_eq!(render("ch{channel} ", &vars()), "ch2 ");
        assert_eq!(render("[DM]{network_tag} ", &vars()), "[DM][lora-casa] ");
    }

    #[test]
    fn applies_a_trailing_byte_budget() {
        assert_eq!(render("{name5}[M]: ", &vars()), "Alice[M]: ");
        assert_eq!(render("{name3}[M]: ", &vars()), "Ali[M]: ");
        assert_eq!(render("{network4}", &vars()), "lora");
    }

    #[test]
    fn leaves_unknown_placeholders_visible() {
        assert_eq!(render("{nope}/{name}", &vars()), "{nope}/Alice");
        assert_eq!(render("{unterminated", &vars()), "{unterminated");
    }

    #[test]
    fn renders_empty_channel_for_direct_messages() {
        let direct = Vars {
            channel: None,
            ..vars()
        };
        assert_eq!(render("ch{channel}", &direct), "ch");
    }

    #[test]
    fn never_splits_a_multibyte_character() {
        // "ñ" is two bytes, so a three-byte budget must stop after the first.
        assert_eq!(truncate("añb", 3), "añ");
        assert_eq!(truncate("añb", 2), "a");
        // A four-byte emoji does not fit in three bytes at all.
        assert_eq!(truncate("🦘", 3), "");
        assert_eq!(truncate("🦘", 4), "🦘");
    }

    #[test]
    fn truncate_is_a_noop_when_it_already_fits() {
        assert_eq!(truncate("hola", 10), "hola");
        assert_eq!(truncate("hola", 4), "hola");
    }

    #[test]
    fn fit_spends_the_budget_on_the_prefix_first() {
        assert_eq!(fit("[A]: ", "hola mundo", 9), "[A]: hola");
        assert_eq!(fit("[A]: ", "hola", 100), "[A]: hola");
    }

    #[test]
    fn fit_truncates_an_oversized_prefix_rather_than_panicking() {
        assert_eq!(fit("[very-long-prefix]: ", "body", 5), "[very");
        assert_eq!(fit("prefix", "body", 0), "");
    }
}
