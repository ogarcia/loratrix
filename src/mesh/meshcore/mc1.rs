//! Reactions, as MeshCore clients write them.
//!
//! The protocol has none: the firmware knows about text messages and nothing
//! else, and a reaction is an ordinary one whose text follows a convention —
//! the same trick that already carries the sender's name and a reply's target.
//! What the convention says is
//!
//! ```text
//! @[ALICE]👍
//! b45pc4ek
//! ```
//!
//! a marker naming who wrote the message being reacted to, the emoji, and then
//! a fingerprint of that message. The fingerprint is the part that matters: it
//! is what makes the reaction point at one message rather than at a person, and
//! it is computed from what everybody on the channel already has — the text and
//! the timestamp the sender stamped on it.
//!
//! Within a direct message there is nobody to name, so the marker goes and the
//! text is the emoji and the fingerprint alone.
//!
//! This is the convention of **MeshCore One**, the Apple client, which is what
//! the mesh this was written against uses. Others exist — `meshcore-open` sends
//! `r:{hash}:{index}` against an emoji table of its own — and are left alone
//! until one turns up on the air.
//!
//! The format is described in MeshMonitor's notes, which were written from
//! observed behaviour rather than from MC1's source, and the published test
//! vectors below are what pin this implementation to it.

/// Crockford's base32 alphabet: the digits and the letters, less `i`, `l`, `o`
/// and `u`, so that a fingerprint cannot be misread aloud or mistyped into a
/// different one.
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// How many characters a fingerprint is: forty bits, five per character.
const FINGERPRINT_LEN: usize = 8;

/// Longest thing we will consider a single emoji. A family with four people in
/// it is seven scalars; past twice that it is text.
const MAX_EMOJI_SCALARS: usize = 16;

/// Where a reaction points, and what it says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reaction {
    /// The emoji, which is the whole of the message as far as Matrix is
    /// concerned.
    pub emoji: String,
    /// The fingerprint of the message being reacted to, normalized.
    pub about: String,
}

/// The part of a channel message a fingerprint is taken over: everything after
/// the first colon, less the spaces and tabs at either end.
///
/// That is the body without the `NAME: ` a client prepends. It is deliberately
/// *not* the split [`super::split_sender`] does — that one wants a name it can
/// believe and gives up when the shape is odd, while this one has to agree
/// byte for byte with whatever the sending client did, which was to cut at the
/// first colon and look no further.
pub fn body(wire: &str) -> &str {
    let rest = match wire.find(':') {
        Some(colon) => &wire[colon + 1..],
        None => wire,
    };
    rest.trim_matches([' ', '\t'])
}

/// Fingerprint a message: the first forty bits of `SHA-256(text ‖ timestamp)`,
/// written in [`ALPHABET`].
///
/// The timestamp is the one the sending client put on the packet, in seconds,
/// little endian — which is also the only reason two radios agree on the
/// answer.
pub fn fingerprint(text: &str, timestamp: u32) -> String {
    let mut input = Vec::with_capacity(text.len() + 4);
    input.extend_from_slice(text.as_bytes());
    input.extend_from_slice(&timestamp.to_le_bytes());

    let digest = sha256(&input);
    let bits = u64::from_be_bytes([
        0, 0, 0, digest[0], digest[1], digest[2], digest[3], digest[4],
    ]);
    (0..FINGERPRINT_LEN)
        .map(|i| ALPHABET[((bits >> (35 - 5 * i)) & 0x1f) as usize] as char)
        .collect()
}

/// Read a channel reaction, or decide this is an ordinary message.
///
/// Two orders are accepted because MC1 changed its mind: the marker comes
/// first now, and came second before 1.4.1. Both are unambiguous, since one
/// ends where the other begins.
pub fn parse_channel(text: &str) -> Option<Reaction> {
    let (head, about) = split_fingerprint(text)?;
    let marker = head.find(super::REPLY_OPEN)?;

    let (author, emoji) = if marker == 0 {
        let close = head.find(']')?;
        (&head[super::REPLY_OPEN.len()..close], &head[close + 1..])
    } else {
        // The older order, with the emoji in front. Nothing may follow the
        // marker there, or this is a sentence that merely mentions somebody.
        let rest = head[marker + super::REPLY_OPEN.len()..].strip_suffix(']')?;
        (rest, &head[..marker])
    };

    (!author.is_empty() && is_emoji(emoji)).then(|| Reaction {
        emoji: emoji.to_owned(),
        about,
    })
}

/// Read a reaction to a direct message, where there is no marker: the two
/// people in the conversation already know who wrote what.
pub fn parse_direct(text: &str) -> Option<Reaction> {
    if text.contains(super::REPLY_OPEN) {
        return None;
    }
    let (emoji, about) = split_fingerprint(text)?;
    is_emoji(emoji).then(|| Reaction {
        emoji: emoji.to_owned(),
        about,
    })
}

/// Cut the trailing fingerprint off, giving what came before it.
///
/// The separator is a newline. It is also sometimes a space: a channel message
/// of our own can come back from the radio with the newline flattened, and a
/// reaction that reads as text on the way out would be a poor trade for
/// strictness. Nothing else is accepted, and the head still has to look like a
/// reaction afterwards.
fn split_fingerprint(text: &str) -> Option<(&str, String)> {
    let at = match text.rfind('\n') {
        Some(at) => at,
        // Only where a marker says this is a channel reaction, so that an
        // ordinary line ending in eight good characters is left alone.
        None if text.contains(super::REPLY_OPEN) => text.rfind(' ')?,
        None => return None,
    };
    Some((&text[..at], normalize(&text[at + 1..])?))
}

/// Fold a fingerprint to the one form, or refuse it.
///
/// Crockford's alphabet leaves out the characters that are read as each other,
/// and says to accept them on the way in: an `O` is a zero, an `I` or an `L` is
/// a one. A `u` is left out to keep accidents out of the alphabet and is not
/// accepted as anything.
fn normalize(text: &str) -> Option<String> {
    if text.len() != FINGERPRINT_LEN {
        return None;
    }
    text.chars()
        .map(|c| match c.to_ascii_lowercase() {
            'o' => Some('0'),
            'i' | 'l' => Some('1'),
            c if c.is_ascii_digit() || (c.is_ascii_lowercase() && c != 'u') => Some(c),
            _ => None,
        })
        .collect()
}

/// Whether this is one emoji and nothing else.
///
/// Properly this asks whether the text is a single grapheme cluster whose first
/// scalar is Emoji, which needs two Unicode tables we would otherwise have no
/// use for. What it asks instead is that nothing in it is ASCII — which no
/// emoji is, bar the keycaps — and that it is either above the symbols block or
/// made of more than one scalar, the rule that keeps a bare `©` or `™` from
/// counting.
///
/// Erring either way is cheap here: a reaction we fail to recognize arrives as
/// the text it is, and a message mistaken for one has to end in a valid
/// fingerprint and carry a marker as well.
fn is_emoji(text: &str) -> bool {
    if text.is_empty() || text.chars().count() > MAX_EMOJI_SCALARS {
        return false;
    }
    if is_keycap(text) {
        return true;
    }
    let mut chars = text.chars();
    let first = chars.next().expect("not empty");
    !text.chars().any(|c| c.is_ascii()) && (first > '\u{238c}' || chars.count() > 0)
}

/// The one emoji with ASCII in it: a digit, `#` or `*`, dressed as a key.
fn is_keycap(text: &str) -> bool {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_digit() && first != '#' && first != '*' {
        return false;
    }
    let rest: String = chars.collect();
    rest == "\u{fe0f}\u{20e3}" || rest == "\u{20e3}"
}

/// SHA-256, by the book (FIPS 180-4).
///
/// Written out here rather than taken from a crate: it is the only hash this
/// program needs, it is used to fingerprint a message everyone on the channel
/// can already read rather than to keep a secret, and the alternative was seven
/// dependencies. The vectors in the tests are what say it is right.
fn sha256(message: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // The message, a one bit, zeroes, and its length in bits — padded to whole
    // blocks, which is what lets the loop below assume there are no short ones.
    let mut padded = Vec::with_capacity(message.len() + 72);
    padded.extend_from_slice(message);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&(message.len() as u64 * 8).to_be_bytes());

    for block in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (word, bytes) in w.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().expect("four bytes"));
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for (k, w) in K.iter().zip(w) {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ (!e & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(choose)
                .wrapping_add(*k)
                .wrapping_add(w);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(majority);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (acc, add) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *acc = acc.wrapping_add(add);
        }
    }

    let mut digest = [0u8; 32];
    for (bytes, word) in digest.chunks_exact_mut(4).zip(state) {
        bytes.copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// FIPS 180-4's own answers. Everything else here rests on these.
    #[test]
    fn sha256_matches_the_standard() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // Long enough to need a second block, which the padding gets wrong in
        // its own particular way if it is wrong at all.
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    /// The vectors published with the format. If these pass, a reaction sent by
    /// a real client names a message we can find.
    #[test]
    fn fingerprints_match_the_published_vectors() {
        for (text, timestamp, expected) in [
            ("Hello", 1_704_067_200, "b45pc4ek"),
            ("Hello world", 1_704_067_200, "7a6r3xy1"),
            ("Hello", 1_704_067_201, "3wh8x8z5"),
            ("World", 1_704_067_200, "vp1vxnf4"),
            ("Message 0", 0, "7mt5pe1m"),
            ("café 👍 ok", 1_760_000_000, "8b5qzwf6"),
            ("", 4_294_967_295, "npah66y0"),
        ] {
            assert_eq!(fingerprint(text, timestamp), expected, "{text:?}");
        }
    }

    #[test]
    fn a_body_is_what_follows_the_first_colon() {
        assert_eq!(body("ALICE: hola a todos"), "hola a todos");
        // The sender's own colons belong to the message.
        assert_eq!(body("ALICE: mira: esto"), "mira: esto");
        // Tabs and spaces go, anything else stays.
        assert_eq!(body("ALICE:\thola \t"), "hola");
        // No prefix at all: the message is the whole of it.
        assert_eq!(body("hola a todos"), "hola a todos");
    }

    #[test]
    fn reads_a_channel_reaction() {
        assert_eq!(
            parse_channel("@[ALICE]😂\nb45pc4ek"),
            Some(Reaction {
                emoji: "😂".to_owned(),
                about: "b45pc4ek".to_owned(),
            })
        );
    }

    /// The order MC1 used before 1.4.1, still on the air wherever nobody has
    /// updated.
    #[test]
    fn reads_the_older_order_too() {
        assert_eq!(
            parse_channel("😂@[ALICE]\nb45pc4ek"),
            Some(Reaction {
                emoji: "😂".to_owned(),
                about: "b45pc4ek".to_owned(),
            })
        );
    }

    /// Crockford says to read back what a person might have written down.
    #[test]
    fn a_fingerprint_is_read_in_either_case_and_either_spelling() {
        let folded = parse_channel("@[ALICE]😂\nB45PC4EK").map(|r| r.about);
        assert_eq!(folded.as_deref(), Some("b45pc4ek"));

        // O for zero, I and L for one.
        let spelled = parse_channel("@[ALICE]😂\nnpaOI66y").map(|r| r.about);
        assert_eq!(spelled.as_deref(), Some("npa0166y"));
    }

    /// Seen in the wild on our own echo, and worth tolerating: the same text
    /// with the newline flattened into a space.
    #[test]
    fn tolerates_a_space_where_the_newline_should_be() {
        assert_eq!(
            parse_channel("@[ALICE]😂 b45pc4ek").map(|r| r.emoji),
            Some("😂".to_owned())
        );
    }

    #[test]
    fn reads_a_direct_reaction() {
        assert_eq!(
            parse_direct("👍\nb45pc4ek"),
            Some(Reaction {
                emoji: "👍".to_owned(),
                about: "b45pc4ek".to_owned(),
            })
        );
    }

    #[test]
    fn an_ordinary_message_is_not_a_reaction() {
        // No fingerprint.
        assert_eq!(parse_channel("@[ALICE] hola Alice"), None);
        // A fingerprint, but words where the emoji should be.
        assert_eq!(parse_channel("@[ALICE]vale\nb45pc4ek"), None);
        // Eight characters that are not a fingerprint: `u` is not in the
        // alphabet.
        assert_eq!(parse_channel("@[ALICE]😂\nb45pc4eu"), None);
        // Nobody named.
        assert_eq!(parse_channel("@[]😂\nb45pc4ek"), None);
        // Not eight characters.
        assert_eq!(parse_direct("👍\nb45pc4e"), None);
        // A direct message naming somebody is a message, whatever else it looks
        // like.
        assert_eq!(parse_direct("@[ALICE]👍\nb45pc4ek"), None);
        // And a plain line that happens to end in eight good characters.
        assert_eq!(parse_direct("hasta luego\nb45pc4ek"), None);
    }

    #[test]
    fn knows_an_emoji_from_a_word() {
        assert!(is_emoji("😂"));
        assert!(is_emoji("👍🏽"), "a skin tone is part of the emoji");
        assert!(is_emoji("👨‍👩‍👧‍👦"), "and so is a family");
        assert!(is_emoji("🇪🇸"), "a flag is two scalars");
        assert!(is_emoji("❤️"), "a heart needs its variation selector");
        assert!(is_emoji("1️⃣"), "a keycap is the one with ASCII in it");

        assert!(!is_emoji(""));
        assert!(!is_emoji("ok"));
        assert!(!is_emoji("1"), "a bare digit is a digit");
        assert!(!is_emoji("hola 😂"), "not on its own");
    }
}
