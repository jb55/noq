use noq::{Error, NoteBuilder, NoteRef, build_note};
use secp256k1::Keypair;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Checks every field of a built note against serde_json's reading of the
/// same json.
fn assert_round_trip(raw: &[u8]) {
    let index = build_note(raw).unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(raw)));
    let note = NoteRef::new(&index, raw).unwrap();
    let v: Value = serde_json::from_slice(raw).unwrap();
    assert_eq!(hex(note.id()), v["id"].as_str().unwrap());
    assert_eq!(hex(note.pubkey()), v["pubkey"].as_str().unwrap());
    assert_eq!(note.created_at(), v["created_at"].as_u64().unwrap());
    assert_eq!(note.kind() as u64, v["kind"].as_u64().unwrap());
    assert_eq!(note.content(), v["content"].as_str().unwrap());
    assert_eq!(note.json(), raw);

    let tags = v["tags"].as_array().unwrap();
    assert_eq!(note.ntags(), tags.len());
    for (tag, want) in note.tags().zip(tags) {
        let want = want.as_array().unwrap();
        assert_eq!(tag.len(), want.len());
        for (got, want) in tag.iter().zip(want) {
            assert_eq!(got, want.as_str().unwrap().as_bytes());
        }
    }
}

fn lines(path: &str) -> Vec<Vec<u8>> {
    std::fs::read(path)
        .unwrap()
        .split(|&c| c == b'\n')
        .filter(|l| !l.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

#[test]
fn round_trips_nostrdb_test_events() {
    let mut n = 0;
    for file in [
        "many-events-sample",
        "old-new",
        "profile-updates",
        "random",
        "reactions",
    ] {
        for raw in lines(&format!("tests/data/{file}.jsonl")) {
            assert_round_trip(&raw);
            n += 1;
        }
    }
    assert_eq!(n, 817);
}

#[test]
fn round_trips_pretty_printed_contact_list() {
    let raw = std::fs::read("tests/data/contacts-event.json").unwrap();
    assert!(raw.contains(&b'\n'));
    assert_round_trip(&raw);
    let index = build_note(&raw).unwrap();
    let note = NoteRef::new(&index, &raw).unwrap();
    assert_eq!(note.kind(), 3);
    assert!(note.ntags() > 100);
}

#[test]
fn real_escaped_tag_values_unescape_into_side() {
    // From nostrdb's many-events.json: tags [["e","\""],["p","+"]].
    let raw = br#"{"id":"f593e379859d386e640a562ddf6550e127ff71188cb5136e563f68da6c30ade1","pubkey":"9ec7a778167afb1d30c4833de9322da0c08ba71a69e1911d5578d3144bb56437","created_at":1662181950,"kind":7,"tags":[["e","\""],["p","+"]],"content":"[","sig":"f3f231df1c957982dcf3837f30adebaf718fa855af1c4d66f3cd9fa8e21f60543eb3a54bcc0aeffc58a784af922d11548af98d9095540cb1b87adac9c3fe73be"}"#;
    let index = build_note(raw).unwrap();
    let note = NoteRef::new(&index, raw).unwrap();
    assert_eq!(note.tag(0).unwrap().get(1).unwrap(), b"\"");
    assert_eq!(note.tag(1).unwrap().get(1).unwrap(), b"+");
    // head + 3 tag starts + 4 spans + one side byte
    assert_eq!(index.len(), noq::HEAD_LEN + 3 * 2 + 4 * 8 + 1);
}

/// Signs an event, computing its id from serde_json's serialization of the
/// canonical array (it matches NIP-01 for strings without control
/// characters other than \n \r \t \b \f), independent of noq's own.
fn sign(created_at: u64, kind: u32, tags: Value, content: &str) -> Value {
    let kp = Keypair::from_secret_bytes([7; 32]).unwrap();
    let pubkey = hex(&kp.x_only_public_key().0.to_byte_array());
    let canon =
        serde_json::to_string(&json!([0, pubkey, created_at, kind, tags, content])).unwrap();
    let id: [u8; 32] = Sha256::digest(canon.as_bytes()).into();
    let sig = kp.sign_schnorr_no_aux_rand(&id);
    json!({
        "id": hex(&id),
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": hex(&sig.to_byte_array()),
    })
}

fn build(raw: &str) -> Result<Vec<u8>, Error> {
    build_note(raw.as_bytes())
}

#[test]
fn escaped_strings() {
    let content = "line one\nline \"two\"\t\\ end ünïcode 😀";
    let ev = sign(
        1700000000,
        1,
        json!([
            ["t", "plain"],
            ["q", "quote \" and \\ and\nnewline"],
            ["emoji", "😀", "é"]
        ]),
        content,
    );
    let raw = ev.to_string();
    assert_round_trip(raw.as_bytes());

    let index = build(&raw).unwrap();
    let note = NoteRef::new(&index, raw.as_bytes()).unwrap();
    assert_eq!(note.content(), content);
    assert_eq!(
        note.content_raw(),
        r#"line one\nline \"two\"\t\\ end ünïcode 😀"#.as_bytes()
    );
    // "plain" points into the json; the escaped value is copied to Side.
    let plain = note.tag(0).unwrap().get(1).unwrap();
    let plain_at = plain.as_ptr() as usize - raw.as_ptr() as usize;
    assert!(plain_at < raw.len());
    assert_eq!(
        note.tag(1).unwrap().str(1).unwrap(),
        "quote \" and \\ and\nnewline"
    );
}

#[test]
fn equivalent_escape_forms_parse_to_the_same_values() {
    // The signature covers the canonical form; the json may spell the same
    // strings with \u escapes, \/ and raw non-ascii.
    let ev = sign(
        1700000000,
        1,
        json!([["r", "https://a/b"], ["t", "é😀"]]),
        "é/😀",
    );
    let raw = ev.to_string();
    let spelled = raw
        .replace("https://a/b", r"https:\/\/a\/b")
        .replace("\"é😀\"", r#""\u00e9\ud83d\ude00""#)
        .replace("\"é/😀\"", r#""\u00E9\/\uD83D\uDE00""#);
    assert_ne!(raw, spelled);
    let index = build(&spelled).unwrap();
    let note = NoteRef::new(&index, spelled.as_bytes()).unwrap();
    assert_eq!(note.tag(0).unwrap().str(1).unwrap(), "https://a/b");
    assert_eq!(note.tag(1).unwrap().str(1).unwrap(), "é😀");
    assert_eq!(note.content(), "é/😀");
}

#[test]
fn empty_tags() {
    for tags in [json!([]), json!([[]]), json!([["e"], [], ["p", ""]])] {
        let raw = sign(1, 1, tags.clone(), "").to_string();
        assert_round_trip(raw.as_bytes());
    }
    let raw = sign(1, 1, json!([["e"], [], ["p", ""]]), "").to_string();
    let index = build(&raw).unwrap();
    let note = NoteRef::new(&index, raw.as_bytes()).unwrap();
    assert_eq!(note.ntags(), 3);
    assert_eq!(note.nvals(), 3);
    assert!(note.tag(1).unwrap().is_empty());
    assert_eq!(note.tag(2).unwrap().get(1).unwrap(), b"");
    assert!(note.tag(3).is_none());
}

/// Swaps the hex digit at the start of `field`'s value for another one.
fn flip_first_digit(raw: &str, field: &str) -> String {
    let at = raw.find(&format!("\"{field}\":\"")).unwrap() + field.len() + 4;
    let mut b = raw.as_bytes().to_vec();
    b[at] = if b[at] == b'0' { b'1' } else { b'0' };
    String::from_utf8(b).unwrap()
}

#[test]
fn bad_id_rejected() {
    let raw = sign(1700000000, 1, json!([["p", "abc"]]), "hi").to_string();
    assert_eq!(build(&flip_first_digit(&raw, "id")), Err(Error::BadId));
    // Content edited after signing no longer hashes to the id.
    assert_eq!(build(&raw.replace("\"hi\"", "\"ho\"")), Err(Error::BadId));
    // Same for a tag value.
    assert_eq!(build(&raw.replace("\"abc\"", "\"abd\"")), Err(Error::BadId));
}

#[test]
fn bad_sig_rejected() {
    let ev = sign(1700000000, 1, json!([]), "hi");
    let raw = ev.to_string();
    assert_eq!(build(&flip_first_digit(&raw, "sig")), Err(Error::BadSig));
    // A signature that verifies for a different event.
    let other = sign(1700000001, 1, json!([]), "hi");
    let swapped = raw.replace(ev["sig"].as_str().unwrap(), other["sig"].as_str().unwrap());
    assert_eq!(build(&swapped), Err(Error::BadSig));
    // build_unverified skips both checks.
    let mut out = vec![];
    NoteBuilder::new()
        .build_unverified(flip_first_digit(&raw, "sig").as_bytes(), &mut out)
        .unwrap();
}

#[test]
fn failed_build_leaves_out_untouched() {
    let good = sign(1, 1, json!([]), "a").to_string();
    let bad = flip_first_digit(&good, "sig");
    let mut b = NoteBuilder::new();
    let mut out = vec![9u8; 3];
    assert_eq!(b.build(bad.as_bytes(), &mut out), Err(Error::BadSig));
    assert_eq!(out, [9, 9, 9]);
    b.build(good.as_bytes(), &mut out).unwrap();
    assert!(NoteRef::new(&out[3..], good.as_bytes()).is_ok());
}

#[test]
fn malformed_events_rejected() {
    let ev = sign(1700000000, 1, json!([["e", "x"]]), "hi");
    let raw = ev.to_string();
    let missing = {
        let mut v = ev.clone();
        v.as_object_mut().unwrap().remove("kind");
        v.to_string()
    };
    assert_eq!(build(&missing), Err(Error::Missing("kind")));
    let dup = raw.replacen("{", "{\"kind\":1,", 1);
    assert_eq!(build(&dup), Err(Error::Duplicate("kind")));

    let bad = |s: &str| assert!(matches!(build(s), Err(Error::Json { .. })), "accepted {s}");
    bad(&raw.replace("\"kind\":1", "\"kind\":1.0"));
    bad(&raw.replace("\"kind\":1", "\"kind\":01"));
    bad(&raw.replace("\"kind\":1", "\"kind\":-1"));
    bad(&raw.replace("\"kind\":1", "\"kind\":\"1\""));
    bad(&raw.replace("\"kind\":1", "\"kind\":4294967296"));
    bad(&raw.replace("[\"e\",\"x\"]", "[\"e\",1]"));
    bad(&raw.replace("[[\"e\",\"x\"]]", "[\"e\"]"));
    bad(&raw.replace("\"hi\"", "\"h\ni\""));
    bad(&raw.replace("\"hi\"", r#""h\qi""#));
    bad(&raw.replace("\"hi\"", r#""h\ud800i""#));
    bad(&raw.replace("\"x\"", r#""\ud800""#));
    bad(&(raw.clone() + "x"));
    bad(&raw[..raw.len() - 1]);
    let id = ev["id"].as_str().unwrap();
    bad(&raw.replace(id, &id.to_uppercase()));
    bad(&raw.replace(id, &id[1..]));
    assert_eq!(build("{}"), Err(Error::Missing("id")));
    assert_eq!(build_note(b"{\"id\":\"\xff\"}"), Err(Error::Utf8));
}

/// A content escape that is well-shaped but decodes to no character.
const LONE_SURROGATES: [&str; 3] = [r"\ud800", r"\ude00", r"\ud83dA"];

#[test]
fn lone_surrogate_in_content_rejected() {
    let z = |n| "0".repeat(n);
    let signed = sign(1700000000, 1, json!([["e", "x"]]), "hi").to_string();
    for esc in LONE_SURROGATES {
        let garbage = format!(
            r#"{{"id":"{}","pubkey":"{}","created_at":1,"kind":1,"tags":[],"content":"{esc}","sig":"{}"}}"#,
            z(64),
            z(64),
            z(128)
        );
        // A real signature over other content: the parse must fail before
        // the id hash, not at it.
        let resigned = signed.replace("\"hi\"", &format!("\"{esc}\""));
        for raw in [&garbage, &resigned] {
            let json = |r: Result<(), Error>| matches!(r, Err(Error::Json { .. }));
            assert!(json(build(raw).map(drop)), "build accepted {raw}");
            let mut out = vec![];
            let r = NoteBuilder::new().build_unverified(raw.as_bytes(), &mut out);
            assert!(json(r), "build_unverified accepted {raw}");
            assert!(out.is_empty());
        }
    }
}

#[test]
fn escaped_content_after_escaped_tags() {
    // The content check decodes into the side buffer the tag values live
    // in; it must leave them as they were.
    let raw = format!(
        r#"{{"id":"{0}","pubkey":"{0}","created_at":1,"kind":1,"tags":[["q","a\nb"],["t","\u00e9"]],"content":"c\ud83d\ude00\"d","sig":"{1}"}}"#,
        "0".repeat(64),
        "0".repeat(128)
    );
    let mut out = vec![];
    NoteBuilder::new()
        .build_unverified(raw.as_bytes(), &mut out)
        .unwrap();
    let note = NoteRef::new(&out, raw.as_bytes()).unwrap();
    assert_eq!(note.tag(0).unwrap().str(1).unwrap(), "a\nb");
    assert_eq!(note.tag(1).unwrap().str(1).unwrap(), "é");
    assert_eq!(note.content(), "c😀\"d");
}

#[test]
fn unknown_fields_and_whitespace_are_skipped() {
    let raw = sign(1700000000, 1, json!([["e", "x"]]), "hi").to_string();
    let extra = raw.replacen(
        "{",
        r#" { "relays" : [ {"a": [1, -2.5e-3, true, false, null, "s\"]"]}, [] ] , "n": 0 ,"#,
        1,
    ) + "\n";
    let index = build(&extra).unwrap();
    let note = NoteRef::new(&index, extra.as_bytes()).unwrap();
    assert_eq!(note.content(), "hi");
    assert_eq!(note.tag(0).unwrap().str(1).unwrap(), "x");
}

#[test]
fn canonical_matches_nip01_escaping() {
    // NIP-01 escapes only " \ and \n \r \t \b \f; other control characters
    // (here U+0001, sent as \u0001) go into the hash verbatim.
    let tags = json!([]);
    let kp = Keypair::from_secret_bytes([7; 32]).unwrap();
    let pubkey = hex(&kp.x_only_public_key().0.to_byte_array());
    let canon = format!("[0,\"{pubkey}\",5,1,[],\"a\u{1}\\n\\\"b\"]");
    let id: [u8; 32] = Sha256::digest(canon.as_bytes()).into();
    let sig = kp.sign_schnorr_no_aux_rand(&id);
    let raw = format!(
        r#"{{"id":"{}","pubkey":"{pubkey}","created_at":5,"kind":1,"tags":{tags},"content":"a\u0001\n\"b","sig":"{}"}}"#,
        hex(&id),
        hex(&sig.to_byte_array())
    );
    let index = build(&raw).unwrap();
    let note = NoteRef::new(&index, raw.as_bytes()).unwrap();
    let mut out = vec![];
    noq::canonical(&note, &mut out);
    assert_eq!(out, canon.as_bytes());
}

#[test]
fn note_ref_rejects_short_index() {
    let raw = sign(1, 1, json!([["a", "b"]]), "").to_string();
    let index = build(&raw).unwrap();
    assert_eq!(
        NoteRef::new(&index[..10], raw.as_bytes()).err(),
        Some(Error::BadIndex)
    );
    assert_eq!(
        NoteRef::new(&index[..index.len() - 1], raw.as_bytes()).err(),
        Some(Error::BadIndex)
    );
}
