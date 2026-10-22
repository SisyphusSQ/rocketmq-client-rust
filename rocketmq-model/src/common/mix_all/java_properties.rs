// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;

use cheetah_string::CheetahString;

/// MixAll.string2Properties uses UTF-8 bytes and Properties.load(InputStream)'s Latin-1 reader.
/// A malformed unicode escape returns None, as Java MixAll does after catching the exception.
pub(super) fn parse(input: &str) -> Option<HashMap<CheetahString, CheetahString>> {
    let bytes = input.as_bytes();
    let mut cursor = 0;
    let mut result = HashMap::new();
    while cursor < bytes.len() {
        let mut line = Vec::new();
        let mut continued = false;
        loop {
            while cursor < bytes.len() && matches!(bytes[cursor], b' ' | b'\t' | 0x0c) {
                cursor += 1;
            }
            let start = cursor;
            while cursor < bytes.len() && !matches!(bytes[cursor], b'\r' | b'\n') {
                cursor += 1;
            }
            let end = cursor;
            if cursor < bytes.len() {
                let separator = bytes[cursor];
                cursor += 1;
                if separator == b'\r' && bytes.get(cursor) == Some(&b'\n') {
                    cursor += 1;
                }
            }
            if !continued && matches!(bytes.get(start), Some(b'#' | b'!')) {
                break;
            }
            line.extend_from_slice(&bytes[start..end]);
            let slashes = line.iter().rev().take_while(|byte| **byte == b'\\').count();
            if slashes % 2 == 1 {
                line.pop();
                continued = true;
                if cursor < bytes.len() {
                    continue;
                }
            }
            if line.is_empty() {
                break;
            }
            let mut key_end = 0;
            let mut escaped = false;
            while key_end < line.len() {
                let byte = line[key_end];
                if !escaped && matches!(byte, b'=' | b':' | b' ' | b'\t' | 0x0c) {
                    break;
                }
                escaped = byte == b'\\' && !escaped;
                key_end += 1;
            }
            let mut value_start = key_end;
            while value_start < line.len() && matches!(line[value_start], b' ' | b'\t' | 0x0c) {
                value_start += 1;
            }
            if matches!(line.get(value_start), Some(b'=' | b':')) {
                value_start += 1;
            }
            while value_start < line.len() && matches!(line[value_start], b' ' | b'\t' | 0x0c) {
                value_start += 1;
            }
            result.insert(
                unescape(&line[..key_end])?.into(),
                unescape(&line[value_start..])?.into(),
            );
            break;
        }
    }
    Some(result)
}

fn unescape(bytes: &[u8]) -> Option<String> {
    let mut units = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        cursor += 1;
        if byte != b'\\' {
            units.push(u16::from(byte));
            continue;
        }
        let escaped = *bytes.get(cursor)?;
        cursor += 1;
        let unit = match escaped {
            b't' => 9,
            b'r' => 13,
            b'n' => 10,
            b'f' => 12,
            b'u' => {
                let mut value = 0;
                for _ in 0..4 {
                    let digit = char::from(*bytes.get(cursor)?).to_digit(16)?;
                    value = (value << 4) | digit as u16;
                    cursor += 1;
                }
                value
            }
            other => u16::from(other),
        };
        units.push(unit);
    }
    String::from_utf16(&units).ok()
}
