use super::*;
use zerocopy::IntoBytes;

#[test]
fn release_major_minor_match_wire_version() {
    let mut parts = env!("CARGO_PKG_VERSION").split('.');
    let major: u16 = parts.next().unwrap().parse().unwrap();
    let minor: u16 = parts.next().unwrap().parse().unwrap();
    assert_eq!((PROTOCOL_MAJOR, PROTOCOL_MINOR), (major, minor));
}

#[test]
fn arbitrary_bytes_and_truncated_controls_never_panic() {
    let valid = Control::new(HELLO, 1, 0, &[0; 76]);
    for len in 0..valid.as_bytes().len() {
        assert!(decode(&valid.as_bytes()[..len]).is_err());
    }
    let mut random = 17u64;
    let mut bytes = [0; 256];
    for len in (0..256).cycle().take(10000) {
        for b in &mut bytes[..len] {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            *b = random as u8;
        }
        let _ = decode(&bytes[..len]);
    }
    let mut data = [0; 20];
    data[..13].copy_from_slice(Header::new(1, 7, 8).as_bytes());
    data[13..17].copy_from_slice(&70000u32.to_le_bytes());
    assert!(matches!(
        decode(&data).unwrap(),
        Packet::Data {
            index: 70000,
            width: 4,
            ..
        }
    ));
}
#[test]
fn layout_and_crc_golden() {
    assert_eq!(size_of::<Header>(), 13);
    assert_eq!(size_of::<Begin>(), 36);
    assert_eq!(crc32c::crc32c(b"123456789"), 0xe3069283);
    let b = Begin::new(1185 * 65536, 1200, 1).unwrap();
    assert_eq!(b.idx_bytes, 2);
    assert_eq!(Begin::new(1185 * 65536 + 1, 1200, 1).unwrap().idx_bytes, 4);
    let mut c = Control::new(BEGIN, 1, 1, b.as_bytes());
    assert_eq!(c.len, BEGIN_BYTES);
    assert!(decode(c.as_bytes()).is_ok());
    c.corrupt(20);
    assert!(decode(c.as_bytes()).is_err());
}
