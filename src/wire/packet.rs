//! Datagram classification and bounded control validation.

use super::{Begin, COMPLETE, Header};
use crate::{Error, Result};
use zerocopy::FromBytes;

pub(crate) enum Packet<'a> {
    Data {
        header: Header,
        index: u32,
        width: u8,
        payload: &'a [u8],
    },
    Control {
        header: Header,
        body: &'a [u8],
    },
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Packet<'_>> {
    let (head, rest) =
        Header::ref_from_prefix(bytes).map_err(|_| Error::Protocol("short header"))?;
    let header = *head;
    if header.flags() & !1 != 0
        || (header.flags() != 0 && !matches!(header.kind(), 0 | 1 | COMPLETE))
    {
        return Err(Error::Protocol("reserved flags"));
    }
    if header.kind() <= 1 {
        if header.frame.get() == 0 || header.incarnation.get() == 0 {
            return Err(Error::Protocol("zero data identity"));
        }
        let width = if header.kind() == 0 { 2 } else { 4 };
        if rest.len() < width {
            return Err(Error::Protocol("short fragment index"));
        }
        let index = if width == 2 {
            u16::from_le_bytes(rest[..2].try_into().expect("two bytes")) as u32
        } else {
            u32::from_le_bytes(rest[..4].try_into().expect("four bytes"))
        };
        return Ok(Packet::Data {
            header,
            index,
            width: width as u8,
            payload: &rest[width..],
        });
    }
    if rest.len() < 4 {
        return Err(Error::Protocol("short control CRC"));
    }
    let (body, crc) = rest.split_at(rest.len() - 4);
    if crc32c::crc32c(&bytes[..bytes.len() - 4])
        != u32::from_le_bytes(crc.try_into().expect("four bytes"))
    {
        return Err(Error::Protocol("control CRC"));
    }
    Ok(Packet::Control { header, body })
}

/// Parse a BEGIN body; length is validated by `read_from_bytes`.
pub(crate) fn read_begin(body: &[u8]) -> Result<Begin> {
    Begin::read_from_bytes(body).map_err(|_| Error::Protocol("BEGIN length"))
}
