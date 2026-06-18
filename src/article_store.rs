use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use arrayvec::ArrayString;

use crate::protocol::MessageId;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ArticleStoreKey<'a> {
    Number(u64),
    MessageId(&'a MessageId<'a>),
}

pub(crate) fn open_article_response_into(
    root: &Path,
    key: ArticleStoreKey<'_>,
    path: &mut PathBuf,
) -> io::Result<Option<fs::File>> {
    path.clear();
    path.push(root);
    match key {
        ArticleStoreKey::Number(article_id) => push_article_id_tree_path(path, article_id)?,
        ArticleStoreKey::MessageId(message_id) => push_message_id_tree_path(path, message_id)?,
    }
    open_optional_file(path)
}

fn open_optional_file(path: impl AsRef<Path>) -> io::Result<Option<fs::File>> {
    match fs::File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn push_article_id_tree_path(path: &mut PathBuf, article_id: u64) -> io::Result<()> {
    let mut top = ArrayString::<20>::new();
    let mut middle = ArrayString::<20>::new();
    let mut leaf = ArrayString::<20>::new();
    write!(&mut top, "{:03}", article_id / 1_000_000)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid article id"))?;
    write!(&mut middle, "{:03}", (article_id / 1_000) % 1_000)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid article id"))?;
    write!(&mut leaf, "{article_id}")
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid article id"))?;
    path.push(top.as_str());
    path.push(middle.as_str());
    path.push(leaf.as_str());
    Ok(())
}

fn push_message_id_tree_path(path: &mut PathBuf, message_id: &MessageId<'_>) -> io::Result<()> {
    let mut encoded = ArrayString::<1024>::new();
    push_hex_lower(&mut encoded, message_id.as_str().as_bytes())?;
    path.push("msgid");
    path.push(&encoded[..2]);
    path.push(encoded.as_str());
    Ok(())
}

#[cfg(test)]
pub(crate) fn article_id_tree_path(root: &Path, article_id: u64) -> PathBuf {
    root.join(format!("{:03}", article_id / 1_000_000))
        .join(format!("{:03}", (article_id / 1_000) % 1_000))
        .join(article_id.to_string())
}

#[cfg(test)]
pub(crate) fn message_id_tree_path(root: &Path, message_id: &MessageId<'_>) -> PathBuf {
    let encoded = hex_lower(message_id.as_str().as_bytes());
    root.join("msgid").join(&encoded[..2]).join(encoded)
}

#[cfg(test)]
fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn push_hex_lower<const N: usize>(out: &mut ArrayString<N>, bytes: &[u8]) -> io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    if bytes.len().saturating_mul(2) > out.capacity() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "message-id path exceeds stack path encoder capacity",
        ));
    }
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(())
}
