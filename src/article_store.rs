use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, BufRead, Read};
use std::path::{Path, PathBuf};

use arrayvec::ArrayString;
use md5::{Digest, Md5};

use crate::protocol::{ArticleRef, MAX_ARTICLE_NUMBER, MessageId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArticleDownloadTarget {
    Number(u64),
    MessageId(MessageId<'static>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ArticleStoreKey<'a> {
    Number(u64),
    MessageId(&'a MessageId<'a>),
}

pub(crate) fn article_ref_for_download_target(
    target: &ArticleDownloadTarget,
) -> io::Result<ArticleRef<'_>> {
    match target {
        ArticleDownloadTarget::Number(article_id) => ArticleRef::from_number(*article_id)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid article id")),
        ArticleDownloadTarget::MessageId(message_id) => Ok(ArticleRef::MessageId(
            MessageId::from_borrowed(message_id.as_str())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid message-id"))?,
        )),
    }
}

pub(crate) fn download_target_label(target: &ArticleDownloadTarget) -> DownloadTargetLabel<'_> {
    DownloadTargetLabel(target)
}

pub(crate) struct DownloadTargetLabel<'a>(&'a ArticleDownloadTarget);

impl fmt::Display for DownloadTargetLabel<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ArticleDownloadTarget::Number(article_id) => write!(f, "{article_id}"),
            ArticleDownloadTarget::MessageId(message_id) => f.write_str(message_id.as_str()),
        }
    }
}

pub(crate) fn read_article_targets(path: &Path) -> io::Result<Vec<ArticleDownloadTarget>> {
    let file = fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let mut line_buf = Vec::with_capacity(512);
    let mut targets = Vec::new();
    let mut line_index = 0;

    loop {
        line_buf.clear();
        let read = reader.read_until(b'\n', &mut line_buf)?;
        if read == 0 {
            break;
        }
        line_index += 1;
        let line = trim_ascii_line(&line_buf);
        if line.is_empty() {
            continue;
        }

        if line.iter().all(u8::is_ascii_digit) {
            let line = std::str::from_utf8(line).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "article number is not utf-8")
            })?;
            let id = line.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid article number on line {line_index}"),
                )
            })?;
            if id == 0 || id > MAX_ARTICLE_NUMBER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("article number out of RFC range on line {line_index}"),
                ));
            }
            targets.push(ArticleDownloadTarget::Number(id));
            continue;
        }

        let line = std::str::from_utf8(line).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "article message-id is not utf-8",
            )
        })?;
        let id = MessageId::from_str_or_wrap(line).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid article message-id on line {line_index}"),
            )
        })?;
        targets.push(ArticleDownloadTarget::MessageId(id));
    }

    if targets.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no article selectors found in {}", path.display()),
        ));
    }

    Ok(targets)
}

fn trim_ascii_line(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\n' | b'\r' | b' ' | b'\t')) {
        line = &line[..line.len() - 1];
    }
    while matches!(line.first(), Some(b' ' | b'\t')) {
        line = &line[1..];
    }
    line
}

pub(crate) fn write_article_response_file_into(
    root: &Path,
    target: &ArticleDownloadTarget,
    response: &[u8],
    path: &mut PathBuf,
) -> io::Result<()> {
    article_download_target_path_into(path, root, target)?;
    write_article_response_file_at(path, response)
}

pub(crate) fn write_failed_article_response_file_into(
    root: &Path,
    target: &ArticleDownloadTarget,
    response: &[u8],
    path: &mut PathBuf,
) -> io::Result<()> {
    failed_article_download_target_path_into(path, root, target)?;
    write_article_response_file_at(path, response)
}

fn write_article_response_file_at(path: &Path, response: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, response)
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

pub(crate) fn verify_article_response_file_into(
    root: Option<&Path>,
    target: &ArticleDownloadTarget,
    response: &[u8],
    path: &mut PathBuf,
) -> io::Result<()> {
    let Some(root) = root else {
        return Ok(());
    };
    article_download_target_path_into(path, root, target)?;
    if !path.exists() {
        return Ok(());
    }

    let expected = md5_file(path)?;
    let mut received_hasher = Md5::new();
    received_hasher.update(response);
    let received = received_hasher.finalize();
    if expected.as_slice() == received.as_slice() {
        return Ok(());
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "ARTICLE {} MD5 mismatch: expected {} from {}, received {}",
            download_target_label(target),
            HexLower(expected.as_slice()),
            path.display(),
            HexLower(received.as_slice())
        ),
    ))
}

fn md5_file(path: &Path) -> io::Result<md5::digest::Output<Md5>> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Md5::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hasher.finalize());
        }
        hasher.update(&buffer[..read]);
    }
}

pub(crate) fn article_download_target_path_into(
    path: &mut PathBuf,
    root: &Path,
    target: &ArticleDownloadTarget,
) -> io::Result<()> {
    path.clear();
    path.push(root);
    push_article_download_target_path(path, target)
}

fn failed_article_download_target_path_into(
    path: &mut PathBuf,
    root: &Path,
    target: &ArticleDownloadTarget,
) -> io::Result<()> {
    path.clear();
    path.push(root);
    path.push("failed");
    push_article_download_target_path(path, target)
}

fn push_article_download_target_path(
    path: &mut PathBuf,
    target: &ArticleDownloadTarget,
) -> io::Result<()> {
    match target {
        ArticleDownloadTarget::Number(article_id) => push_article_id_tree_path(path, *article_id),
        ArticleDownloadTarget::MessageId(message_id) => push_message_id_tree_path(path, message_id),
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

struct HexLower<'a>(&'a [u8]);

impl fmt::Display for HexLower<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in self.0 {
            f.write_char(HEX[(byte >> 4) as usize] as char)?;
            f.write_char(HEX[(byte & 0x0f) as usize] as char)?;
        }
        Ok(())
    }
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
