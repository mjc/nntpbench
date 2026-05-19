use std::fmt::Write as _;
use std::fs;
use std::io::{self, Read};
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

pub(crate) fn download_target_label(target: &ArticleDownloadTarget) -> String {
    match target {
        ArticleDownloadTarget::Number(article_id) => article_id.to_string(),
        ArticleDownloadTarget::MessageId(message_id) => message_id.as_str().to_string(),
    }
}

pub(crate) fn read_article_targets(path: &Path) -> io::Result<Vec<ArticleDownloadTarget>> {
    let contents = fs::read_to_string(path)?;
    let mut targets = Vec::new();

    for (line_index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.bytes().all(|byte| byte.is_ascii_digit()) {
            let id = line.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid article number on line {}", line_index + 1),
                )
            })?;
            if id == 0 || id > MAX_ARTICLE_NUMBER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("article number out of RFC range on line {}", line_index + 1),
                ));
            }
            targets.push(ArticleDownloadTarget::Number(id));
            continue;
        }

        let id = MessageId::from_str_or_wrap(line).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid article message-id on line {}", line_index + 1),
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

pub(crate) fn open_article_response(
    root: &Path,
    key: ArticleStoreKey<'_>,
) -> io::Result<Option<fs::File>> {
    let path = match key {
        ArticleStoreKey::Number(article_id) => article_id_tree_path(root, article_id),
        ArticleStoreKey::MessageId(message_id) => message_id_tree_path(root, message_id),
    };
    open_optional_file(path)
}

fn open_optional_file(path: PathBuf) -> io::Result<Option<fs::File>> {
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
            hex_lower(expected.as_slice()),
            path.display(),
            hex_lower(received.as_slice())
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

pub(crate) fn article_id_tree_path(root: &Path, article_id: u64) -> PathBuf {
    root.join(format!("{:03}", article_id / 1_000_000))
        .join(format!("{:03}", (article_id / 1_000) % 1_000))
        .join(article_id.to_string())
}

pub(crate) fn message_id_tree_path(root: &Path, message_id: &MessageId<'_>) -> PathBuf {
    let encoded = hex_lower(message_id.as_str().as_bytes());
    root.join("msgid").join(&encoded[..2]).join(encoded)
}

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
