use std::fmt;
use std::fmt::Write as FmtWrite;
use std::future::poll_fn;
use std::io;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::protocol::{
    ArticleRef, Request, ResponseFrameDecoder, ResponseFrameParse, ResponseInitialParse,
};
use crate::terminator::{
    DOT_TERMINATOR, EmptyMultilineTerminator, EmptyTerminatorStatus, MultilineTerminatorDetector,
    crlf_normalized_payload_lines,
};
use crate::{
    Article, ArticleParseError, ArticleSelector, ArticleTransfer, AuthInfoKind, AuthInfoValue,
    GroupName, HeaderName, ListGroupRange, ListKind, MessageId, NntpDate, NntpTime, RequestKind,
    StatusCode, Wildmat,
};

const DRAINED_PENDING_READ_BYTES: usize = 1024 * 1024;
const OWNED_RESPONSE_PREALLOC_BYTES: usize = 8 * 1024 * 1024;
const STREAMING_STATUS_LINE_BYTES: usize = crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES;

/// Options for the typed one-connection client prototype.
#[derive(Debug, Clone, Copy)]
pub struct TypedClientOptions {
    pub read_buffer_bytes: usize,
    pub nodelay: bool,
    pub socket_recv_buffer: usize,
    pub socket_send_buffer: usize,
    pub pipeline_depth: usize,
}

impl Default for TypedClientOptions {
    fn default() -> Self {
        Self {
            read_buffer_bytes: crate::CLIENT_READER_CAPACITY,
            nodelay: true,
            socket_recv_buffer: 0,
            socket_send_buffer: 0,
            pipeline_depth: 64,
        }
    }
}

/// Primary request-specific client surface for typed NNTP calls.
#[derive(Debug, Clone)]
pub struct Client {
    connection: TypedClientConnection,
}

impl Client {
    /// Connect and consume the NNTP greeting.
    pub async fn connect(addr: SocketAddr) -> Result<Self, TypedClientError> {
        Self::connect_with_options(addr, TypedClientOptions::default()).await
    }

    /// Connect with explicit socket and buffer options.
    pub async fn connect_with_options(
        addr: SocketAddr,
        options: TypedClientOptions,
    ) -> Result<Self, TypedClientError> {
        Ok(Self {
            connection: TypedClientConnection::connect_with_options(addr, options).await?,
        })
    }

    /// Execute a typed article-style request directly and return a typed owned article-style response.
    pub async fn execute(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let response = self.connection.execute(request).await?;
        OwnedArticle::try_from(response)
    }

    /// Execute a typed request directly and return the completed request/response pair.
    pub async fn execute_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let exchange = self.connection.execute_exchange(request).await?;
        OwnedArticleExchange::try_from(exchange)
    }

    /// Execute a typed request directly and return the owned raw response frame.
    pub async fn execute_raw(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.connection.execute(request).await
    }

    /// Execute a typed request directly and return the completed raw request/response pair.
    pub async fn execute_raw_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.connection.execute_exchange(request).await
    }

    /// Send an ARTICLE request and return a typed owned article response.
    pub async fn article(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request =
            Request::article(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send an ARTICLE request for the current article and return a typed owned article response.
    pub async fn article_current(&self) -> Result<OwnedArticle, TypedClientError> {
        self.execute(Request::article_current()).await
    }

    /// Send an ARTICLE request using a numeric/message-id selector and return a typed owned article response.
    pub async fn article_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::article_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send an ARTICLE request and return the completed request/response pair.
    pub async fn article_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request =
            Request::article(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send an ARTICLE request for the current article and return the completed request/response pair.
    pub async fn article_current_exchange(&self) -> Result<OwnedArticleExchange, TypedClientError> {
        self.execute_exchange(Request::article_current()).await
    }

    /// Send an ARTICLE request using a numeric/message-id selector and return the completed request/response pair.
    pub async fn article_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::article_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a BODY request and return a typed owned article-style response.
    pub async fn body(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::body(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send a BODY request for the current article and return a typed owned article-style response.
    pub async fn body_current(&self) -> Result<OwnedArticle, TypedClientError> {
        self.execute(Request::body_current()).await
    }

    /// Send a BODY request using a numeric/message-id selector and return a typed owned article-style response.
    pub async fn body_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::body_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send a BODY request and return the completed request/response pair.
    pub async fn body_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::body(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a BODY request for the current article and return the completed request/response pair.
    pub async fn body_current_exchange(&self) -> Result<OwnedArticleExchange, TypedClientError> {
        self.execute_exchange(Request::body_current()).await
    }

    /// Send a BODY request using a numeric/message-id selector and return the completed request/response pair.
    pub async fn body_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::body_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a HEAD request and return a typed owned article-style response.
    pub async fn head(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::head(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send a HEAD request for the current article and return a typed owned article-style response.
    pub async fn head_current(&self) -> Result<OwnedArticle, TypedClientError> {
        self.execute(Request::head_current()).await
    }

    /// Send a HEAD request using a numeric/message-id selector and return a typed owned article-style response.
    pub async fn head_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::head_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send a HEAD request and return the completed request/response pair.
    pub async fn head_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::head(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a HEAD request for the current article and return the completed request/response pair.
    pub async fn head_current_exchange(&self) -> Result<OwnedArticleExchange, TypedClientError> {
        self.execute_exchange(Request::head_current()).await
    }

    /// Send a HEAD request using a numeric/message-id selector and return the completed request/response pair.
    pub async fn head_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::head_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a STAT request and return a typed owned article-style response.
    pub async fn stat(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::stat(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send a STAT request for the current article and return a typed owned article-style response.
    pub async fn stat_current(&self) -> Result<OwnedArticle, TypedClientError> {
        self.execute(Request::stat_current()).await
    }

    /// Send a STAT request using a numeric/message-id selector and return a typed owned article-style response.
    pub async fn stat_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::stat_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send a STAT request and return the completed request/response pair.
    pub async fn stat_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::stat(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a STAT request for the current article and return the completed request/response pair.
    pub async fn stat_current_exchange(&self) -> Result<OwnedArticleExchange, TypedClientError> {
        self.execute_exchange(Request::stat_current()).await
    }

    /// Send a STAT request using a numeric/message-id selector and return the completed request/response pair.
    pub async fn stat_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::stat_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a GROUP request and return the owned raw response frame.
    pub async fn group(&self, group: impl AsRef<str>) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::group(group).map_err(|_| TypedClientError::InvalidGroupName)?;
        self.execute_raw(request).await
    }

    /// Send a GROUP request and return the completed raw request/response pair.
    pub async fn group_exchange(
        &self,
        group: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::group(group).map_err(|_| TypedClientError::InvalidGroupName)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LISTGROUP request and return the owned raw response frame.
    pub async fn listgroup(
        &self,
        group: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::listgroup(group).map_err(|_| TypedClientError::InvalidGroupName)?;
        self.execute_raw(request).await
    }

    /// Send a LISTGROUP request for the current selected group and return the owned raw response frame.
    pub async fn listgroup_current(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::listgroup_current()).await
    }

    /// Send a LISTGROUP request for the current selected group with a range filter and return the owned raw response frame.
    pub async fn listgroup_range(
        &self,
        range: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::listgroup_range(range).map_err(|_| TypedClientError::InvalidListGroupRange)?;
        self.execute_raw(request).await
    }

    /// Send a LISTGROUP request with explicit group and range arguments and return the owned raw response frame.
    pub async fn listgroup_group_range(
        &self,
        group: impl AsRef<str>,
        range: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::listgroup_group_range(group, range).map_err(TypedClientError::from)?;
        self.execute_raw(request).await
    }

    /// Send a LISTGROUP request and return the completed raw request/response pair.
    pub async fn listgroup_exchange(
        &self,
        group: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::listgroup(group).map_err(|_| TypedClientError::InvalidGroupName)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LISTGROUP request for the current selected group and return the completed raw request/response pair.
    pub async fn listgroup_current_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::listgroup_current())
            .await
    }

    /// Send a LISTGROUP request for the current selected group with a range filter and return the completed raw request/response pair.
    pub async fn listgroup_range_exchange(
        &self,
        range: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::listgroup_range(range).map_err(|_| TypedClientError::InvalidListGroupRange)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LISTGROUP request with explicit group and range arguments and return the completed raw request/response pair.
    pub async fn listgroup_group_range_exchange(
        &self,
        group: impl AsRef<str>,
        range: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::listgroup_group_range(group, range).map_err(TypedClientError::from)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LAST request and return the owned raw response frame.
    pub async fn last(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::last()).await
    }

    /// Send a LAST request and return the completed raw request/response pair.
    pub async fn last_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::last()).await
    }

    /// Send a NEXT request and return the owned raw response frame.
    pub async fn next(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::next()).await
    }

    /// Send a NEXT request and return the completed raw request/response pair.
    pub async fn next_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::next()).await
    }

    /// Send a NEWGROUPS request and return the owned raw response frame.
    pub async fn newgroups(
        &self,
        date: impl AsRef<str>,
        time: impl AsRef<str>,
        gmt: bool,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::newgroups(date, time, gmt).map_err(TypedClientError::from)?;
        self.execute_raw(request).await
    }

    /// Send a NEWGROUPS request and return the completed raw request/response pair.
    pub async fn newgroups_exchange(
        &self,
        date: impl AsRef<str>,
        time: impl AsRef<str>,
        gmt: bool,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::newgroups(date, time, gmt).map_err(TypedClientError::from)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a NEWNEWS request and return the owned raw response frame.
    pub async fn newnews(
        &self,
        wildmat: impl AsRef<str>,
        date: impl AsRef<str>,
        time: impl AsRef<str>,
        gmt: bool,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::newnews(wildmat, date, time, gmt).map_err(TypedClientError::from)?;
        self.execute_raw(request).await
    }

    /// Send a NEWNEWS request and return the completed raw request/response pair.
    pub async fn newnews_exchange(
        &self,
        wildmat: impl AsRef<str>,
        date: impl AsRef<str>,
        time: impl AsRef<str>,
        gmt: bool,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::newnews(wildmat, date, time, gmt).map_err(TypedClientError::from)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a POST request and return the owned raw response frame.
    pub async fn post(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::post()).await
    }

    /// Send a POST request and return the completed raw request/response pair.
    pub async fn post_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::post()).await
    }

    /// Send an IHAVE request and return the owned raw response frame.
    pub async fn ihave(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::ihave(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_raw(request).await
    }

    /// Send an IHAVE request and return the completed raw request/response pair.
    pub async fn ihave_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::ihave(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a CHECK request and return the owned raw response frame.
    pub async fn check(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::check(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_raw(request).await
    }

    /// Send a CHECK request and return the completed raw request/response pair.
    pub async fn check_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::check(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a TAKETHIS request and return the owned raw response frame.
    pub async fn takethis(
        &self,
        message_id: impl AsRef<str>,
        article: impl AsRef<[u8]>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::takethis(message_id, article)
            .map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_raw(request).await
    }

    /// Send a TAKETHIS request and return the completed raw request/response pair.
    pub async fn takethis_exchange(
        &self,
        message_id: impl AsRef<str>,
        article: impl AsRef<[u8]>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::takethis(message_id, article)
            .map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_raw_exchange(request).await
    }

    /// Send an AUTHINFO USER request and return the owned raw response frame.
    pub async fn authinfo_user(
        &self,
        value: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::authinfo_user(value).map_err(|_| TypedClientError::InvalidAuthInfoValue)?;
        self.execute_raw(request).await
    }

    /// Send an AUTHINFO USER request and return the completed raw request/response pair.
    pub async fn authinfo_user_exchange(
        &self,
        value: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::authinfo_user(value).map_err(|_| TypedClientError::InvalidAuthInfoValue)?;
        self.execute_raw_exchange(request).await
    }

    /// Send an AUTHINFO PASS request and return the owned raw response frame.
    pub async fn authinfo_pass(
        &self,
        value: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::authinfo_pass(value).map_err(|_| TypedClientError::InvalidAuthInfoValue)?;
        self.execute_raw(request).await
    }

    /// Send an AUTHINFO PASS request and return the completed raw request/response pair.
    pub async fn authinfo_pass_exchange(
        &self,
        value: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::authinfo_pass(value).map_err(|_| TypedClientError::InvalidAuthInfoValue)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a STARTTLS request and return the owned raw response frame.
    pub async fn starttls(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::starttls()).await
    }

    /// Send a STARTTLS request and return the completed raw request/response pair.
    pub async fn starttls_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::starttls()).await
    }

    /// Send an OVER request and return the owned raw response frame.
    pub async fn over(&self, selector: impl AsRef<str>) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::over(selector).map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_raw(request).await
    }

    /// Send an OVER request and return the completed raw request/response pair.
    pub async fn over_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::over(selector).map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_raw_exchange(request).await
    }

    /// Send an XOVER request and return the owned raw response frame.
    pub async fn xover(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::xover(selector).map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_raw(request).await
    }

    /// Send an XOVER request and return the completed raw request/response pair.
    pub async fn xover_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::xover(selector).map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_raw_exchange(request).await
    }

    /// Send an HDR request and return the owned raw response frame.
    pub async fn hdr(
        &self,
        header: impl AsRef<str>,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::hdr(header, selector).map_err(TypedClientError::from)?;
        self.execute_raw(request).await
    }

    /// Send an HDR request and return the completed raw request/response pair.
    pub async fn hdr_exchange(
        &self,
        header: impl AsRef<str>,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::hdr(header, selector).map_err(TypedClientError::from)?;
        self.execute_raw_exchange(request).await
    }

    /// Send an XHDR request and return the owned raw response frame.
    pub async fn xhdr(
        &self,
        header: impl AsRef<str>,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::xhdr(header, selector).map_err(TypedClientError::from)?;
        self.execute_raw(request).await
    }

    /// Send an XHDR request and return the completed raw request/response pair.
    pub async fn xhdr_exchange(
        &self,
        header: impl AsRef<str>,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::xhdr(header, selector).map_err(TypedClientError::from)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LIST request and return the owned raw response frame.
    pub async fn list(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list()).await
    }

    /// Send a LIST request and return the completed raw request/response pair.
    pub async fn list_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list()).await
    }

    /// Send a LIST ACTIVE request and return the owned raw response frame.
    pub async fn list_active(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list_active()).await
    }

    /// Send a LIST ACTIVE request and return the completed raw request/response pair.
    pub async fn list_active_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list_active()).await
    }

    /// Send a LIST ACTIVE [wildmat] request and return the owned raw response frame.
    pub async fn list_active_wildmat(
        &self,
        wildmat: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request =
            Request::list_active_wildmat(wildmat).map_err(|_| TypedClientError::InvalidWildmat)?;
        self.execute_raw(request).await
    }

    /// Send a LIST ACTIVE [wildmat] request and return the completed raw request/response pair.
    pub async fn list_active_wildmat_exchange(
        &self,
        wildmat: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request =
            Request::list_active_wildmat(wildmat).map_err(|_| TypedClientError::InvalidWildmat)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LIST ACTIVE.TIMES request and return the owned raw response frame.
    pub async fn list_active_times(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list_active_times()).await
    }

    /// Send a LIST ACTIVE.TIMES request and return the completed raw request/response pair.
    pub async fn list_active_times_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list_active_times())
            .await
    }

    /// Send a LIST ACTIVE.TIMES [wildmat] request and return the owned raw response frame.
    pub async fn list_active_times_wildmat(
        &self,
        wildmat: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::list_active_times_wildmat(wildmat)
            .map_err(|_| TypedClientError::InvalidWildmat)?;
        self.execute_raw(request).await
    }

    /// Send a LIST ACTIVE.TIMES [wildmat] request and return the completed raw request/response pair.
    pub async fn list_active_times_wildmat_exchange(
        &self,
        wildmat: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::list_active_times_wildmat(wildmat)
            .map_err(|_| TypedClientError::InvalidWildmat)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LIST NEWSGROUPS request and return the owned raw response frame.
    pub async fn list_newsgroups(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list_newsgroups()).await
    }

    /// Send a LIST NEWSGROUPS request and return the completed raw request/response pair.
    pub async fn list_newsgroups_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list_newsgroups()).await
    }

    /// Send a LIST NEWSGROUPS [wildmat] request and return the owned raw response frame.
    pub async fn list_newsgroups_wildmat(
        &self,
        wildmat: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::list_newsgroups_wildmat(wildmat)
            .map_err(|_| TypedClientError::InvalidWildmat)?;
        self.execute_raw(request).await
    }

    /// Send a LIST NEWSGROUPS [wildmat] request and return the completed raw request/response pair.
    pub async fn list_newsgroups_wildmat_exchange(
        &self,
        wildmat: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::list_newsgroups_wildmat(wildmat)
            .map_err(|_| TypedClientError::InvalidWildmat)?;
        self.execute_raw_exchange(request).await
    }

    /// Send a LIST OVERVIEW.FMT request and return the owned raw response frame.
    pub async fn list_overview_fmt(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list_overview_fmt()).await
    }

    /// Send a LIST OVERVIEW.FMT request and return the completed raw request/response pair.
    pub async fn list_overview_fmt_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list_overview_fmt())
            .await
    }

    /// Send a LIST HEADERS request and return the owned raw response frame.
    pub async fn list_headers(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list_headers()).await
    }

    /// Send a LIST HEADERS request and return the completed raw request/response pair.
    pub async fn list_headers_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list_headers()).await
    }

    /// Send a LIST DISTRIB.PATS request and return the owned raw response frame.
    pub async fn list_distrib_pats(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::list_distrib_pats()).await
    }

    /// Send a LIST DISTRIB.PATS request and return the completed raw request/response pair.
    pub async fn list_distrib_pats_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::list_distrib_pats())
            .await
    }

    /// Send a HELP request and return the owned raw response frame.
    pub async fn help(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::help()).await
    }

    /// Send a HELP request and return the completed raw request/response pair.
    pub async fn help_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::help()).await
    }

    /// Send a CAPABILITIES request and return the owned raw response frame.
    pub async fn capabilities(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::capabilities()).await
    }

    /// Send a CAPABILITIES request and return the completed raw request/response pair.
    pub async fn capabilities_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::capabilities()).await
    }

    /// Send a DATE request and return the owned raw response frame.
    pub async fn date(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::date()).await
    }

    /// Send a DATE request and return the completed raw request/response pair.
    pub async fn date_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::date()).await
    }

    /// Send a MODE READER request and return the owned raw response frame.
    pub async fn mode_reader(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::mode_reader()).await
    }

    /// Send a MODE READER request and return the completed raw request/response pair.
    pub async fn mode_reader_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::mode_reader()).await
    }

    /// Send a QUIT request and return the owned raw response frame.
    pub async fn quit(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::quit()).await
    }

    /// Send a QUIT request and return the completed raw request/response pair.
    pub async fn quit_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::quit()).await
    }

    /// Access the lower-level raw-frame connection.
    #[must_use]
    pub fn connection(&self) -> &TypedClientConnection {
        &self.connection
    }
}

/// One TCP connection exposing request-specific async methods.
#[derive(Debug, Clone)]
pub struct TypedClientConnection {
    inner: Arc<ConnectionHandle>,
}

impl TypedClientConnection {
    /// Connect and consume the NNTP greeting.
    pub async fn connect(addr: SocketAddr) -> Result<Self, TypedClientError> {
        Self::connect_with_options(addr, TypedClientOptions::default()).await
    }

    /// Connect with explicit socket and buffer options.
    pub async fn connect_with_options(
        addr: SocketAddr,
        options: TypedClientOptions,
    ) -> Result<Self, TypedClientError> {
        let mut stream = crate::connect_client_socket(
            addr,
            options.nodelay,
            options.socket_recv_buffer,
            options.socket_send_buffer,
        )
        .await?;
        let mut read_buffer = vec![
            0;
            options
                .read_buffer_bytes
                .max(crate::terminator::TERMINATOR_TAIL_SIZE)
        ]
        .into_boxed_slice();
        crate::read_greeting(&mut stream, &mut read_buffer).await?;
        let (reader, writer) = stream.into_split();
        let (request_tx, request_rx) = mpsc::channel(options.pipeline_depth.max(1));
        let (inflight_tx, inflight_rx) = mpsc::channel(options.pipeline_depth.max(1));
        let poisoned = Arc::new(Mutex::new(None));
        let read_chunk_bytes = read_buffer.len();

        let writer_task = tokio::spawn(run_writer_task(
            writer,
            request_rx,
            inflight_tx,
            poisoned.clone(),
        ));
        let writer_abort = writer_task.abort_handle();
        let reader_task = tokio::spawn(run_reader_task(
            reader,
            read_chunk_bytes,
            inflight_rx,
            poisoned.clone(),
            writer_abort,
        ));

        Ok(Self {
            inner: Arc::new(ConnectionHandle {
                request_tx,
                poisoned,
                writer_task,
                reader_task,
            }),
        })
    }

    /// Send an ARTICLE request and return the owned response frame.
    pub async fn article(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Article {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send an ARTICLE request and return the completed request/response pair.
    pub async fn article_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Article {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send an ARTICLE request for the current article and return the owned response frame.
    pub async fn article_current(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::article_current()).await
    }

    /// Send an ARTICLE request using a numeric or explicit message-id selector and return the owned response frame.
    pub async fn article_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::article_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send an ARTICLE request for the current article and return the completed request/response pair.
    pub async fn article_current_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::article_current()).await
    }

    /// Send an ARTICLE request using a numeric or explicit message-id selector and return the completed request/response pair.
    pub async fn article_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::article_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a BODY request and return the owned response frame.
    pub async fn body(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Body {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send a BODY request and return the completed request/response pair.
    pub async fn body_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Body {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send a BODY request for the current article and return the owned response frame.
    pub async fn body_current(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::body_current()).await
    }

    /// Send a BODY request using a numeric or explicit message-id selector and return the owned response frame.
    pub async fn body_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::body_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send a BODY request for the current article and return the completed request/response pair.
    pub async fn body_current_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::body_current()).await
    }

    /// Send a BODY request using a numeric or explicit message-id selector and return the completed request/response pair.
    pub async fn body_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::body_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a HEAD request and return the owned response frame.
    pub async fn head(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Head {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send a HEAD request and return the completed request/response pair.
    pub async fn head_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Head {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send a HEAD request for the current article and return the owned response frame.
    pub async fn head_current(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::head_current()).await
    }

    /// Send a HEAD request using a numeric or explicit message-id selector and return the owned response frame.
    pub async fn head_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::head_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send a HEAD request for the current article and return the completed request/response pair.
    pub async fn head_current_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::head_current()).await
    }

    /// Send a HEAD request using a numeric or explicit message-id selector and return the completed request/response pair.
    pub async fn head_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::head_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a STAT request and return the owned response frame.
    pub async fn stat(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Stat {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send a STAT request and return the completed request/response pair.
    pub async fn stat_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Stat {
            article_ref: ArticleRef::MessageId(message_id),
        })
        .await
    }

    /// Send a STAT request for the current article and return the owned response frame.
    pub async fn stat_current(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::stat_current()).await
    }

    /// Send a STAT request using a numeric or explicit message-id selector and return the owned response frame.
    pub async fn stat_selector(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedResponse, TypedClientError> {
        let request = Request::stat_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute(request).await
    }

    /// Send a STAT request for the current article and return the completed request/response pair.
    pub async fn stat_current_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::stat_current()).await
    }

    /// Send a STAT request using a numeric or explicit message-id selector and return the completed request/response pair.
    pub async fn stat_selector_exchange(
        &self,
        selector: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::stat_selector(selector)
            .map_err(|_| TypedClientError::InvalidArticleSelector)?;
        self.execute_exchange(request).await
    }

    /// Send a GROUP request and return the owned response frame.
    pub async fn group(
        &self,
        group: GroupName<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Group { group }).await
    }

    /// Send a GROUP request and return the completed request/response pair.
    pub async fn group_exchange(
        &self,
        group: GroupName<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Group { group }).await
    }

    /// Send a LISTGROUP request and return the owned response frame.
    pub async fn listgroup(
        &self,
        group: GroupName<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(
            Request::listgroup(group.as_str()).map_err(|_| TypedClientError::InvalidGroupName)?,
        )
        .await
    }

    /// Send a LISTGROUP request for the current selected group and return the owned response frame.
    pub async fn listgroup_current(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::listgroup_current()).await
    }

    /// Send a LISTGROUP request for the current selected group with a range filter and return the owned response frame.
    pub async fn listgroup_range(
        &self,
        range: ListGroupRange<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(
            Request::listgroup_range(range.as_str())
                .map_err(|_| TypedClientError::InvalidListGroupRange)?,
        )
        .await
    }

    /// Send a LISTGROUP request with explicit group and range arguments and return the owned response frame.
    pub async fn listgroup_group_range(
        &self,
        group: GroupName<'static>,
        range: ListGroupRange<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(
            Request::listgroup_group_range(group.as_str(), range.as_str())
                .map_err(TypedClientError::from)?,
        )
        .await
    }

    /// Send a LISTGROUP request and return the completed request/response pair.
    pub async fn listgroup_exchange(
        &self,
        group: GroupName<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(
            Request::listgroup(group.as_str()).map_err(|_| TypedClientError::InvalidGroupName)?,
        )
        .await
    }

    /// Send a LISTGROUP request for the current selected group and return the completed request/response pair.
    pub async fn listgroup_current_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::listgroup_current()).await
    }

    /// Send a LISTGROUP request for the current selected group with a range filter and return the completed request/response pair.
    pub async fn listgroup_range_exchange(
        &self,
        range: ListGroupRange<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(
            Request::listgroup_range(range.as_str())
                .map_err(|_| TypedClientError::InvalidListGroupRange)?,
        )
        .await
    }

    /// Send a LISTGROUP request with explicit group and range arguments and return the completed request/response pair.
    pub async fn listgroup_group_range_exchange(
        &self,
        group: GroupName<'static>,
        range: ListGroupRange<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(
            Request::listgroup_group_range(group.as_str(), range.as_str())
                .map_err(TypedClientError::from)?,
        )
        .await
    }

    /// Send a LAST request and return the owned response frame.
    pub async fn last(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Last).await
    }

    /// Send a LAST request and return the completed request/response pair.
    pub async fn last_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Last).await
    }

    /// Send a NEXT request and return the owned response frame.
    pub async fn next(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Next).await
    }

    /// Send a NEXT request and return the completed request/response pair.
    pub async fn next_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Next).await
    }

    /// Send a NEWGROUPS request and return the owned response frame.
    pub async fn newgroups(
        &self,
        date: NntpDate<'static>,
        time: NntpTime<'static>,
        gmt: bool,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::NewGroups { date, time, gmt }).await
    }

    /// Send a NEWGROUPS request and return the completed request/response pair.
    pub async fn newgroups_exchange(
        &self,
        date: NntpDate<'static>,
        time: NntpTime<'static>,
        gmt: bool,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::NewGroups { date, time, gmt })
            .await
    }

    /// Send a NEWNEWS request and return the owned response frame.
    pub async fn newnews(
        &self,
        wildmat: Wildmat<'static>,
        date: NntpDate<'static>,
        time: NntpTime<'static>,
        gmt: bool,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::NewNews {
            wildmat,
            date,
            time,
            gmt,
        })
        .await
    }

    /// Send a NEWNEWS request and return the completed request/response pair.
    pub async fn newnews_exchange(
        &self,
        wildmat: Wildmat<'static>,
        date: NntpDate<'static>,
        time: NntpTime<'static>,
        gmt: bool,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::NewNews {
            wildmat,
            date,
            time,
            gmt,
        })
        .await
    }

    /// Send a POST request and return the owned response frame.
    pub async fn post(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Post).await
    }

    /// Send a POST request and return the completed request/response pair.
    pub async fn post_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Post).await
    }

    /// Send an IHAVE request and return the owned response frame.
    pub async fn ihave(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Ihave { message_id }).await
    }

    /// Send an IHAVE request and return the completed request/response pair.
    pub async fn ihave_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Ihave { message_id }).await
    }

    /// Send a CHECK request and return the owned response frame.
    pub async fn check(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Check { message_id }).await
    }

    /// Send a CHECK request and return the completed request/response pair.
    pub async fn check_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Check { message_id }).await
    }

    /// Send a TAKETHIS request and return the owned response frame.
    pub async fn takethis(
        &self,
        message_id: MessageId<'static>,
        article: ArticleTransfer<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::TakeThis {
            message_id,
            article,
        })
        .await
    }

    /// Send a TAKETHIS request and return the completed request/response pair.
    pub async fn takethis_exchange(
        &self,
        message_id: MessageId<'static>,
        article: ArticleTransfer<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::TakeThis {
            message_id,
            article,
        })
        .await
    }

    /// Send an AUTHINFO USER request and return the owned response frame.
    pub async fn authinfo_user(
        &self,
        value: AuthInfoValue<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::AuthInfo {
            kind: crate::protocol::AuthInfoKind::User,
            value,
        })
        .await
    }

    /// Send an AUTHINFO USER request and return the completed request/response pair.
    pub async fn authinfo_user_exchange(
        &self,
        value: AuthInfoValue<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::AuthInfo {
            kind: crate::protocol::AuthInfoKind::User,
            value,
        })
        .await
    }

    /// Send an AUTHINFO PASS request and return the owned response frame.
    pub async fn authinfo_pass(
        &self,
        value: AuthInfoValue<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::AuthInfo {
            kind: crate::protocol::AuthInfoKind::Pass,
            value,
        })
        .await
    }

    /// Send an AUTHINFO PASS request and return the completed request/response pair.
    pub async fn authinfo_pass_exchange(
        &self,
        value: AuthInfoValue<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::AuthInfo {
            kind: crate::protocol::AuthInfoKind::Pass,
            value,
        })
        .await
    }

    /// Send a STARTTLS request and return the owned response frame.
    pub async fn starttls(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::StartTls).await
    }

    /// Send a STARTTLS request and return the completed request/response pair.
    pub async fn starttls_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::StartTls).await
    }

    /// Send an OVER request and return the owned response frame.
    pub async fn over(
        &self,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Over { selector }).await
    }

    /// Send an OVER request and return the completed request/response pair.
    pub async fn over_exchange(
        &self,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Over { selector }).await
    }

    /// Send an XOVER request and return the owned response frame.
    pub async fn xover(
        &self,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Xover { selector }).await
    }

    /// Send an XOVER request and return the completed request/response pair.
    pub async fn xover_exchange(
        &self,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Xover { selector }).await
    }

    /// Send an HDR request and return the owned response frame.
    pub async fn hdr(
        &self,
        header: HeaderName<'static>,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Hdr { header, selector }).await
    }

    /// Send an HDR request and return the completed request/response pair.
    pub async fn hdr_exchange(
        &self,
        header: HeaderName<'static>,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Hdr { header, selector })
            .await
    }

    /// Send an XHDR request and return the owned response frame.
    pub async fn xhdr(
        &self,
        header: HeaderName<'static>,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Xhdr { header, selector }).await
    }

    /// Send an XHDR request and return the completed request/response pair.
    pub async fn xhdr_exchange(
        &self,
        header: HeaderName<'static>,
        selector: ArticleSelector<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Xhdr { header, selector })
            .await
    }

    /// Send a LIST request and return the owned response frame.
    pub async fn list(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::List).await
    }

    /// Send a LIST request and return the completed request/response pair.
    pub async fn list_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::List).await
    }

    /// Send a LIST ACTIVE request and return the owned response frame.
    pub async fn list_active(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::list_active()).await
    }

    /// Send a LIST ACTIVE request and return the completed request/response pair.
    pub async fn list_active_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::list_active()).await
    }

    /// Send a LIST ACTIVE [wildmat] request and return the owned response frame.
    pub async fn list_active_wildmat(
        &self,
        wildmat: Wildmat<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::ListVariant {
            kind: crate::protocol::ListKind::Active,
            wildmat: Some(wildmat),
        })
        .await
    }

    /// Send a LIST ACTIVE [wildmat] request and return the completed request/response pair.
    pub async fn list_active_wildmat_exchange(
        &self,
        wildmat: Wildmat<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::ListVariant {
            kind: crate::protocol::ListKind::Active,
            wildmat: Some(wildmat),
        })
        .await
    }

    /// Send a LIST ACTIVE.TIMES request and return the owned response frame.
    pub async fn list_active_times(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::list_active_times()).await
    }

    /// Send a LIST ACTIVE.TIMES request and return the completed request/response pair.
    pub async fn list_active_times_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::list_active_times()).await
    }

    /// Send a LIST ACTIVE.TIMES [wildmat] request and return the owned response frame.
    pub async fn list_active_times_wildmat(
        &self,
        wildmat: Wildmat<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::ListVariant {
            kind: crate::protocol::ListKind::ActiveTimes,
            wildmat: Some(wildmat),
        })
        .await
    }

    /// Send a LIST ACTIVE.TIMES [wildmat] request and return the completed request/response pair.
    pub async fn list_active_times_wildmat_exchange(
        &self,
        wildmat: Wildmat<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::ListVariant {
            kind: crate::protocol::ListKind::ActiveTimes,
            wildmat: Some(wildmat),
        })
        .await
    }

    /// Send a LIST NEWSGROUPS request and return the owned response frame.
    pub async fn list_newsgroups(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::list_newsgroups()).await
    }

    /// Send a LIST NEWSGROUPS request and return the completed request/response pair.
    pub async fn list_newsgroups_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::list_newsgroups()).await
    }

    /// Send a LIST NEWSGROUPS [wildmat] request and return the owned response frame.
    pub async fn list_newsgroups_wildmat(
        &self,
        wildmat: Wildmat<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::ListVariant {
            kind: crate::protocol::ListKind::Newsgroups,
            wildmat: Some(wildmat),
        })
        .await
    }

    /// Send a LIST NEWSGROUPS [wildmat] request and return the completed request/response pair.
    pub async fn list_newsgroups_wildmat_exchange(
        &self,
        wildmat: Wildmat<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::ListVariant {
            kind: crate::protocol::ListKind::Newsgroups,
            wildmat: Some(wildmat),
        })
        .await
    }

    /// Send a LIST OVERVIEW.FMT request and return the owned response frame.
    pub async fn list_overview_fmt(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::list_overview_fmt()).await
    }

    /// Send a LIST OVERVIEW.FMT request and return the completed request/response pair.
    pub async fn list_overview_fmt_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::list_overview_fmt()).await
    }

    /// Send a LIST HEADERS request and return the owned response frame.
    pub async fn list_headers(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::list_headers()).await
    }

    /// Send a LIST HEADERS request and return the completed request/response pair.
    pub async fn list_headers_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::list_headers()).await
    }

    /// Send a LIST DISTRIB.PATS request and return the owned response frame.
    pub async fn list_distrib_pats(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::list_distrib_pats()).await
    }

    /// Send a LIST DISTRIB.PATS request and return the completed request/response pair.
    pub async fn list_distrib_pats_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::list_distrib_pats()).await
    }

    /// Send a HELP request and return the owned response frame.
    pub async fn help(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Help).await
    }

    /// Send a HELP request and return the completed request/response pair.
    pub async fn help_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Help).await
    }

    /// Send a CAPABILITIES request and return the owned response frame.
    pub async fn capabilities(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Capabilities).await
    }

    /// Send a CAPABILITIES request and return the completed request/response pair.
    pub async fn capabilities_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Capabilities).await
    }

    /// Send a DATE request and return the owned response frame.
    pub async fn date(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Date).await
    }

    /// Send a DATE request and return the completed request/response pair.
    pub async fn date_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Date).await
    }

    /// Send a MODE READER request and return the owned response frame.
    pub async fn mode_reader(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::ModeReader).await
    }

    /// Send a MODE READER request and return the completed request/response pair.
    pub async fn mode_reader_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::ModeReader).await
    }

    /// Send a QUIT request and return the owned response frame.
    pub async fn quit(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Quit).await
    }

    /// Send a QUIT request and return the completed request/response pair.
    pub async fn quit_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Quit).await
    }

    /// Execute a typed request on this connection.
    pub async fn execute(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.queue_request(request).await?.receive().await
    }

    pub(crate) async fn queue_request(
        &self,
        request: Request<'static>,
    ) -> Result<PendingResponse, TypedClientError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .request_tx
            .send(QueuedRequest {
                request,
                response_tx,
            })
            .await
            .map_err(|_| TypedClientError::ConnectionClosed)?;

        Ok(PendingResponse {
            inner: self.inner.clone(),
            response_rx,
        })
    }

    pub(crate) async fn queue_request_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<PendingExchange, TypedClientError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .request_tx
            .send(QueuedRequest {
                request,
                response_tx,
            })
            .await
            .map_err(|_| TypedClientError::ConnectionClosed)?;

        Ok(PendingExchange {
            inner: self.inner.clone(),
            response_rx,
        })
    }

    /// Execute a typed request and return the completed request/response pair.
    pub async fn execute_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.queue_request_exchange(request).await?.receive().await
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        self.writer_task.abort();
        self.reader_task.abort();
    }
}

#[derive(Debug)]
struct ConnectionHandle {
    request_tx: mpsc::Sender<QueuedRequest>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
    writer_task: JoinHandle<()>,
    reader_task: JoinHandle<()>,
}

/// Owned response bytes for the typed client path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedResponse {
    kind: RequestKind,
    status: StatusCode,
    bytes: Bytes,
}

impl OwnedResponse {
    /// Request kind that produced this response.
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        self.kind
    }

    /// Parsed status code from the response status line.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Raw response bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Parse the response as an ARTICLE/HEAD/BODY/STAT article-style frame.
    pub fn parse_article(&self) -> Result<Article<'_>, ArticleParseError> {
        Article::parse(&self.bytes)
    }
}

/// Owned request/response pair for the typed connection surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedExchange {
    request: Request<'static>,
    response: OwnedResponse,
}

impl OwnedExchange {
    /// Original typed request.
    #[must_use]
    pub const fn request(&self) -> &Request<'static> {
        &self.request
    }

    /// Completed owned response.
    #[must_use]
    pub const fn response(&self) -> &OwnedResponse {
        &self.response
    }

    /// Consume the exchange into its original request and completed response.
    #[must_use]
    pub fn into_parts(self) -> (Request<'static>, OwnedResponse) {
        (self.request, self.response)
    }
}

/// Owned typed article-style response that reparses the zero-copy article view on demand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedArticle {
    response: OwnedResponse,
}

impl OwnedArticle {
    /// Request kind that produced this article-style response.
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        self.response.kind()
    }

    /// Parsed status code from the response status line.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.response.status()
    }

    /// Raw response bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.response.as_bytes()
    }

    /// Borrow the parsed article/body view from the owned wire bytes.
    pub fn article(&self) -> Result<Article<'_>, ArticleParseError> {
        self.response.parse_article()
    }

    /// Borrow the underlying raw response wrapper.
    #[must_use]
    pub const fn response(&self) -> &OwnedResponse {
        &self.response
    }

    /// Consume the typed article-style wrapper and return the raw response.
    #[must_use]
    pub fn into_response(self) -> OwnedResponse {
        self.response
    }
}

/// Owned request/article-response pair for the high-level typed client surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedArticleExchange {
    request: Request<'static>,
    article: OwnedArticle,
}

impl OwnedArticleExchange {
    /// Original typed request.
    #[must_use]
    pub const fn request(&self) -> &Request<'static> {
        &self.request
    }

    /// Completed typed article-style response.
    #[must_use]
    pub const fn article(&self) -> &OwnedArticle {
        &self.article
    }

    /// Consume the exchange into its original request and typed article-style response.
    #[must_use]
    pub fn into_parts(self) -> (Request<'static>, OwnedArticle) {
        (self.request, self.article)
    }
}

impl TryFrom<OwnedExchange> for OwnedArticleExchange {
    type Error = TypedClientError;

    fn try_from(exchange: OwnedExchange) -> Result<Self, Self::Error> {
        let article = OwnedArticle::try_from(exchange.response)?;
        Ok(Self {
            request: exchange.request,
            article,
        })
    }
}

impl TryFrom<OwnedResponse> for OwnedArticle {
    type Error = TypedClientError;

    fn try_from(response: OwnedResponse) -> Result<Self, Self::Error> {
        if let Err(source) = response.parse_article() {
            return Err(TypedClientError::UnexpectedArticleResponse { response, source });
        }

        Ok(Self { response })
    }
}

/// Errors from the typed client prototype.
#[derive(Debug)]
pub enum TypedClientError {
    Io(io::Error),
    UnexpectedEof,
    InvalidStatusLine,
    InvalidMessageId,
    MissingMessageId,
    InvalidGroupName,
    MissingGroupName,
    InvalidWildmat,
    MissingWildmat,
    InvalidDate,
    MissingDate,
    InvalidTime,
    MissingTime,
    InvalidAuthInfoValue,
    MissingAuthInfoValue,
    MissingArticleBody,
    InvalidHeaderName,
    MissingHeaderName,
    InvalidArticleSelector,
    MissingArticleSelector,
    InvalidListGroupRange,
    ConnectionClosed,
    UnexpectedArticleResponse {
        response: OwnedResponse,
        source: ArticleParseError,
    },
}

impl fmt::Display for TypedClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::UnexpectedEof => write!(f, "server closed before completing response"),
            Self::InvalidStatusLine => write!(f, "invalid NNTP status line"),
            Self::InvalidMessageId => write!(f, "invalid message-id"),
            Self::MissingMessageId => write!(f, "message-id is required for this request"),
            Self::InvalidGroupName => write!(f, "invalid group name"),
            Self::MissingGroupName => write!(f, "group is required for this request"),
            Self::InvalidWildmat => write!(f, "invalid wildmat"),
            Self::MissingWildmat => write!(f, "wildmat is required for this request"),
            Self::InvalidDate => write!(f, "invalid NNTP date"),
            Self::MissingDate => write!(f, "date is required for this request"),
            Self::InvalidTime => write!(f, "invalid NNTP time"),
            Self::MissingTime => write!(f, "time is required for this request"),
            Self::InvalidAuthInfoValue => write!(f, "invalid authinfo value"),
            Self::MissingAuthInfoValue => write!(f, "authinfo value is required for this request"),
            Self::MissingArticleBody => write!(f, "article body is required for this request"),
            Self::InvalidHeaderName => write!(f, "invalid header name"),
            Self::MissingHeaderName => write!(f, "header is required for this request"),
            Self::InvalidArticleSelector => write!(f, "invalid article selector"),
            Self::MissingArticleSelector => {
                write!(f, "article selector is required for this request")
            }
            Self::InvalidListGroupRange => write!(f, "invalid LISTGROUP range"),
            Self::ConnectionClosed => write!(f, "connection engine closed"),
            Self::UnexpectedArticleResponse { response, source } => write!(
                f,
                "unexpected article response status {}: {source}",
                response.status().as_u16()
            ),
        }
    }
}

impl std::error::Error for TypedClientError {}

impl From<io::Error> for TypedClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<SharedEngineError> for TypedClientError {
    fn from(value: SharedEngineError) -> Self {
        match value {
            SharedEngineError::UnexpectedEof => Self::UnexpectedEof,
            SharedEngineError::InvalidStatusLine => Self::InvalidStatusLine,
            SharedEngineError::ConnectionClosed => Self::ConnectionClosed,
            SharedEngineError::Io { kind, message } => Self::Io(io::Error::new(kind, message)),
        }
    }
}

impl From<crate::protocol::InvalidHeaderQuery> for TypedClientError {
    fn from(value: crate::protocol::InvalidHeaderQuery) -> Self {
        match value {
            crate::protocol::InvalidHeaderQuery::Header(_) => Self::InvalidHeaderName,
            crate::protocol::InvalidHeaderQuery::Selector(_) => Self::InvalidArticleSelector,
        }
    }
}

impl From<crate::protocol::InvalidDiscoveryArguments> for TypedClientError {
    fn from(value: crate::protocol::InvalidDiscoveryArguments) -> Self {
        match value {
            crate::protocol::InvalidDiscoveryArguments::Wildmat(_) => Self::InvalidWildmat,
            crate::protocol::InvalidDiscoveryArguments::Date(_) => Self::InvalidDate,
            crate::protocol::InvalidDiscoveryArguments::Time(_) => Self::InvalidTime,
        }
    }
}

impl From<crate::protocol::InvalidAuthInfoValue> for TypedClientError {
    fn from(_: crate::protocol::InvalidAuthInfoValue) -> Self {
        Self::InvalidAuthInfoValue
    }
}

impl From<crate::protocol::InvalidListGroupRange> for TypedClientError {
    fn from(_: crate::protocol::InvalidListGroupRange) -> Self {
        Self::InvalidListGroupRange
    }
}

impl From<crate::protocol::InvalidListGroupRangeOrGroupName> for TypedClientError {
    fn from(value: crate::protocol::InvalidListGroupRangeOrGroupName) -> Self {
        match value {
            crate::protocol::InvalidListGroupRangeOrGroupName::Range(_) => {
                Self::InvalidListGroupRange
            }
            crate::protocol::InvalidListGroupRangeOrGroupName::GroupName(_) => {
                Self::InvalidGroupName
            }
        }
    }
}

#[derive(Debug)]
struct ResponseDecoder {
    inner: ResponseFrameDecoder,
}

impl ResponseDecoder {
    fn new(kind: RequestKind) -> Self {
        Self {
            inner: ResponseFrameDecoder::new(kind),
        }
    }

    fn push(&mut self, buffer: &[u8]) -> Result<DecodeProgress, TypedClientError> {
        match self.inner.decode(buffer) {
            ResponseFrameParse::Complete(response) => Ok(DecodeProgress::Complete {
                status: response.status(),
                consumed: response.consumed(),
            }),
            ResponseFrameParse::NeedMore => Ok(DecodeProgress::NeedMore),
            ResponseFrameParse::Invalid => Err(TypedClientError::InvalidStatusLine),
        }
    }
}

#[derive(Debug)]
enum DecodeProgress {
    NeedMore,
    Complete { status: StatusCode, consumed: usize },
}

#[derive(Debug)]
struct StreamingResponseDecoder {
    kind: RequestKind,
    status: Option<StatusCode>,
    status_buf: [u8; STREAMING_STATUS_LINE_BYTES],
    status_len: usize,
    content_started: bool,
    empty_terminator: EmptyMultilineTerminator,
    tail: MultilineTerminatorDetector,
}

impl StreamingResponseDecoder {
    fn new(kind: RequestKind) -> Self {
        Self {
            kind,
            status: None,
            status_buf: [0; STREAMING_STATUS_LINE_BYTES],
            status_len: 0,
            content_started: false,
            empty_terminator: EmptyMultilineTerminator::default(),
            tail: MultilineTerminatorDetector::default(),
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<StreamingDecodeProgress, TypedClientError> {
        let mut content_start = 0;
        let status = match self.status {
            Some(status) => status,
            None => {
                let mut consumed = 0;
                while consumed < chunk.len() {
                    if self.status_len == self.status_buf.len() {
                        return Err(TypedClientError::InvalidStatusLine);
                    }
                    self.status_buf[self.status_len] = chunk[consumed];
                    self.status_len += 1;
                    consumed += 1;

                    match crate::protocol::ResponseInitial::parse(
                        self.kind,
                        &self.status_buf[..self.status_len],
                    ) {
                        ResponseInitialParse::Complete(initial) => {
                            let status = initial.status();
                            self.status = Some(status);
                            if !initial.descriptor().framing().is_multiline() {
                                return Ok(StreamingDecodeProgress::Complete { status, consumed });
                            }
                            content_start = consumed;
                            break;
                        }
                        ResponseInitialParse::NeedMore => {}
                        ResponseInitialParse::Invalid => {
                            return Err(TypedClientError::InvalidStatusLine);
                        }
                    }
                }

                let Some(status) = self.status else {
                    return Ok(StreamingDecodeProgress::NeedMore {
                        consumed: chunk.len(),
                    });
                };
                status
            }
        };

        if content_start >= chunk.len() {
            return Ok(StreamingDecodeProgress::NeedMore {
                consumed: chunk.len(),
            });
        }

        let content_chunk = &chunk[content_start..];
        if !self.content_started || self.empty_terminator.is_active() {
            match self.empty_terminator.detect(content_chunk) {
                EmptyTerminatorStatus::FoundAt(end) => {
                    return Ok(StreamingDecodeProgress::Complete {
                        status,
                        consumed: content_start + end,
                    });
                }
                EmptyTerminatorStatus::NeedMore => {
                    return Ok(StreamingDecodeProgress::NeedMore {
                        consumed: chunk.len(),
                    });
                }
                EmptyTerminatorStatus::NotFound {
                    previous_prefix_len,
                } => {
                    self.content_started = true;
                    if previous_prefix_len != 0 {
                        self.tail.update(&DOT_TERMINATOR[..previous_prefix_len]);
                    }
                }
            }
        }

        match detect_streaming_terminator(&self.tail, content_chunk) {
            Some(end) => Ok(StreamingDecodeProgress::Complete {
                status,
                consumed: content_start + end,
            }),
            None => {
                self.content_started = true;
                self.tail.update(content_chunk);
                Ok(StreamingDecodeProgress::NeedMore {
                    consumed: chunk.len(),
                })
            }
        }
    }
}

fn detect_streaming_terminator(tail: &MultilineTerminatorDetector, chunk: &[u8]) -> Option<usize> {
    match (
        tail.find_spanning_terminator(chunk),
        find_in_chunk_terminator(chunk),
    ) {
        (Some(spanning), Some(in_chunk)) => Some(spanning.min(in_chunk)),
        (Some(spanning), None) => Some(spanning),
        (None, Some(in_chunk)) => Some(in_chunk),
        (None, None) => None,
    }
}

fn find_in_chunk_terminator(chunk: &[u8]) -> Option<usize> {
    memchr::memchr_iter(b'.', chunk).find_map(|dot| {
        if dot >= crate::CRLF.len()
            && dot + crate::CRLF.len() < chunk.len()
            && chunk[dot - 2] == b'\r'
            && chunk[dot - 1] == b'\n'
            && chunk[dot + 1] == b'\r'
            && chunk[dot + 2] == b'\n'
        {
            Some(dot + DOT_TERMINATOR.len())
        } else {
            None
        }
    })
}

#[derive(Debug)]
enum StreamingDecodeProgress {
    NeedMore { consumed: usize },
    Complete { status: StatusCode, consumed: usize },
}

#[doc(hidden)]
pub fn bench_streaming_decode_response(
    kind: RequestKind,
    response: &[u8],
) -> Result<(StatusCode, usize), TypedClientError> {
    let mut decoder = StreamingResponseDecoder::new(kind);
    match decoder.push(response)? {
        StreamingDecodeProgress::Complete { status, consumed } => Ok((status, consumed)),
        StreamingDecodeProgress::NeedMore { .. } => Err(TypedClientError::UnexpectedEof),
    }
}

#[derive(Debug)]
struct QueuedRequest {
    request: Request<'static>,
    response_tx: oneshot::Sender<Result<CompletedRequest, SharedEngineError>>,
}

#[derive(Debug)]
pub(crate) struct PendingResponse {
    inner: Arc<ConnectionHandle>,
    response_rx: oneshot::Receiver<Result<CompletedRequest, SharedEngineError>>,
}

impl PendingResponse {
    pub(crate) async fn receive(self) -> Result<OwnedResponse, TypedClientError> {
        let response = match self.response_rx.await {
            Ok(response) => response,
            Err(_) => {
                return Err(self
                    .inner
                    .poisoned
                    .lock()
                    .await
                    .clone()
                    .map(TypedClientError::from)
                    .unwrap_or(TypedClientError::ConnectionClosed));
            }
        };

        match response.map_err(TypedClientError::from)?.response {
            CompletedResponse::Owned(response) => Ok(response),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PendingExchange {
    inner: Arc<ConnectionHandle>,
    response_rx: oneshot::Receiver<Result<CompletedRequest, SharedEngineError>>,
}

impl PendingExchange {
    pub(crate) async fn receive(self) -> Result<OwnedExchange, TypedClientError> {
        let response = match self.response_rx.await {
            Ok(response) => response,
            Err(_) => {
                return Err(self
                    .inner
                    .poisoned
                    .lock()
                    .await
                    .clone()
                    .map(TypedClientError::from)
                    .unwrap_or(TypedClientError::ConnectionClosed));
            }
        };

        let completed = response.map_err(TypedClientError::from)?;
        match completed.response {
            CompletedResponse::Owned(response) => Ok(OwnedExchange {
                request: completed.request,
                response,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainedResponse {
    status: StatusCode,
    bytes_len: usize,
}

impl DrainedResponse {
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    pub(crate) fn bytes_len(&self) -> usize {
        self.bytes_len
    }
}

#[derive(Debug)]
pub(crate) struct DrainedResponseFrame<'a> {
    status: StatusCode,
    bytes: &'a [u8],
}

impl<'a> DrainedResponseFrame<'a> {
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    pub(crate) fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

pub(crate) struct DrainedResponseReader {
    pending_read: Box<[u8; DRAINED_PENDING_READ_BYTES]>,
    pending_len: usize,
    pending_start: usize,
    read_chunk_bytes: usize,
}

impl DrainedResponseReader {
    pub(crate) fn new(read_chunk_bytes: usize) -> Self {
        Self {
            pending_read: Box::new([0; DRAINED_PENDING_READ_BYTES]),
            pending_len: 0,
            pending_start: 0,
            read_chunk_bytes: read_chunk_bytes.min(DRAINED_PENDING_READ_BYTES),
        }
    }

    pub(crate) async fn read_response<R>(
        &mut self,
        reader: &mut R,
        kind: RequestKind,
    ) -> Result<DrainedResponse, TypedClientError>
    where
        R: AsyncRead + Unpin,
    {
        let mut decoder = StreamingResponseDecoder::new(kind);
        let mut bytes_len = 0usize;

        loop {
            if self.pending_start < self.pending_len {
                match decoder.push(&self.pending_read[self.pending_start..self.pending_len]) {
                    Ok(StreamingDecodeProgress::NeedMore { consumed }) => {
                        bytes_len += consumed;
                        self.pending_start += consumed;
                        if self.pending_start == self.pending_len {
                            self.pending_len = 0;
                            self.pending_start = 0;
                        }
                    }
                    Ok(StreamingDecodeProgress::Complete { status, consumed }) => {
                        bytes_len += consumed;
                        self.pending_start += consumed;
                        if self.pending_start == self.pending_len {
                            self.pending_len = 0;
                            self.pending_start = 0;
                        }

                        return Ok(DrainedResponse { status, bytes_len });
                    }
                    Err(err) => return Err(err),
                }
            }

            if self.pending_len == self.pending_read.len() {
                compact_drained_pending_read(
                    &mut self.pending_read,
                    &mut self.pending_start,
                    &mut self.pending_len,
                );
                if self.pending_len == self.pending_read.len() {
                    return Err(TypedClientError::InvalidStatusLine);
                }
            }

            let read_len = self
                .read_chunk_bytes
                .min(self.pending_read.len() - self.pending_len);
            let read = reader
                .read(&mut self.pending_read[self.pending_len..self.pending_len + read_len])
                .await
                .map_err(TypedClientError::Io)?;

            if read == 0 {
                return Err(TypedClientError::UnexpectedEof);
            }

            self.pending_len += read;
        }
    }

    pub(crate) async fn read_response_frame<R>(
        &mut self,
        reader: &mut R,
        kind: RequestKind,
    ) -> Result<DrainedResponseFrame<'_>, TypedClientError>
    where
        R: AsyncRead + Unpin,
    {
        let mut frame_start = self.pending_start;

        loop {
            if frame_start < self.pending_len {
                match ResponseDecoder::new(kind)
                    .push(&self.pending_read[frame_start..self.pending_len])
                {
                    Ok(DecodeProgress::NeedMore) => {}
                    Ok(DecodeProgress::Complete { status, consumed }) => {
                        let frame_end = frame_start + consumed;
                        self.pending_start = frame_end;
                        if self.pending_start == self.pending_len {
                            self.pending_len = 0;
                            self.pending_start = 0;
                        }
                        return Ok(DrainedResponseFrame {
                            status,
                            bytes: &self.pending_read[frame_start..frame_end],
                        });
                    }
                    Err(err) => return Err(err),
                }
            }

            if self.pending_len == self.pending_read.len() {
                if frame_start == 0 {
                    return Err(TypedClientError::InvalidStatusLine);
                }
                let frame_len = self.pending_len - frame_start;
                self.pending_read
                    .copy_within(frame_start..self.pending_len, 0);
                self.pending_start = 0;
                self.pending_len = frame_len;
                frame_start = 0;
            }

            let read_len = self
                .read_chunk_bytes
                .min(self.pending_read.len() - self.pending_len);
            let read = reader
                .read(&mut self.pending_read[self.pending_len..self.pending_len + read_len])
                .await
                .map_err(TypedClientError::Io)?;

            if read == 0 {
                return Err(TypedClientError::UnexpectedEof);
            }

            self.pending_len += read;
        }
    }
}

#[derive(Debug)]
enum CompletedResponse {
    Owned(OwnedResponse),
}

#[derive(Debug)]
struct CompletedRequest {
    request: Request<'static>,
    response: CompletedResponse,
}

#[derive(Debug)]
struct InFlightRequest {
    request: Request<'static>,
    kind: RequestKind,
    response_tx: oneshot::Sender<Result<CompletedRequest, SharedEngineError>>,
}

#[derive(Debug, Clone)]
enum SharedEngineError {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    UnexpectedEof,
    InvalidStatusLine,
    ConnectionClosed,
}

async fn run_writer_task(
    mut writer: OwnedWriteHalf,
    mut request_rx: mpsc::Receiver<QueuedRequest>,
    inflight_tx: mpsc::Sender<InFlightRequest>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
) {
    while let Some(queued) = request_rx.recv().await {
        let kind = queued.request.kind();
        if let Err(err) = write_request_wire(&mut writer, &queued.request).await {
            let error = SharedEngineError::Io {
                kind: err.kind(),
                message: err.to_string(),
            };
            let _ = queued.response_tx.send(Err(error.clone()));
            poison_writer_engine(&poisoned, &mut request_rx, error).await;
            return;
        }

        let inflight = InFlightRequest {
            request: queued.request,
            kind,
            response_tx: queued.response_tx,
        };
        if let Err(err) = inflight_tx.send(inflight).await {
            let error = SharedEngineError::ConnectionClosed;
            let _ = err.0.response_tx.send(Err(error.clone()));
            poison_writer_engine(&poisoned, &mut request_rx, error).await;
            break;
        }
    }
}

pub(crate) async fn write_request_wire<W>(
    writer: &mut W,
    request: &Request<'static>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match request {
        Request::Article { article_ref } => {
            write_article_ref_request_wire(writer, b"ARTICLE", article_ref).await
        }
        Request::Body { article_ref } => {
            write_article_ref_request_wire(writer, b"BODY", article_ref).await
        }
        Request::Head { article_ref } => {
            write_article_ref_request_wire(writer, b"HEAD", article_ref).await
        }
        Request::Stat { article_ref } => {
            write_article_ref_request_wire(writer, b"STAT", article_ref).await
        }
        Request::ListVariant { kind, wildmat } => {
            write_list_request_wire(writer, *kind, wildmat.as_ref()).await
        }
        Request::Group { group } => {
            write_one_arg_request_wire(writer, b"GROUP ", group.as_str()).await
        }
        Request::ListGroup { group, range } => {
            write_listgroup_request_wire(writer, group.as_ref(), range.as_ref()).await
        }
        Request::Last => write_simple_request_wire(writer, b"LAST").await,
        Request::Next => write_simple_request_wire(writer, b"NEXT").await,
        Request::Over { selector } => {
            write_one_arg_request_wire(writer, b"OVER ", selector.as_str()).await
        }
        Request::Xover { selector } => {
            write_one_arg_request_wire(writer, b"XOVER ", selector.as_str()).await
        }
        Request::Hdr { header, selector } => {
            write_two_arg_request_wire(writer, b"HDR ", header.as_str(), selector.as_str()).await
        }
        Request::Xhdr { header, selector } => {
            write_two_arg_request_wire(writer, b"XHDR ", header.as_str(), selector.as_str()).await
        }
        Request::NewGroups { date, time, gmt } => {
            write_datetime_request_wire(writer, b"NEWGROUPS ", date.as_str(), time.as_str(), *gmt)
                .await
        }
        Request::NewNews {
            wildmat,
            date,
            time,
            gmt,
        } => {
            write_newnews_request_wire(writer, wildmat.as_str(), date.as_str(), time.as_str(), *gmt)
                .await
        }
        Request::Post => write_simple_request_wire(writer, b"POST").await,
        Request::Ihave { message_id } => {
            write_message_id_request_wire(writer, b"IHAVE ", message_id).await
        }
        Request::Check { message_id } => {
            write_message_id_request_wire(writer, b"CHECK ", message_id).await
        }
        Request::TakeThis {
            message_id,
            article,
        } => write_takethis_request_wire(writer, message_id, article).await,
        Request::AuthInfo { kind, value } => {
            write_authinfo_request_wire(writer, *kind, value.as_str()).await
        }
        Request::StartTls => write_simple_request_wire(writer, b"STARTTLS").await,
        Request::List => write_simple_request_wire(writer, b"LIST").await,
        Request::Help => write_simple_request_wire(writer, b"HELP").await,
        Request::Capabilities => write_simple_request_wire(writer, b"CAPABILITIES").await,
        Request::Date => write_simple_request_wire(writer, b"DATE").await,
        Request::ModeReader => write_simple_request_wire(writer, b"MODE READER").await,
        Request::Quit => write_simple_request_wire(writer, b"QUIT").await,
    }
}

pub(crate) async fn write_article_request_wire<W>(
    writer: &mut W,
    article_ref: &ArticleRef<'_>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_article_ref_request_wire(writer, b"ARTICLE", article_ref).await
}

#[doc(hidden)]
pub async fn bench_write_request_wire_to_sink(request: &Request<'static>) -> io::Result<()> {
    let mut writer = tokio::io::sink();
    write_request_wire(&mut writer, request).await
}

async fn write_article_ref_request_wire<W>(
    writer: &mut W,
    verb: &[u8],
    article_ref: &ArticleRef<'_>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match article_ref {
        ArticleRef::Current => {
            write_slices(writer, &mut [IoSlice::new(verb), IoSlice::new(crate::CRLF)]).await
        }
        ArticleRef::Number(number) => {
            let mut number_buf = arrayvec::ArrayString::<20>::new();
            write!(&mut number_buf, "{number}").map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid article number")
            })?;
            write_slices(
                writer,
                &mut [
                    IoSlice::new(verb),
                    IoSlice::new(b" "),
                    IoSlice::new(number_buf.as_bytes()),
                    IoSlice::new(crate::CRLF),
                ],
            )
            .await
        }
        ArticleRef::MessageId(message_id) => {
            write_slices(
                writer,
                &mut [
                    IoSlice::new(verb),
                    IoSlice::new(b" "),
                    IoSlice::new(message_id.as_str().as_bytes()),
                    IoSlice::new(crate::CRLF),
                ],
            )
            .await
        }
    }
}

async fn write_message_id_request_wire<W>(
    writer: &mut W,
    verb: &[u8],
    message_id: &MessageId<'_>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_slices(
        writer,
        &mut [
            IoSlice::new(verb),
            IoSlice::new(message_id.as_str().as_bytes()),
            IoSlice::new(crate::CRLF),
        ],
    )
    .await
}

async fn write_one_arg_request_wire<W>(writer: &mut W, verb: &[u8], arg: &str) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_slices(
        writer,
        &mut [
            IoSlice::new(verb),
            IoSlice::new(arg.as_bytes()),
            IoSlice::new(crate::CRLF),
        ],
    )
    .await
}

async fn write_two_arg_request_wire<W>(
    writer: &mut W,
    verb: &[u8],
    left: &str,
    right: &str,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_slices(
        writer,
        &mut [
            IoSlice::new(verb),
            IoSlice::new(left.as_bytes()),
            IoSlice::new(b" "),
            IoSlice::new(right.as_bytes()),
            IoSlice::new(crate::CRLF),
        ],
    )
    .await
}

async fn write_listgroup_request_wire<W>(
    writer: &mut W,
    group: Option<&GroupName<'_>>,
    range: Option<&ListGroupRange<'_>>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match (group, range) {
        (None, None) => write_simple_request_wire(writer, b"LISTGROUP").await,
        (Some(group), None) => {
            write_one_arg_request_wire(writer, b"LISTGROUP ", group.as_str()).await
        }
        (None, Some(range)) => {
            write_one_arg_request_wire(writer, b"LISTGROUP ", range.as_str()).await
        }
        (Some(group), Some(range)) => {
            write_two_arg_request_wire(writer, b"LISTGROUP ", group.as_str(), range.as_str()).await
        }
    }
}

async fn write_datetime_request_wire<W>(
    writer: &mut W,
    verb: &[u8],
    date: &str,
    time: &str,
    gmt: bool,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let suffix = if gmt {
        b" GMT\r\n".as_slice()
    } else {
        crate::CRLF
    };
    write_slices(
        writer,
        &mut [
            IoSlice::new(verb),
            IoSlice::new(date.as_bytes()),
            IoSlice::new(b" "),
            IoSlice::new(time.as_bytes()),
            IoSlice::new(suffix),
        ],
    )
    .await
}

async fn write_newnews_request_wire<W>(
    writer: &mut W,
    wildmat: &str,
    date: &str,
    time: &str,
    gmt: bool,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let suffix = if gmt {
        b" GMT\r\n".as_slice()
    } else {
        crate::CRLF
    };
    write_slices(
        writer,
        &mut [
            IoSlice::new(b"NEWNEWS "),
            IoSlice::new(wildmat.as_bytes()),
            IoSlice::new(b" "),
            IoSlice::new(date.as_bytes()),
            IoSlice::new(b" "),
            IoSlice::new(time.as_bytes()),
            IoSlice::new(suffix),
        ],
    )
    .await
}

async fn write_list_request_wire<W>(
    writer: &mut W,
    kind: ListKind,
    wildmat: Option<&Wildmat<'_>>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match wildmat {
        Some(wildmat) => {
            write_slices(
                writer,
                &mut [
                    IoSlice::new(b"LIST "),
                    IoSlice::new(kind.as_wire()),
                    IoSlice::new(b" "),
                    IoSlice::new(wildmat.as_str().as_bytes()),
                    IoSlice::new(crate::CRLF),
                ],
            )
            .await
        }
        None => {
            write_slices(
                writer,
                &mut [
                    IoSlice::new(b"LIST "),
                    IoSlice::new(kind.as_wire()),
                    IoSlice::new(crate::CRLF),
                ],
            )
            .await
        }
    }
}

async fn write_authinfo_request_wire<W>(
    writer: &mut W,
    kind: AuthInfoKind,
    value: &str,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_slices(
        writer,
        &mut [
            IoSlice::new(b"AUTHINFO "),
            IoSlice::new(kind.as_wire()),
            IoSlice::new(b" "),
            IoSlice::new(value.as_bytes()),
            IoSlice::new(crate::CRLF),
        ],
    )
    .await
}

async fn write_simple_request_wire<W>(writer: &mut W, verb: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_slices(writer, &mut [IoSlice::new(verb), IoSlice::new(crate::CRLF)]).await
}

async fn write_takethis_request_wire<W>(
    writer: &mut W,
    message_id: &MessageId<'_>,
    article: &ArticleTransfer<'_>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_message_id_request_wire(writer, b"TAKETHIS ", message_id).await?;

    let payload = article.as_bytes();
    if payload.is_empty() {
        writer.write_all(DOT_TERMINATOR).await?;
        return Ok(());
    }

    for line in crlf_normalized_payload_lines(payload) {
        if line.starts_with(b".") {
            let mut slices = [
                IoSlice::new(b"."),
                IoSlice::new(line),
                IoSlice::new(crate::CRLF),
            ];
            write_all_vectored(writer, &mut slices).await?;
        } else {
            let mut slices = [IoSlice::new(line), IoSlice::new(crate::CRLF)];
            write_all_vectored(writer, &mut slices).await?;
        }
    }

    writer.write_all(DOT_TERMINATOR).await
}

async fn write_slices<W>(writer: &mut W, slices: &mut [IoSlice<'_>]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_all_vectored(writer, slices).await
}

async fn write_all_vectored<W>(writer: &mut W, slices: &mut [IoSlice<'_>]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining = slices;
    while !remaining.is_empty() {
        let written =
            poll_fn(|cx| Pin::new(&mut *writer).poll_write_vectored(cx, remaining)).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write buffers",
            ));
        }
        IoSlice::advance_slices(&mut remaining, written);
    }

    Ok(())
}

async fn run_reader_task(
    reader: OwnedReadHalf,
    read_chunk_bytes: usize,
    inflight_rx: mpsc::Receiver<InFlightRequest>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
    writer_abort: tokio::task::AbortHandle,
) {
    let mut reader = reader;
    let _read_chunk_bytes = read_chunk_bytes;
    let mut inflight_rx = inflight_rx;
    let mut pending_read = BytesMut::with_capacity(OWNED_RESPONSE_PREALLOC_BYTES);

    while let Some(inflight_request) = inflight_rx.recv().await {
        let InFlightRequest {
            request,
            kind,
            response_tx,
        } = inflight_request;
        let mut decoder = ResponseDecoder::new(kind);

        loop {
            if !pending_read.is_empty() {
                match decoder.push(&pending_read) {
                    Ok(DecodeProgress::NeedMore) => {}
                    Ok(DecodeProgress::Complete { status, consumed }) => {
                        let bytes = Bytes::copy_from_slice(&pending_read[..consumed]);
                        let pending_len = pending_read.len();
                        let leftover_len = pending_len - consumed;
                        pending_read.copy_within(consumed..pending_len, 0);
                        pending_read.truncate(leftover_len);
                        let response = CompletedResponse::Owned(OwnedResponse {
                            kind,
                            status,
                            bytes,
                        });
                        let _ = response_tx.send(Ok(CompletedRequest { request, response }));
                        break;
                    }
                    Err(TypedClientError::InvalidStatusLine) => {
                        let error = SharedEngineError::InvalidStatusLine;
                        let _ = response_tx.send(Err(error.clone()));
                        writer_abort.abort();
                        poison_reader_engine(&poisoned, &mut inflight_rx, error).await;
                        return;
                    }
                    Err(err) => {
                        let error = shared_engine_error_from_typed(err);
                        let _ = response_tx.send(Err(error.clone()));
                        writer_abort.abort();
                        poison_reader_engine(&poisoned, &mut inflight_rx, error).await;
                        return;
                    }
                }
            }

            if pending_read.capacity() == pending_read.len() {
                let error = SharedEngineError::InvalidStatusLine;
                let _ = response_tx.send(Err(error.clone()));
                writer_abort.abort();
                poison_reader_engine(&poisoned, &mut inflight_rx, error).await;
                return;
            }

            let read = match reader.read_buf(&mut pending_read).await {
                Ok(read) => read,
                Err(err) => {
                    let error = SharedEngineError::Io {
                        kind: err.kind(),
                        message: err.to_string(),
                    };
                    let _ = response_tx.send(Err(error.clone()));
                    writer_abort.abort();
                    poison_reader_engine(&poisoned, &mut inflight_rx, error).await;
                    return;
                }
            };

            if read == 0 {
                let error = SharedEngineError::UnexpectedEof;
                let _ = response_tx.send(Err(error.clone()));
                writer_abort.abort();
                poison_reader_engine(&poisoned, &mut inflight_rx, error).await;
                return;
            }
        }
    }
}

fn compact_drained_pending_read(
    pending_read: &mut [u8; DRAINED_PENDING_READ_BYTES],
    pending_start: &mut usize,
    pending_len: &mut usize,
) {
    if *pending_start == 0 {
        return;
    }

    if *pending_start == *pending_len {
        *pending_len = 0;
        *pending_start = 0;
        return;
    }

    let tail_len = *pending_len - *pending_start;
    pending_read.copy_within(*pending_start..*pending_len, 0);
    *pending_len = tail_len;
    *pending_start = 0;
}

async fn poison_writer_engine(
    poisoned: &Arc<Mutex<Option<SharedEngineError>>>,
    request_rx: &mut mpsc::Receiver<QueuedRequest>,
    error: SharedEngineError,
) {
    store_poisoned(poisoned, &error).await;
    while let Ok(queued) = request_rx.try_recv() {
        let _ = queued.response_tx.send(Err(error.clone()));
    }
}

async fn poison_reader_engine(
    poisoned: &Arc<Mutex<Option<SharedEngineError>>>,
    inflight_rx: &mut mpsc::Receiver<InFlightRequest>,
    error: SharedEngineError,
) {
    store_poisoned(poisoned, &error).await;
    while let Ok(request) = inflight_rx.try_recv() {
        let _ = request.response_tx.send(Err(error.clone()));
    }
}

async fn store_poisoned(
    poisoned: &Arc<Mutex<Option<SharedEngineError>>>,
    error: &SharedEngineError,
) {
    let mut guard = poisoned.lock().await;
    if guard.is_none() {
        *guard = Some(error.clone());
    }
}

fn shared_engine_error_from_typed(err: TypedClientError) -> SharedEngineError {
    match err {
        TypedClientError::Io(err) => SharedEngineError::Io {
            kind: err.kind(),
            message: err.to_string(),
        },
        TypedClientError::UnexpectedEof => SharedEngineError::UnexpectedEof,
        TypedClientError::InvalidStatusLine => SharedEngineError::InvalidStatusLine,
        TypedClientError::ConnectionClosed => SharedEngineError::ConnectionClosed,
        TypedClientError::InvalidMessageId
        | TypedClientError::MissingMessageId
        | TypedClientError::InvalidGroupName
        | TypedClientError::MissingGroupName
        | TypedClientError::InvalidWildmat
        | TypedClientError::MissingWildmat
        | TypedClientError::InvalidDate
        | TypedClientError::MissingDate
        | TypedClientError::InvalidTime
        | TypedClientError::MissingTime
        | TypedClientError::InvalidAuthInfoValue
        | TypedClientError::MissingAuthInfoValue
        | TypedClientError::MissingArticleBody
        | TypedClientError::InvalidHeaderName
        | TypedClientError::MissingHeaderName
        | TypedClientError::InvalidArticleSelector
        | TypedClientError::MissingArticleSelector
        | TypedClientError::InvalidListGroupRange
        | TypedClientError::UnexpectedArticleResponse { .. } => SharedEngineError::ConnectionClosed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    fn dangerous_wire_bytes() -> impl Strategy<Value = u8> {
        prop_oneof![
            Just(b'\r'),
            Just(b'\n'),
            Just(b'.'),
            Just(b' '),
            b'0'..=b'9',
            b'a'..=b'z',
        ]
    }

    fn terminator_end_oracle(buffer: &[u8]) -> Option<usize> {
        buffer
            .windows(crate::TERMINATOR.len())
            .position(|window| window == crate::TERMINATOR)
            .map(|start| start + crate::TERMINATOR.len())
    }

    fn remove_rfc_multiline_terminators(buffer: &mut [u8]) {
        while let Some(start) = buffer
            .windows(crate::TERMINATOR.len())
            .position(|window| window == crate::TERMINATOR)
        {
            buffer[start + 2] = b'x';
        }
    }

    fn complete_after_split(kind: RequestKind, frame: &[u8], split: usize) -> (StatusCode, usize) {
        let mut decoder = ResponseDecoder::new(kind);
        match decoder
            .push(&frame[..split])
            .expect("first decoder push should succeed")
        {
            DecodeProgress::Complete { status, consumed } => (status, consumed),
            DecodeProgress::NeedMore => match decoder
                .push(frame)
                .expect("second decoder push should succeed")
            {
                DecodeProgress::Complete { status, consumed } => (status, consumed),
                DecodeProgress::NeedMore => panic!("decoder did not complete at split {split}"),
            },
        }
    }

    fn assert_decoder_completes_on_all_three_push_schedules(
        kind: RequestKind,
        frame: &[u8],
        expected_status: u16,
        expected_consumed: usize,
    ) {
        for first in 0..=frame.len() {
            for second in first..=frame.len() {
                let mut decoder = ResponseDecoder::new(kind);
                for prefix_len in [first, second, frame.len()] {
                    let progress = decoder
                        .push(&frame[..prefix_len])
                        .expect("decoder push should succeed");
                    if prefix_len < expected_consumed {
                        assert!(
                            matches!(progress, DecodeProgress::NeedMore),
                            "completed before frame end: first {first} second {second} prefix {prefix_len} frame {frame:?}",
                        );
                    } else {
                        let DecodeProgress::Complete { status, consumed } = progress else {
                            panic!(
                                "decoder did not complete: first {first} second {second} prefix {prefix_len} frame {frame:?}"
                            );
                        };
                        assert_eq!(status.as_u16(), expected_status);
                        assert_eq!(consumed, expected_consumed);
                        break;
                    }
                }
            }
        }
    }

    async fn assert_read_request(stream: &mut tokio::net::TcpStream, expected: &[u8]) {
        let mut request = vec![0_u8; expected.len()];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(request, expected);
    }

    fn response_from_bytes(kind: RequestKind, status: StatusCode, bytes: &[u8]) -> OwnedResponse {
        OwnedResponse {
            kind,
            status,
            bytes: Bytes::copy_from_slice(bytes),
        }
    }

    #[test]
    fn decoder_completes_single_line_error_without_waiting_for_terminator() {
        // RFC 3977 section 3.1 says the response initial line is CRLF-terminated.
        // Error statuses for ARTICLE are single-line responses, so the decoder must stop
        // after that CRLF without waiting for any multiline terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        let mut decoder = ResponseDecoder::new(RequestKind::Article);
        let DecodeProgress::Complete { status, consumed } = decoder
            .push(b"430 no article with that message-id\r\n")
            .unwrap()
        else {
            panic!("decoder should complete");
        };
        let response = response_from_bytes(
            RequestKind::Article,
            status,
            b"430 no article with that message-id\r\n",
        );

        assert_eq!(consumed, b"430 no article with that message-id\r\n".len());
        assert_eq!(response.kind(), RequestKind::Article);
        assert_eq!(response.status().as_u16(), 430);
        assert_eq!(
            response.as_bytes(),
            b"430 no article with that message-id\r\n"
        );
    }

    #[test]
    fn decoder_compact_frames_do_not_allocate() {
        // RFC 3977 section 9.4 frames responses as either an initial response
        // line alone or that line followed by a multi-line data block.
        let mut stat_decoder = ResponseDecoder::new(RequestKind::Stat);
        let mut body_decoder = ResponseDecoder::new(RequestKind::Body);

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

        assert!(matches!(
            stat_decoder.push(b"223 1 <stat@test> article retrieved\r\n"),
            Ok(DecodeProgress::Complete { status, consumed })
                if status.as_u16() == 223
                    && consumed == b"223 1 <stat@test> article retrieved\r\n".len()
        ));
        assert!(matches!(
            body_decoder.push(b"222 1 <body@test> body follows\r\nbody\r\n.\r\n"),
            Ok(DecodeProgress::Complete { status, consumed })
                if status.as_u16() == 222
                    && consumed == b"222 1 <body@test> body follows\r\nbody\r\n.\r\n".len()
        ));

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        let allocations = crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(allocations, 0, "compact decoder push allocated");
    }

    #[test]
    fn decoder_waits_for_complete_crlf_status_line() {
        // RFC 3977 section 3.1 requires CRLF, not a lone final CR, to terminate the
        // response initial line. The decoder must keep waiting until LF arrives:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        let mut decoder = ResponseDecoder::new(RequestKind::Article);
        assert!(matches!(
            decoder
                .push(b"430 no article with that message-id\r")
                .unwrap(),
            DecodeProgress::NeedMore
        ));

        let DecodeProgress::Complete { status, consumed } = decoder
            .push(b"430 no article with that message-id\r\n")
            .unwrap()
        else {
            panic!("decoder should complete once CRLF arrives");
        };

        assert_eq!(consumed, b"430 no article with that message-id\r\n".len());
        assert_eq!(status.as_u16(), 430);
    }

    #[test]
    fn decoder_rejects_bare_lf_status_line() {
        // RFC 3977 section 3.1 defines response lines as CRLF-terminated.
        // A bare LF before CRLF is malformed and must not be treated as a line ending:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for input in [
            b"430 no article with that message-id\n".as_slice(),
            b"430 no article with that message-id\nextra\r\n".as_slice(),
            b"430 no article with that message-id\n\r\n".as_slice(),
        ] {
            assert!(
                matches!(
                    ResponseDecoder::new(RequestKind::Article).push(input),
                    Err(TypedClientError::InvalidStatusLine)
                ),
                "{input:?}"
            );
        }
    }

    #[test]
    fn decoder_rejects_embedded_cr_in_status_line() {
        // RFC 3977 section 3.1 gives CR meaning only as the first byte of CRLF.
        // Embedded or doubled CR before the status-line terminator is invalid:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for input in [
            b"430 no article with that message-id\r extra\r\n".as_slice(),
            b"430 no article with that message-id\r\r\n".as_slice(),
        ] {
            assert!(
                matches!(
                    ResponseDecoder::new(RequestKind::Article).push(input),
                    Err(TypedClientError::InvalidStatusLine)
                ),
                "{input:?}"
            );
        }
    }

    #[test]
    fn decoder_enforces_rfc_initial_response_line_limit() {
        // RFC 3977 section 3.1 limits the response initial line to 512 octets,
        // including the status code and terminating CRLF.
        let mut exact = Vec::from(b"223 ".as_slice());
        exact.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 2, b'x');
        exact.extend_from_slice(b"\r\n");
        assert_eq!(
            exact.len(),
            crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES
        );
        assert!(matches!(
            ResponseDecoder::new(RequestKind::Stat).push(&exact),
            Ok(DecodeProgress::Complete { .. })
        ));

        let mut too_long_complete = Vec::from(b"223 ".as_slice());
        too_long_complete.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 1, b'x');
        too_long_complete.extend_from_slice(b"\r\n");
        assert_eq!(
            too_long_complete.len(),
            crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES + 1
        );
        assert!(matches!(
            ResponseDecoder::new(RequestKind::Stat).push(&too_long_complete),
            Err(TypedClientError::InvalidStatusLine)
        ));

        let mut too_long_incomplete = Vec::from(b"223 ".as_slice());
        too_long_incomplete.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES, b'x');
        assert!(matches!(
            ResponseDecoder::new(RequestKind::Stat).push(&too_long_incomplete),
            Err(TypedClientError::InvalidStatusLine)
        ));
    }

    #[test]
    fn streaming_decoder_enforces_rfc_initial_response_line_limit() {
        // RFC 3977 section 3.1 applies the same 512-octet initial-line limit
        // when the line arrives across streaming chunks.
        let mut exact = Vec::from(b"223 ".as_slice());
        exact.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 2, b'x');
        exact.extend_from_slice(b"\r\n");
        let split = 17;
        let mut decoder = StreamingResponseDecoder::new(RequestKind::Stat);
        assert!(matches!(
            decoder.push(&exact[..split]),
            Ok(StreamingDecodeProgress::NeedMore { consumed }) if consumed == split
        ));
        assert!(matches!(
            decoder.push(&exact[split..]),
            Ok(StreamingDecodeProgress::Complete { status, consumed })
                if status.as_u16() == 223 && consumed == exact.len() - split
        ));

        let mut too_long = Vec::from(b"223 ".as_slice());
        too_long.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES, b'x');
        too_long.push(b'x');
        let mut decoder = StreamingResponseDecoder::new(RequestKind::Stat);
        assert!(matches!(
            decoder.push(&too_long),
            Err(TypedClientError::InvalidStatusLine)
        ));
    }

    #[test]
    fn decoder_completes_multiline_response_across_chunks() {
        // RFC 3977 section 3.1.1 terminates multiline data with CRLF "." CRLF.
        // The decoder must retain enough state to recognize that sequence across reads:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut decoder = ResponseDecoder::new(RequestKind::Body);
        let mut buffer = b"222 1 <a@b> body follows\r\nbody\r".to_vec();
        assert!(matches!(
            decoder.push(&buffer).unwrap(),
            DecodeProgress::NeedMore
        ));
        buffer.extend_from_slice(b"\n.\r\n");
        let DecodeProgress::Complete { status, consumed } = decoder.push(&buffer).unwrap() else {
            panic!("decoder should complete");
        };
        let response = response_from_bytes(RequestKind::Body, status, &buffer[..consumed]);

        assert_eq!(consumed, b"222 1 <a@b> body follows\r\nbody\r\n.\r\n".len());
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(
            response.as_bytes(),
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n"
        );
    }

    #[test]
    fn decoder_treats_rfc2980_xhdr_221_as_multiline() {
        // RFC 2980 section 2.1.6 specifies XHDR as a 221 multiline response.
        // The decoder must consume through the dot line so pipelined reads do
        // not leave XHDR payload bytes in the socket buffer.
        let mut decoder = ResponseDecoder::new(RequestKind::Xhdr);
        let buffer = b"221 Header follows\r\n1 Subject\r\n.\r\nNEXT";

        let DecodeProgress::Complete { status, consumed } = decoder.push(buffer).unwrap() else {
            panic!("decoder should complete");
        };
        let response = response_from_bytes(RequestKind::Xhdr, status, &buffer[..consumed]);

        assert_eq!(consumed, b"221 Header follows\r\n1 Subject\r\n.\r\n".len());
        assert_eq!(response.status().as_u16(), 221);
        assert_eq!(
            response.as_bytes(),
            b"221 Header follows\r\n1 Subject\r\n.\r\n"
        );
    }

    #[test]
    fn decoder_completes_empty_multiline_response() {
        // RFC 3977 section 3.1.1 represents an empty multiline response as "." CRLF
        // immediately after the response initial line:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut decoder = ResponseDecoder::new(RequestKind::Capabilities);
        let buffer = b"101 Capability list:\r\n.\r\n";

        let DecodeProgress::Complete { status, consumed } = decoder.push(buffer).unwrap() else {
            panic!("decoder should complete");
        };
        let response = response_from_bytes(RequestKind::Capabilities, status, &buffer[..consumed]);

        assert_eq!(consumed, buffer.len());
        assert_eq!(response.status().as_u16(), 101);
        assert_eq!(response.as_bytes(), buffer);
    }

    #[test]
    fn decoder_completes_empty_multiline_response_across_pushes() {
        // RFC 3977 section 3.1.1 allows the empty "." CRLF terminator to arrive in a
        // later read; the decoder must still treat it as content-start termination:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut decoder = ResponseDecoder::new(RequestKind::Capabilities);
        let mut buffer = b"101 Capability list:\r\n".to_vec();
        assert!(matches!(
            decoder.push(&buffer).unwrap(),
            DecodeProgress::NeedMore
        ));

        buffer.extend_from_slice(b".\r\n");
        let DecodeProgress::Complete { status, consumed } = decoder.push(&buffer).unwrap() else {
            panic!("decoder should complete");
        };
        let response = response_from_bytes(RequestKind::Capabilities, status, &buffer[..consumed]);

        assert_eq!(consumed, buffer.len());
        assert_eq!(response.status().as_u16(), 101);
        assert_eq!(response.as_bytes(), buffer);
    }

    #[test]
    fn decoder_completes_empty_multiline_response_with_split_terminator() {
        // RFC 3977 section 3.1.1 defines the empty multiline body as exactly "." CRLF.
        // This exercises all split positions inside that three-byte terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        for split in 1..3 {
            let mut decoder = ResponseDecoder::new(RequestKind::Capabilities);
            let mut buffer = b"101 Capability list:\r\n".to_vec();
            buffer.extend_from_slice(&b".\r\n"[..split]);
            assert!(matches!(
                decoder.push(&buffer).unwrap(),
                DecodeProgress::NeedMore
            ));

            buffer.extend_from_slice(&b".\r\n"[split..]);
            let DecodeProgress::Complete { status, consumed } = decoder.push(&buffer).unwrap()
            else {
                panic!("decoder should complete for split {split}");
            };
            let response =
                response_from_bytes(RequestKind::Capabilities, status, &buffer[..consumed]);

            assert_eq!(consumed, buffer.len());
            assert_eq!(response.status().as_u16(), 101);
            assert_eq!(response.as_bytes(), buffer);
        }
    }

    #[test]
    fn decoder_does_not_treat_start_of_next_chunk_as_terminator() {
        // RFC 3977 section 3.1.1 requires CRLF before the dot line. A dot that merely
        // starts the next read after body bytes is data, not the terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut decoder = ResponseDecoder::new(RequestKind::Body);
        let mut buffer = b"222 1 <a@b> body follows\r\nbody".to_vec();
        assert!(matches!(
            decoder.push(&buffer).unwrap(),
            DecodeProgress::NeedMore
        ));

        buffer.extend_from_slice(b".\r\nstill body\r\n.\r\n");
        let DecodeProgress::Complete { status, consumed } = decoder.push(&buffer).unwrap() else {
            panic!("decoder should complete");
        };
        let response = response_from_bytes(RequestKind::Body, status, &buffer[..consumed]);

        assert_eq!(
            consumed,
            b"222 1 <a@b> body follows\r\nbody.\r\nstill body\r\n.\r\n".len()
        );
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(
            response.as_bytes(),
            b"222 1 <a@b> body follows\r\nbody.\r\nstill body\r\n.\r\n"
        );
    }

    #[test]
    fn decoder_rejects_bare_lf_before_later_crlf_status_line() {
        // RFC 3977 section 3.1 requires the response initial line to end with CRLF.
        // A malformed line like "210 foo\nboo \r\n" must fail at the bare LF instead of
        // resynchronizing on the later CRLF:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        assert!(matches!(
            ResponseDecoder::new(RequestKind::Article).push(b"210 foo\nboo \r\n"),
            Err(TypedClientError::InvalidStatusLine)
        ));
    }

    #[test]
    fn decoder_handles_all_three_push_schedules_for_overlapping_terminators() {
        // RFC 3977 sections 3.1 and 3.1.1 define exact response-line and multiline
        // terminators. This exhausts every three-push schedule for compact frames with
        // trailers and overlapping dot-line shapes, so state carried between pushes cannot
        // move completion earlier or later than the first RFC terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let single = b"430 no article\r\n222 later\r\n.\r\n";
        assert_decoder_completes_on_all_three_push_schedules(
            RequestKind::Article,
            single,
            430,
            b"430 no article\r\n".len(),
        );

        let empty = b"101 Capability list:\r\n.\r\n222 later\r\n.\r\n";
        assert_decoder_completes_on_all_three_push_schedules(
            RequestKind::Capabilities,
            empty,
            101,
            b"101 Capability list:\r\n.\r\n".len(),
        );

        let non_empty = b"222 1 <a@b> body follows\r\nxx\r\n.\r\n.\r\n";
        assert_decoder_completes_on_all_three_push_schedules(
            RequestKind::Body,
            non_empty,
            222,
            b"222 1 <a@b> body follows\r\nxx\r\n.\r\n".len(),
        );

        let near_miss = b"222 1 <a@b> body follows\r\nx\n.\r\nx\r\n.\r\n";
        assert_decoder_completes_on_all_three_push_schedules(
            RequestKind::Body,
            near_miss,
            222,
            b"222 1 <a@b> body follows\r\nx\n.\r\nx\r\n.\r\n".len(),
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn decoder_consumes_single_line_response_at_rfc_crlf_for_every_split(
            trailer in vec(dangerous_wire_bytes(), 0..24),
        ) {
            // RFC 3977 section 3.1 terminates single-line responses at the status-line CRLF.
            // Extra bytes may belong to a later response, so every read split must report
            // consumption at exactly that CRLF and never include trailer bytes:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let status_line = b"430 no article with that message-id\r\n";
            let mut frame = status_line.to_vec();
            frame.extend_from_slice(&trailer);

            for split in 0..=frame.len() {
                let (status, consumed) = complete_after_split(RequestKind::Article, &frame, split);
                prop_assert_eq!(status.as_u16(), 430);
                prop_assert_eq!(
                    consumed,
                    status_line.len(),
                    "split {} frame {:?}",
                    split,
                    frame,
                );
            }
        }

        #[test]
        fn decoder_consumes_empty_multiline_response_at_rfc_dot_crlf_for_every_split(
            trailer in vec(dangerous_wire_bytes(), 0..24),
        ) {
            // RFC 3977 section 3.1.1 allows an empty multiline response whose content is
            // exactly "." CRLF after the response initial line. The decoder must consume
            // that frame, across every split, and leave any trailer for the next response:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let response = b"101 Capability list:\r\n.\r\n";
            let mut frame = response.to_vec();
            frame.extend_from_slice(&trailer);

            for split in 0..=frame.len() {
                let (status, consumed) = complete_after_split(RequestKind::Capabilities, &frame, split);
                prop_assert_eq!(status.as_u16(), 101);
                prop_assert_eq!(
                    consumed,
                    response.len(),
                    "split {} frame {:?}",
                    split,
                    frame,
                );
            }
        }

        #[test]
        fn decoder_consumes_non_empty_multiline_response_at_first_rfc_terminator_for_every_split(
            mut body in vec(dangerous_wire_bytes(), 0..48),
            trailer in vec(dangerous_wire_bytes(), 0..24),
        ) {
            // RFC 3977 section 3.1.1 terminates multiline data at the first CRLF "." CRLF.
            // Generated body bytes are scrubbed of that exact sequence before appending the
            // real terminator, so any early completion is a decoder bug:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_multiline_terminators(&mut body);
            body.insert(0, b'x');
            body.push(b'x');
            let status_line = b"222 1 <a@b> body follows\r\n";
            let mut frame = status_line.to_vec();
            frame.extend_from_slice(&body);
            frame.extend_from_slice(crate::TERMINATOR);
            frame.extend_from_slice(&trailer);
            let expected_consumed = status_line.len() + body.len() + crate::TERMINATOR.len();

            for split in 0..=frame.len() {
                let (status, consumed) = complete_after_split(RequestKind::Body, &frame, split);
                prop_assert_eq!(status.as_u16(), 222);
                prop_assert_eq!(
                    consumed,
                    expected_consumed,
                    "split {} frame {:?}",
                    split,
                    frame,
                );
            }
        }

        #[test]
        fn decoder_rejects_malformed_status_lines_before_any_later_crlf(
            before in "[0-9]{3} [A-Za-z0-9 ]{0,20}",
            after in "[A-Za-z0-9 ]{0,20}",
            bad_separator in prop::sample::select(vec![b"\n".to_vec(), b"\r ".to_vec(), b"\r\r".to_vec()]),
        ) {
            // RFC 3977 section 3.1 uses CRLF as the only response-line terminator.
            // If bare LF or non-terminal CR appears first, the decoder must reject the
            // frame immediately instead of scanning forward to a later CRLF:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let mut frame = before.into_bytes();
            frame.extend_from_slice(&bad_separator);
            frame.extend_from_slice(after.as_bytes());
            frame.extend_from_slice(b"\r\n");

            prop_assert!(matches!(
                ResponseDecoder::new(RequestKind::Article).push(&frame),
                Err(TypedClientError::InvalidStatusLine),
            ));
        }

        #[test]
        fn decoder_ignores_multiline_near_misses_until_first_rfc_terminator_for_every_split(
            mut prefix in vec(dangerous_wire_bytes(), 0..24),
            mut suffix in vec(dangerous_wire_bytes(), 0..24),
            near_miss in prop::sample::select(vec![
                b"\n.\r\n".to_vec(),
                b"\r.\r\n".to_vec(),
                b"\r\n.\n".to_vec(),
                b"\r\n.\r".to_vec(),
                b".foo\r\n".to_vec(),
                b"..\r\n".to_vec(),
                b"body.\r\n".to_vec(),
            ]),
            trailer in vec(dangerous_wire_bytes(), 0..16),
        ) {
            // RFC 3977 section 3.1.1 names only CRLF "." CRLF as the multiline
            // terminator. Bare-LF, bare-CR, and dot-prefixed near misses must remain body
            // data until the first exact terminator is reached:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_multiline_terminators(&mut prefix);
            remove_rfc_multiline_terminators(&mut suffix);
            let status_line = b"222 1 <a@b> body follows\r\n";
            let mut body = prefix;
            body.extend_from_slice(&near_miss);
            body.extend_from_slice(&suffix);
            remove_rfc_multiline_terminators(&mut body);
            body.insert(0, b'x');
            body.push(b'x');

            let mut frame = status_line.to_vec();
            frame.extend_from_slice(&body);
            frame.extend_from_slice(crate::TERMINATOR);
            frame.extend_from_slice(&trailer);
            let expected_consumed = status_line.len()
                + terminator_end_oracle(&frame[status_line.len()..]).expect("terminator");

            for split in 0..=frame.len() {
                let (status, consumed) = complete_after_split(RequestKind::Body, &frame, split);
                prop_assert_eq!(status.as_u16(), 222);
                prop_assert_eq!(
                    consumed,
                    expected_consumed,
                    "split {} frame {:?}",
                    split,
                    frame,
                );
            }
        }

        #[test]
        fn streaming_decoder_consumes_large_multiline_response_at_rfc_terminator_across_chunk_schedules(
            response_case in 0usize..4,
            line_count in 0usize..4096,
            chunk_sizes in vec(1usize..=65536, 0..48),
            trailer in vec(dangerous_wire_bytes(), 0..64),
        ) {
            // RFC 3977 section 3.1.1: https://datatracker.ietf.org/doc/html/rfc3977#section-3.1.1
            // Multiline responses end at the first dot line. The drained streaming
            // decoder must find that exact terminator across arbitrary read chunks,
            // return the bytes consumed through the terminator, and leave trailers for
            // the next pipelined response without buffering the payload.
            let (kind, status_line, line, expected_status) = match response_case {
                0 => (
                    RequestKind::Article,
                    b"220 1 <article@test> article follows\r\n".as_slice(),
                    b"Header: value\r\n\r\narticle body line\r\n".as_slice(),
                    220,
                ),
                1 => (
                    RequestKind::Body,
                    b"222 1 <body@test> body follows\r\n".as_slice(),
                    b"article body line for a large body response\r\n".as_slice(),
                    222,
                ),
                2 => (
                    RequestKind::Over,
                    b"224 Overview information follows\r\n".as_slice(),
                    b"1\tSubject\tposter@example.test\tFri, 15 May 2026 00:00:00 +0000\t<message@example.test>\t<ref@example.test>\t1048576\t12000\r\n".as_slice(),
                    224,
                ),
                _ => (
                    RequestKind::Xover,
                    b"224 Overview information follows\r\n".as_slice(),
                    b"2\tSubject\tposter@example.test\tFri, 15 May 2026 00:00:00 +0000\t<message@example.test>\t<ref@example.test>\t1048576\t12000\r\n".as_slice(),
                    224,
                ),
            };

            let mut frame = status_line.to_vec();
            for _ in 0..line_count {
                frame.extend_from_slice(line);
            }
            frame.extend_from_slice(b".\r\n");
            let expected_consumed = frame.len();
            frame.extend_from_slice(&trailer);

            let mut decoder = StreamingResponseDecoder::new(kind);
            let mut offset = 0;
            let mut chunk_index = 0;
            let mut completed = false;

            while offset < frame.len() {
                let requested = chunk_sizes
                    .get(chunk_index)
                    .copied()
                    .unwrap_or(frame.len() - offset);
                chunk_index += 1;
                let end = (offset + requested).min(frame.len());
                let chunk = &frame[offset..end];

                match decoder.push(chunk)? {
                    StreamingDecodeProgress::NeedMore { consumed } => {
                        prop_assert_eq!(consumed, chunk.len());
                        offset += consumed;
                        prop_assert!(
                            offset < expected_consumed,
                            "decoder needed more after passing RFC terminator: offset {offset} expected {expected_consumed}",
                        );
                    }
                    StreamingDecodeProgress::Complete { status, consumed } => {
                        prop_assert_eq!(status.as_u16(), expected_status);
                        prop_assert!(consumed <= chunk.len());
                        prop_assert_eq!(
                            offset + consumed,
                            expected_consumed,
                            "streaming decoder consumed trailer bytes or stopped before terminator",
                        );
                        completed = true;
                        break;
                    }
                }
            }

            prop_assert!(completed, "streaming decoder did not complete");
        }
    }

    #[tokio::test]
    async fn write_request_wire_streams_takethis_without_full_buffer_copy() {
        let (client, server) = tokio::io::duplex(4096);
        let (_client_reader, mut client_writer) = tokio::io::split(client);
        let (mut server_reader, _server_writer) = tokio::io::split(server);
        let request = Request::TakeThis {
            message_id: MessageId::from_borrowed("<stream@test>").unwrap(),
            article: ArticleTransfer::from_borrowed(b".first\nsecond\r\nthird"),
        };

        write_request_wire(
            // DuplexStream split writer matches AsyncWrite contract used by OwnedWriteHalf.
            &mut client_writer,
            &request,
        )
        .await
        .unwrap();

        let expected = b"TAKETHIS <stream@test>\r\n..first\r\nsecond\r\nthird\r\n.\r\n";
        let mut actual = vec![0_u8; expected.len()];
        server_reader.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn decoder_reports_consumed_bytes_and_preserves_leftover_chunk_data() {
        let chunk =
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n220 1 <b@c> article follows\r\nh: v\r\n\r\nx\r\n.\r\n";

        let mut first = ResponseDecoder::new(RequestKind::Body);
        let DecodeProgress::Complete { status, consumed } = first.push(chunk).unwrap() else {
            panic!("first decoder should complete");
        };
        let response = response_from_bytes(RequestKind::Body, status, &chunk[..consumed]);
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(
            response.as_bytes(),
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n"
        );

        let mut second = ResponseDecoder::new(RequestKind::Article);
        let DecodeProgress::Complete {
            status: second_status,
            consumed: second_consumed,
        } = second.push(&chunk[consumed..]).unwrap()
        else {
            panic!("second decoder should complete");
        };
        let second_response = response_from_bytes(
            RequestKind::Article,
            second_status,
            &chunk[consumed..consumed + second_consumed],
        );
        assert_eq!(second_consumed, chunk.len() - consumed);
        assert_eq!(second_response.status().as_u16(), 220);
    }

    #[test]
    fn streaming_drained_decoder_does_not_allocate_for_large_multiline_responses() {
        let mut body_decoder = StreamingResponseDecoder::new(RequestKind::Body);
        let body_status = b"222 1 <large@test> body follows\r\n";
        let mut over_decoder = StreamingResponseDecoder::new(RequestKind::Over);
        let over_status = b"224 Overview information follows\r\n";
        let body_line =
            b"This is synthetic NNTP article payload for throughput and latency benchmarking\r\n";
        let over_line = b"123456\tSubject\tposter@example.test\tFri, 15 May 2026 00:00:00 +0000\t<message@example.test>\t<ref@example.test>\t1048576\t12000\r\n";
        let terminator = b".\r\n";
        let mut bytes = 0usize;

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

        assert!(matches!(
            body_decoder.push(body_status).unwrap(),
            StreamingDecodeProgress::NeedMore { .. }
        ));
        bytes += body_status.len();

        while bytes < 1024 * 1024 {
            assert!(matches!(
                body_decoder.push(body_line).unwrap(),
                StreamingDecodeProgress::NeedMore { .. }
            ));
            bytes += body_line.len();
        }

        let StreamingDecodeProgress::Complete { consumed, .. } =
            body_decoder.push(terminator).unwrap()
        else {
            panic!("streaming decoder should complete at RFC terminator");
        };
        assert_eq!(consumed, terminator.len());

        bytes = 0;
        assert!(matches!(
            over_decoder.push(over_status).unwrap(),
            StreamingDecodeProgress::NeedMore { .. }
        ));
        bytes += over_status.len();

        while bytes < 5 * 1024 * 1024 {
            assert!(matches!(
                over_decoder.push(over_line).unwrap(),
                StreamingDecodeProgress::NeedMore { .. }
            ));
            bytes += over_line.len();
        }

        let StreamingDecodeProgress::Complete { consumed, .. } =
            over_decoder.push(terminator).unwrap()
        else {
            panic!("streaming OVER decoder should complete at RFC terminator");
        };
        assert_eq!(consumed, terminator.len());

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        assert_eq!(
            crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn streaming_drained_decoder_handles_pipelined_body_frames_in_fixed_chunks() {
        let mut response = Vec::new();
        for id in 1..=4 {
            response
                .extend_from_slice(format!("222 {id} <body{id}@test> body follows\r\n").as_bytes());
            while response.len() % (64 * 1024) < (64 * 1024 - 128) {
                response.extend_from_slice(
                    b"This is synthetic NNTP article payload for throughput and latency benchmarking\r\n",
                );
            }
            response.extend_from_slice(b".\r\n");
        }

        let mut offset = 0;
        for _ in 0..4 {
            let mut decoder = StreamingResponseDecoder::new(RequestKind::Body);
            loop {
                let end = (offset + DRAINED_PENDING_READ_BYTES).min(response.len());
                match decoder.push(&response[offset..end]).unwrap() {
                    StreamingDecodeProgress::NeedMore { consumed } => {
                        offset += consumed;
                    }
                    StreamingDecodeProgress::Complete { consumed, .. } => {
                        offset += consumed;
                        break;
                    }
                }
            }
        }
        assert_eq!(offset, response.len());
    }

    #[test]
    fn compact_drained_pending_read_clears_fully_consumed_buffer() {
        let mut pending_read = [0; DRAINED_PENDING_READ_BYTES];
        pending_read[..3].copy_from_slice(b"abc");
        let mut pending_len = 3;
        let mut pending_start = pending_len;

        compact_drained_pending_read(&mut pending_read, &mut pending_start, &mut pending_len);

        assert_eq!(pending_len, 0);
        assert_eq!(pending_start, 0);
    }

    #[test]
    fn compact_drained_pending_read_moves_leftover_prefix_between_responses() {
        let mut pending_read = [0; DRAINED_PENDING_READ_BYTES];
        pending_read[..8].copy_from_slice(b"abcd1234");
        let mut pending_len = 8;
        let mut pending_start = 4;

        compact_drained_pending_read(&mut pending_read, &mut pending_start, &mut pending_len);

        assert_eq!(&pending_read[..pending_len], b"1234");
        assert_eq!(pending_start, 0);
    }

    #[tokio::test]
    async fn typed_connection_fetches_article_and_parses_zero_copy_view() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE <typed@test>\r\n").await;

            stream
                .write_all(
                    b"220 1 <typed@test> article follows\r\nSubject: Typed\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let response = connection
            .article(MessageId::from_str_or_wrap("typed@test").unwrap())
            .await
            .unwrap();
        let article = response.parse_article().unwrap();

        assert_eq!(response.kind(), RequestKind::Article);
        assert_eq!(response.status().as_u16(), 220);
        assert_eq!(article.message_id.as_str(), "<typed@test>");
        assert_eq!(article.headers.unwrap().get("Subject"), Some(&b"Typed"[..]));
        assert_eq!(article.body, Some(&b"payload\r\n"[..]));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_supports_current_and_numeric_article_selectors() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE\r\n").await;
            stream
                .write_all(
                    b"220 1 <current@test> article follows\r\nSubject: Current\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
            assert_read_request(&mut stream, b"BODY 42\r\n").await;
            stream
                .write_all(b"222 42 <forty-two@test> body follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let current = connection.article_current().await.unwrap();
        let body = connection.body_selector("42").await.unwrap();

        assert_eq!(current.kind(), RequestKind::Article);
        assert_eq!(current.status().as_u16(), 220);
        assert_eq!(
            current.parse_article().unwrap().message_id.as_str(),
            "<current@test>"
        );
        assert_eq!(body.kind(), RequestKind::Body);
        assert_eq!(body.status().as_u16(), 222);
        assert_eq!(
            body.parse_article().unwrap().message_id.as_str(),
            "<forty-two@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_head_and_stat_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"HEAD <head@test>\r\n").await;
            stream
                .write_all(b"221 1 <head@test> article retrieved\r\nSubject: Head\r\n.\r\n")
                .await
                .unwrap();
            assert_read_request(&mut stream, b"STAT <stat@test>\r\n").await;
            stream
                .write_all(b"223 1 <stat@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let head = connection
            .head(MessageId::from_str_or_wrap("head@test").unwrap())
            .await
            .unwrap();
        let stat = connection
            .stat(MessageId::from_str_or_wrap("stat@test").unwrap())
            .await
            .unwrap();

        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.status().as_u16(), 221);
        assert_eq!(
            head.parse_article()
                .unwrap()
                .headers
                .unwrap()
                .get("Subject"),
            Some(&b"Head"[..])
        );

        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.parse_article().unwrap().message_id.as_str(),
            "<stat@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_returns_single_line_article_error_frame() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE <missing@test>\r\n").await;

            stream
                .write_all(b"430 no article with that message-id\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let response = connection
            .article(MessageId::from_str_or_wrap("missing@test").unwrap())
            .await
            .unwrap();

        assert_eq!(response.kind(), RequestKind::Article);
        assert_eq!(response.status().as_u16(), 430);
        assert_eq!(
            response.as_bytes(),
            b"430 no article with that message-id\r\n"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_execute_exchange_returns_request_and_response() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"BODY <pair@test>\r\n").await;

            stream
                .write_all(b"222 1 <pair@test> body follows\r\npair body\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let exchange = connection
            .body_exchange(MessageId::from_str_or_wrap("pair@test").unwrap())
            .await
            .unwrap();

        assert_eq!(
            exchange.request(),
            &Request::Body {
                article_ref: ArticleRef::MessageId(
                    MessageId::from_str_or_wrap("pair@test").unwrap()
                )
            }
        );
        assert_eq!(exchange.response().status().as_u16(), 222);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_exchange_into_parts_returns_literal_pair() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"STAT <parts@test>\r\n").await;

            stream
                .write_all(b"223 1 <parts@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let exchange = connection
            .stat_exchange(MessageId::from_str_or_wrap("parts@test").unwrap())
            .await
            .unwrap();
        let (request, response) = exchange.into_parts();

        assert_eq!(
            request,
            Request::Stat {
                article_ref: ArticleRef::MessageId(
                    MessageId::from_str_or_wrap("parts@test").unwrap()
                )
            }
        );
        assert_eq!(response.kind(), RequestKind::Stat);
        assert_eq!(response.status().as_u16(), 223);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_list_family_and_general_raw_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"LIST\r\n").await;
            stream.write_all(crate::LIST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LIST ACTIVE\r\n").await;
            stream.write_all(crate::LIST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LIST ACTIVE.TIMES\r\n").await;
            stream
                .write_all(crate::LIST_ACTIVE_TIMES_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST ACTIVE.TIMES comp.lang.*\r\n").await;
            stream
                .write_all(crate::LIST_ACTIVE_TIMES_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST NEWSGROUPS\r\n").await;
            stream
                .write_all(crate::LIST_NEWSGROUPS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST NEWSGROUPS comp.lang.*\r\n").await;
            stream
                .write_all(crate::LIST_NEWSGROUPS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST OVERVIEW.FMT\r\n").await;
            stream
                .write_all(crate::LIST_OVERVIEW_FMT_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST HEADERS\r\n").await;
            stream
                .write_all(crate::LIST_HEADERS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST DISTRIB.PATS\r\n").await;
            stream
                .write_all(crate::LIST_DISTRIB_PATS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"HELP\r\n").await;
            stream.write_all(crate::HELP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"CAPABILITIES\r\n").await;
            stream
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();
            assert_read_request(&mut stream, b"DATE\r\n").await;
            stream.write_all(b"111 20260515120000\r\n").await.unwrap();
            assert_read_request(&mut stream, b"MODE READER\r\n").await;
            stream
                .write_all(b"201 posting not permitted\r\n")
                .await
                .unwrap();
            assert_read_request(&mut stream, b"QUIT\r\n").await;
            stream.write_all(crate::QUIT_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let list = connection.list().await.unwrap();
        let list_active = connection.list_active().await.unwrap();
        let list_active_times = connection.list_active_times().await.unwrap();
        let list_active_times_wildmat = connection
            .list_active_times_wildmat(Wildmat::from_borrowed("comp.lang.*").unwrap())
            .await
            .unwrap();
        let list_newsgroups = connection.list_newsgroups().await.unwrap();
        let list_newsgroups_wildmat = connection
            .list_newsgroups_wildmat(Wildmat::from_borrowed("comp.lang.*").unwrap())
            .await
            .unwrap();
        let list_overview_fmt = connection.list_overview_fmt().await.unwrap();
        let list_headers = connection.list_headers().await.unwrap();
        let list_distrib_pats = connection.list_distrib_pats().await.unwrap();
        let help = connection.help().await.unwrap();
        let capabilities = connection.capabilities().await.unwrap();
        let date = connection.date().await.unwrap();
        let mode_reader = connection.mode_reader().await.unwrap();
        let quit = connection.quit().await.unwrap();

        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(list.status().as_u16(), 215);
        assert_eq!(list.as_bytes(), crate::LIST_RESPONSE);
        assert_eq!(list_active.kind(), RequestKind::ListActive);
        assert_eq!(list_active.status().as_u16(), 215);
        assert_eq!(list_active.as_bytes(), crate::LIST_RESPONSE);
        assert_eq!(list_active_times.kind(), RequestKind::ListActiveTimes);
        assert_eq!(list_active_times.status().as_u16(), 215);
        assert_eq!(
            list_active_times.as_bytes(),
            crate::LIST_ACTIVE_TIMES_RESPONSE
        );
        assert_eq!(
            list_active_times_wildmat.kind(),
            RequestKind::ListActiveTimes
        );
        assert_eq!(list_active_times_wildmat.status().as_u16(), 215);
        assert_eq!(
            list_active_times_wildmat.as_bytes(),
            crate::LIST_ACTIVE_TIMES_RESPONSE
        );
        assert_eq!(list_newsgroups.kind(), RequestKind::ListNewsgroups);
        assert_eq!(list_newsgroups.status().as_u16(), 215);
        assert_eq!(list_newsgroups.as_bytes(), crate::LIST_NEWSGROUPS_RESPONSE);
        assert_eq!(list_newsgroups_wildmat.kind(), RequestKind::ListNewsgroups);
        assert_eq!(list_newsgroups_wildmat.status().as_u16(), 215);
        assert_eq!(
            list_newsgroups_wildmat.as_bytes(),
            crate::LIST_NEWSGROUPS_RESPONSE
        );
        assert_eq!(list_overview_fmt.kind(), RequestKind::ListOverviewFmt);
        assert_eq!(list_overview_fmt.status().as_u16(), 215);
        assert_eq!(
            list_overview_fmt.as_bytes(),
            crate::LIST_OVERVIEW_FMT_RESPONSE
        );
        assert_eq!(list_headers.kind(), RequestKind::ListHeaders);
        assert_eq!(list_headers.status().as_u16(), 215);
        assert_eq!(list_headers.as_bytes(), crate::LIST_HEADERS_RESPONSE);
        assert_eq!(list_distrib_pats.kind(), RequestKind::ListDistribPats);
        assert_eq!(list_distrib_pats.status().as_u16(), 215);
        assert_eq!(
            list_distrib_pats.as_bytes(),
            crate::LIST_DISTRIB_PATS_RESPONSE
        );

        assert_eq!(help.kind(), RequestKind::Help);
        assert_eq!(help.status().as_u16(), 100);
        assert_eq!(help.as_bytes(), crate::HELP_RESPONSE);

        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(capabilities.status().as_u16(), 101);
        assert_eq!(
            capabilities.as_bytes(),
            b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n"
        );

        assert_eq!(date.kind(), RequestKind::Date);
        assert_eq!(date.status().as_u16(), 111);
        assert_eq!(date.as_bytes(), b"111 20260515120000\r\n");

        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert_eq!(mode_reader.status().as_u16(), 201);
        assert_eq!(mode_reader.as_bytes(), b"201 posting not permitted\r\n");

        assert_eq!(quit.kind(), RequestKind::Quit);
        assert_eq!(quit.status().as_u16(), 205);
        assert_eq!(quit.as_bytes(), crate::QUIT_RESPONSE);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_group_listgroup_last_and_next_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"GROUP alt.test\r\n").await;
            stream.write_all(crate::GROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LISTGROUP\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LISTGROUP 1-\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LISTGROUP alt.test 1-10\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LAST\r\n").await;
            stream.write_all(crate::LAST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"NEXT\r\n").await;
            stream.write_all(crate::NEXT_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let group = connection
            .group(GroupName::from_owned("alt.test").unwrap())
            .await
            .unwrap();
        let listgroup_current = connection.listgroup_current().await.unwrap();
        let listgroup_range = connection
            .listgroup_range(ListGroupRange::from_owned("1-").unwrap())
            .await
            .unwrap();
        let listgroup = connection
            .listgroup_group_range(
                GroupName::from_owned("alt.test").unwrap(),
                ListGroupRange::from_owned("1-10").unwrap(),
            )
            .await
            .unwrap();
        let last = connection.last().await.unwrap();
        let next = connection.next().await.unwrap();

        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.status().as_u16(), 211);
        assert_eq!(group.as_bytes(), crate::GROUP_RESPONSE);
        assert_eq!(listgroup_current.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup_current.status().as_u16(), 211);
        assert_eq!(listgroup_current.as_bytes(), crate::LISTGROUP_RESPONSE);
        assert_eq!(listgroup_range.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup_range.status().as_u16(), 211);
        assert_eq!(listgroup_range.as_bytes(), crate::LISTGROUP_RESPONSE);
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup.status().as_u16(), 211);
        assert_eq!(listgroup.as_bytes(), crate::LISTGROUP_RESPONSE);
        assert_eq!(last.kind(), RequestKind::Last);
        assert_eq!(last.status().as_u16(), 223);
        assert_eq!(last.as_bytes(), crate::LAST_RESPONSE);
        assert_eq!(next.kind(), RequestKind::Next);
        assert_eq!(next.status().as_u16(), 223);
        assert_eq!(next.as_bytes(), crate::NEXT_RESPONSE);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_newgroups_and_newnews_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"NEWGROUPS 20260101 000000 GMT\r\n").await;
            stream.write_all(crate::NEWGROUPS_RESPONSE).await.unwrap();
            assert_read_request(
                &mut stream,
                b"NEWNEWS comp.lang.*,alt.test 20260101 000000\r\n",
            )
            .await;
            stream.write_all(crate::NEWNEWS_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let newgroups = connection
            .newgroups(
                NntpDate::from_owned("20260101").unwrap(),
                NntpTime::from_owned("000000").unwrap(),
                true,
            )
            .await
            .unwrap();
        let newnews = connection
            .newnews(
                Wildmat::from_owned("comp.lang.*,alt.test").unwrap(),
                NntpDate::from_owned("20260101").unwrap(),
                NntpTime::from_owned("000000").unwrap(),
                false,
            )
            .await
            .unwrap();

        assert_eq!(newgroups.kind(), RequestKind::NewGroups);
        assert_eq!(newgroups.status().as_u16(), 231);
        assert_eq!(newgroups.as_bytes(), crate::NEWGROUPS_RESPONSE);
        assert_eq!(newnews.kind(), RequestKind::NewNews);
        assert_eq!(newnews.status().as_u16(), 230);
        assert_eq!(newnews.as_bytes(), crate::NEWNEWS_RESPONSE);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_remaining_rfc_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"POST\r\n").await;
            stream.write_all(crate::POST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"IHAVE <ihave@test>\r\n").await;
            stream.write_all(crate::IHAVE_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"CHECK <check@test>\r\n").await;
            stream.write_all(crate::CHECK_RESPONSE).await.unwrap();
            assert_read_request(
                &mut stream,
                b"TAKETHIS <take@test>\r\nSubject: Take\r\n\r\n..line\r\nbody\r\n.\r\n",
            )
            .await;
            stream.write_all(crate::TAKETHIS_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"AUTHINFO USER bench user\r\n").await;
            stream.write_all(crate::AUTHINFO_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"AUTHINFO PASS bench pass\r\n").await;
            stream.write_all(crate::AUTHINFO_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"STARTTLS\r\n").await;
            stream.write_all(crate::STARTTLS_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let post = connection.post().await.unwrap();
        let ihave = connection
            .ihave(MessageId::from_str_or_wrap("ihave@test").unwrap())
            .await
            .unwrap();
        let check = connection
            .check(MessageId::from_str_or_wrap("check@test").unwrap())
            .await
            .unwrap();
        let takethis = connection
            .takethis(
                MessageId::from_str_or_wrap("take@test").unwrap(),
                ArticleTransfer::from_owned(b"Subject: Take\r\n\r\n.line\r\nbody"),
            )
            .await
            .unwrap();
        let auth_user = connection
            .authinfo_user(AuthInfoValue::from_owned("bench user").unwrap())
            .await
            .unwrap();
        let auth_pass = connection
            .authinfo_pass(AuthInfoValue::from_owned("bench pass").unwrap())
            .await
            .unwrap();
        let starttls = connection.starttls().await.unwrap();

        assert_eq!(post.kind(), RequestKind::Post);
        assert_eq!(post.status().as_u16(), 340);
        assert_eq!(ihave.kind(), RequestKind::Ihave);
        assert_eq!(ihave.status().as_u16(), 335);
        assert_eq!(check.kind(), RequestKind::Check);
        assert_eq!(check.status().as_u16(), 238);
        assert_eq!(takethis.kind(), RequestKind::TakeThis);
        assert_eq!(takethis.status().as_u16(), 239);
        assert_eq!(auth_user.kind(), RequestKind::AuthInfoUser);
        assert_eq!(auth_user.status().as_u16(), 281);
        assert_eq!(auth_pass.kind(), RequestKind::AuthInfoPass);
        assert_eq!(auth_pass.status().as_u16(), 281);
        assert_eq!(starttls.kind(), RequestKind::StartTls);
        assert_eq!(starttls.status().as_u16(), 382);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_hdr_and_xhdr_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"HDR Subject 1-10\r\n").await;
            stream.write_all(crate::HDR_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"HDR Message-ID <headers@test>\r\n").await;
            stream.write_all(crate::HDR_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XHDR Subject 1-10\r\n").await;
            stream.write_all(crate::XHDR_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XHDR Message-ID <headers@test>\r\n").await;
            stream.write_all(crate::XHDR_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let hdr_range = connection
            .hdr(
                HeaderName::from_owned("Subject").unwrap(),
                ArticleSelector::from_owned("1-10").unwrap(),
            )
            .await
            .unwrap();
        let hdr_message_id = connection
            .hdr(
                HeaderName::from_owned("Message-ID").unwrap(),
                ArticleSelector::from_owned("<headers@test>").unwrap(),
            )
            .await
            .unwrap();
        let xhdr_range = connection
            .xhdr(
                HeaderName::from_owned("Subject").unwrap(),
                ArticleSelector::from_owned("1-10").unwrap(),
            )
            .await
            .unwrap();
        let xhdr_message_id = connection
            .xhdr(
                HeaderName::from_owned("Message-ID").unwrap(),
                ArticleSelector::from_owned("<headers@test>").unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(hdr_range.kind(), RequestKind::Hdr);
        assert_eq!(hdr_range.status().as_u16(), 225);
        assert_eq!(hdr_range.as_bytes(), crate::HDR_RESPONSE);
        assert_eq!(hdr_message_id.kind(), RequestKind::Hdr);
        assert_eq!(hdr_message_id.status().as_u16(), 225);
        assert_eq!(hdr_message_id.as_bytes(), crate::HDR_RESPONSE);
        assert_eq!(xhdr_range.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr_range.status().as_u16(), 225);
        assert_eq!(xhdr_range.as_bytes(), crate::XHDR_RESPONSE);
        assert_eq!(xhdr_message_id.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr_message_id.status().as_u16(), 225);
        assert_eq!(xhdr_message_id.as_bytes(), crate::XHDR_RESPONSE);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_over_and_xover_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"OVER 1-10\r\n").await;
            stream.write_all(crate::OVER_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"OVER <overview@test>\r\n").await;
            stream.write_all(crate::OVER_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XOVER 1-10\r\n").await;
            stream.write_all(crate::XOVER_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XOVER <overview@test>\r\n").await;
            stream.write_all(crate::XOVER_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let over_range = connection
            .over(ArticleSelector::from_owned("1-10").unwrap())
            .await
            .unwrap();
        let over_message_id = connection
            .over(ArticleSelector::from_owned("<overview@test>").unwrap())
            .await
            .unwrap();
        let xover_range = connection
            .xover(ArticleSelector::from_owned("1-10").unwrap())
            .await
            .unwrap();
        let xover_message_id = connection
            .xover(ArticleSelector::from_owned("<overview@test>").unwrap())
            .await
            .unwrap();

        assert_eq!(over_range.kind(), RequestKind::Over);
        assert_eq!(over_range.status().as_u16(), 224);
        assert_eq!(over_range.as_bytes(), crate::OVER_RESPONSE);
        assert_eq!(over_message_id.kind(), RequestKind::Over);
        assert_eq!(over_message_id.status().as_u16(), 224);
        assert_eq!(over_message_id.as_bytes(), crate::OVER_RESPONSE);
        assert_eq!(xover_range.kind(), RequestKind::Xover);
        assert_eq!(xover_range.status().as_u16(), 224);
        assert_eq!(xover_range.as_bytes(), crate::XOVER_RESPONSE);
        assert_eq!(xover_message_id.kind(), RequestKind::Xover);
        assert_eq!(xover_message_id.status().as_u16(), 224);
        assert_eq!(xover_message_id.as_bytes(), crate::XOVER_RESPONSE);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_returns_typed_owned_article_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE <surface@test>\r\n").await;

            stream
                .write_all(
                    b"220 1 <surface@test> article follows\r\nSubject: Surface\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let article = client.article("surface@test").await.unwrap();
        let parsed = article.article().unwrap();

        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(article.status().as_u16(), 220);
        assert_eq!(parsed.message_id.as_str(), "<surface@test>");
        assert_eq!(
            parsed.headers.unwrap().get("Subject"),
            Some(&b"Surface"[..])
        );
        assert_eq!(parsed.body, Some(&b"payload\r\n"[..]));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_exchange_returns_request_and_typed_article() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE <exchange@test>\r\n").await;

            stream
                .write_all(
                    b"220 1 <exchange@test> article follows\r\nSubject: Exchange\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let exchange = client.article_exchange("exchange@test").await.unwrap();
        let parsed = exchange.article().article().unwrap();

        assert_eq!(
            exchange.request(),
            &Request::Article {
                article_ref: ArticleRef::MessageId(
                    MessageId::from_str_or_wrap("exchange@test").unwrap()
                )
            }
        );
        assert_eq!(exchange.article().status().as_u16(), 220);
        assert_eq!(parsed.message_id.as_str(), "<exchange@test>");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_exchange_into_parts_returns_literal_pair() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE <pair-surface@test>\r\n").await;

            stream
                .write_all(
                    b"220 1 <pair-surface@test> article follows\r\nSubject: Pair\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let exchange = client.article_exchange("pair-surface@test").await.unwrap();
        let (request, article) = exchange.into_parts();
        let raw = article.clone().into_response();

        assert_eq!(
            request,
            Request::Article {
                article_ref: ArticleRef::MessageId(
                    MessageId::from_str_or_wrap("pair-surface@test").unwrap()
                )
            }
        );
        assert_eq!(article.status().as_u16(), 220);
        assert_eq!(raw.kind(), RequestKind::Article);
        assert_eq!(
            article.article().unwrap().message_id.as_str(),
            "<pair-surface@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_methods_support_current_and_numeric_selectors() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE\r\n").await;
            stream
                .write_all(
                    b"220 1 <surface-current@test> article follows\r\nSubject: Current\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
            assert_read_request(&mut stream, b"STAT 42\r\n").await;
            stream
                .write_all(b"223 42 <surface-42@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let current = client.article_current().await.unwrap();
        let stat = client.stat_selector("42").await.unwrap();

        assert_eq!(current.kind(), RequestKind::Article);
        assert_eq!(current.status().as_u16(), 220);
        assert_eq!(
            current.article().unwrap().message_id.as_str(),
            "<surface-current@test>"
        );
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.article().unwrap().message_id.as_str(),
            "<surface-42@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_raw_methods_expose_list_family_and_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"LIST\r\n").await;
            stream.write_all(crate::LIST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LIST ACTIVE\r\n").await;
            stream.write_all(crate::LIST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LIST ACTIVE.TIMES\r\n").await;
            stream
                .write_all(crate::LIST_ACTIVE_TIMES_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST ACTIVE.TIMES comp.lang.*\r\n").await;
            stream
                .write_all(crate::LIST_ACTIVE_TIMES_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST NEWSGROUPS\r\n").await;
            stream
                .write_all(crate::LIST_NEWSGROUPS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST NEWSGROUPS comp.lang.*\r\n").await;
            stream
                .write_all(crate::LIST_NEWSGROUPS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST OVERVIEW.FMT\r\n").await;
            stream
                .write_all(crate::LIST_OVERVIEW_FMT_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST HEADERS\r\n").await;
            stream
                .write_all(crate::LIST_HEADERS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"LIST DISTRIB.PATS\r\n").await;
            stream
                .write_all(crate::LIST_DISTRIB_PATS_RESPONSE)
                .await
                .unwrap();
            assert_read_request(&mut stream, b"CAPABILITIES\r\n").await;
            stream
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();
            assert_read_request(&mut stream, b"DATE\r\n").await;
            stream.write_all(b"111 20260515120000\r\n").await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let list = client.list().await.unwrap();
        let list_active = client.list_active().await.unwrap();
        let list_active_times = client.list_active_times().await.unwrap();
        let list_active_times_wildmat = client
            .list_active_times_wildmat("comp.lang.*")
            .await
            .unwrap();
        let list_newsgroups = client.list_newsgroups().await.unwrap();
        let list_newsgroups_wildmat = client.list_newsgroups_wildmat("comp.lang.*").await.unwrap();
        let list_overview_fmt = client.list_overview_fmt().await.unwrap();
        let list_headers = client.list_headers().await.unwrap();
        let list_distrib_pats = client.list_distrib_pats().await.unwrap();
        let capabilities = client.capabilities().await.unwrap();
        let exchange = client.date_exchange().await.unwrap();

        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(list.status().as_u16(), 215);
        assert_eq!(list_active.kind(), RequestKind::ListActive);
        assert_eq!(list_active.status().as_u16(), 215);
        assert_eq!(list_active_times.kind(), RequestKind::ListActiveTimes);
        assert_eq!(list_active_times.status().as_u16(), 215);
        assert_eq!(
            list_active_times_wildmat.kind(),
            RequestKind::ListActiveTimes
        );
        assert_eq!(list_active_times_wildmat.status().as_u16(), 215);
        assert_eq!(list_newsgroups.kind(), RequestKind::ListNewsgroups);
        assert_eq!(list_newsgroups.status().as_u16(), 215);
        assert_eq!(list_newsgroups_wildmat.kind(), RequestKind::ListNewsgroups);
        assert_eq!(list_newsgroups_wildmat.status().as_u16(), 215);
        assert_eq!(list_overview_fmt.kind(), RequestKind::ListOverviewFmt);
        assert_eq!(list_overview_fmt.status().as_u16(), 215);
        assert_eq!(list_headers.kind(), RequestKind::ListHeaders);
        assert_eq!(list_headers.status().as_u16(), 215);
        assert_eq!(list_distrib_pats.kind(), RequestKind::ListDistribPats);
        assert_eq!(list_distrib_pats.status().as_u16(), 215);
        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(capabilities.status().as_u16(), 101);
        assert_eq!(exchange.request(), &Request::Date);
        assert_eq!(exchange.response().status().as_u16(), 111);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_group_navigation_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"GROUP alt.test\r\n").await;
            stream.write_all(crate::GROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LISTGROUP\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LISTGROUP 1-\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LISTGROUP alt.test 1-10\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LAST\r\n").await;
            stream.write_all(crate::LAST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"NEXT\r\n").await;
            stream.write_all(crate::NEXT_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let group = client.group("alt.test").await.unwrap();
        let listgroup_current = client.listgroup_current().await.unwrap();
        let listgroup_range = client.listgroup_range("1-").await.unwrap();
        let listgroup = client
            .listgroup_group_range("alt.test", "1-10")
            .await
            .unwrap();
        let last = client.last().await.unwrap();
        let next = client.next_exchange().await.unwrap();

        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.status().as_u16(), 211);
        assert_eq!(listgroup_current.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup_current.status().as_u16(), 211);
        assert_eq!(listgroup_range.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup_range.status().as_u16(), 211);
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup.status().as_u16(), 211);
        assert_eq!(last.kind(), RequestKind::Last);
        assert_eq!(last.status().as_u16(), 223);
        assert_eq!(next.request(), &Request::Next);
        assert_eq!(next.response().kind(), RequestKind::Next);
        assert_eq!(next.response().status().as_u16(), 223);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_discovery_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"NEWGROUPS 20260101 000000 GMT\r\n").await;
            stream.write_all(crate::NEWGROUPS_RESPONSE).await.unwrap();
            assert_read_request(
                &mut stream,
                b"NEWNEWS comp.lang.*,alt.test 20260101 000000\r\n",
            )
            .await;
            stream.write_all(crate::NEWNEWS_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let newgroups = client.newgroups("20260101", "000000", true).await.unwrap();
        let newnews = client
            .newnews_exchange("comp.lang.*,alt.test", "20260101", "000000", false)
            .await
            .unwrap();

        assert_eq!(newgroups.kind(), RequestKind::NewGroups);
        assert_eq!(newgroups.status().as_u16(), 231);
        assert_eq!(
            newnews
                .request()
                .discovery_datetime()
                .map(|(date, time, gmt)| (date.as_str(), time.as_str(), gmt)),
            Some(("20260101", "000000", false))
        );
        assert_eq!(
            newnews.request().wildmat().map(Wildmat::as_str),
            Some("comp.lang.*,alt.test")
        );
        assert_eq!(newnews.response().kind(), RequestKind::NewNews);
        assert_eq!(newnews.response().status().as_u16(), 230);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_remaining_rfc_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"POST\r\n").await;
            stream.write_all(crate::POST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"IHAVE <ihave-surface@test>\r\n").await;
            stream.write_all(crate::IHAVE_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"CHECK <check-surface@test>\r\n").await;
            stream.write_all(crate::CHECK_RESPONSE).await.unwrap();
            assert_read_request(
                &mut stream,
                b"TAKETHIS <take-surface@test>\r\nSubject: Surface\r\n\r\n..line\r\nbody\r\n.\r\n",
            )
            .await;
            stream.write_all(crate::TAKETHIS_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"AUTHINFO USER bench user\r\n").await;
            stream.write_all(crate::AUTHINFO_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"AUTHINFO PASS bench pass\r\n").await;
            stream.write_all(crate::AUTHINFO_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"STARTTLS\r\n").await;
            stream.write_all(crate::STARTTLS_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let post = client.post().await.unwrap();
        let ihave = client.ihave("ihave-surface@test").await.unwrap();
        let check = client.check("check-surface@test").await.unwrap();
        let takethis = client
            .takethis(
                "take-surface@test",
                b"Subject: Surface\r\n\r\n.line\r\nbody",
            )
            .await
            .unwrap();
        let auth_user = client.authinfo_user("bench user").await.unwrap();
        let auth_pass = client.authinfo_pass("bench pass").await.unwrap();
        let starttls = client.starttls_exchange().await.unwrap();

        assert_eq!(post.kind(), RequestKind::Post);
        assert_eq!(post.status().as_u16(), 340);
        assert_eq!(ihave.kind(), RequestKind::Ihave);
        assert_eq!(ihave.status().as_u16(), 335);
        assert_eq!(check.kind(), RequestKind::Check);
        assert_eq!(check.status().as_u16(), 238);
        assert_eq!(takethis.kind(), RequestKind::TakeThis);
        assert_eq!(takethis.status().as_u16(), 239);
        assert_eq!(auth_user.kind(), RequestKind::AuthInfoUser);
        assert_eq!(auth_user.status().as_u16(), 281);
        assert_eq!(auth_pass.kind(), RequestKind::AuthInfoPass);
        assert_eq!(auth_pass.status().as_u16(), 281);
        assert_eq!(starttls.request(), &Request::StartTls);
        assert_eq!(starttls.response().kind(), RequestKind::StartTls);
        assert_eq!(starttls.response().status().as_u16(), 382);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_header_query_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"HDR Subject 1-10\r\n").await;
            stream.write_all(crate::HDR_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"HDR Message-ID <headers@test>\r\n").await;
            stream.write_all(crate::HDR_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XHDR Subject 1-10\r\n").await;
            stream.write_all(crate::XHDR_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XHDR Message-ID <headers@test>\r\n").await;
            stream.write_all(crate::XHDR_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let hdr_range = client.hdr("Subject", "1-10").await.unwrap();
        let hdr_message_id = client.hdr("Message-ID", "<headers@test>").await.unwrap();
        let xhdr_range = client.xhdr("Subject", "1-10").await.unwrap();
        let xhdr_message_id = client
            .xhdr_exchange("Message-ID", "<headers@test>")
            .await
            .unwrap();

        assert_eq!(hdr_range.kind(), RequestKind::Hdr);
        assert_eq!(hdr_range.status().as_u16(), 225);
        assert_eq!(hdr_message_id.kind(), RequestKind::Hdr);
        assert_eq!(hdr_message_id.status().as_u16(), 225);
        assert_eq!(xhdr_range.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr_range.status().as_u16(), 225);
        assert_eq!(
            xhdr_message_id
                .request()
                .header_query()
                .map(|(header, selector)| (header.as_str(), selector.as_str())),
            Some(("Message-ID", "<headers@test>"))
        );
        assert_eq!(xhdr_message_id.response().kind(), RequestKind::Xhdr);
        assert_eq!(xhdr_message_id.response().status().as_u16(), 225);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_overview_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"OVER 1-10\r\n").await;
            stream.write_all(crate::OVER_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"OVER <overview@test>\r\n").await;
            stream.write_all(crate::OVER_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XOVER 1-10\r\n").await;
            stream.write_all(crate::XOVER_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"XOVER <overview@test>\r\n").await;
            stream.write_all(crate::XOVER_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let over_range = client.over("1-10").await.unwrap();
        let over_message_id = client.over("<overview@test>").await.unwrap();
        let xover_range = client.xover("1-10").await.unwrap();
        let xover_message_id = client.xover_exchange("<overview@test>").await.unwrap();

        assert_eq!(over_range.kind(), RequestKind::Over);
        assert_eq!(over_range.status().as_u16(), 224);
        assert_eq!(over_message_id.kind(), RequestKind::Over);
        assert_eq!(over_message_id.status().as_u16(), 224);
        assert_eq!(xover_range.kind(), RequestKind::Xover);
        assert_eq!(xover_range.status().as_u16(), 224);
        assert_eq!(
            xover_message_id
                .request()
                .overview_selector()
                .map(ArticleSelector::as_str),
            Some("<overview@test>")
        );
        assert_eq!(xover_message_id.response().kind(), RequestKind::Xover);
        assert_eq!(xover_message_id.response().status().as_u16(), 224);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_head_and_stat_return_typed_article_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"HEAD <head-surface@test>\r\n").await;
            stream
                .write_all(
                    b"221 1 <head-surface@test> article retrieved\r\nSubject: Surface Head\r\n.\r\n",
                )
                .await
                .unwrap();
            assert_read_request(&mut stream, b"STAT <stat-surface@test>\r\n").await;
            stream
                .write_all(b"223 1 <stat-surface@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let head = client.head("head-surface@test").await.unwrap();
        let stat = client.stat("stat-surface@test").await.unwrap();

        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.status().as_u16(), 221);
        assert_eq!(
            head.article().unwrap().headers.unwrap().get("Subject"),
            Some(&b"Surface Head"[..])
        );

        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.article().unwrap().message_id.as_str(),
            "<stat-surface@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_execute_exchange_accepts_typed_request_directly() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"BODY <direct@test>\r\n").await;

            stream
                .write_all(b"222 1 <direct@test> body follows\r\ndirect body\r\n.\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let request = Request::Body {
            article_ref: ArticleRef::MessageId(MessageId::from_str_or_wrap("direct@test").unwrap()),
        };
        let exchange = client.execute_exchange(request.clone()).await.unwrap();

        assert_eq!(exchange.request(), &request);
        assert_eq!(exchange.article().status().as_u16(), 222);
        assert_eq!(
            exchange.article().article().unwrap().body,
            Some(&b"direct body\r\n"[..])
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_reports_unexpected_single_line_error_as_typed_error() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"ARTICLE <missing-surface@test>\r\n").await;

            stream
                .write_all(b"430 no article with that message-id\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let err = client.article("missing-surface@test").await.unwrap_err();
        assert!(matches!(
            err,
            TypedClientError::UnexpectedArticleResponse { .. }
        ));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_supports_concurrent_request_futures_on_one_connection() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(
                &pending[..],
                b"ARTICLE <first@test>\r\nBODY <second@test>\r\n"
            );

            stream
                .write_all(
                    b"220 1 <first@test> article follows\r\nSubject: First\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
            stream
                .write_all(b"222 1 <second@test> body follows\r\ntwo\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let (first, second) = tokio::join!(
            connection.article(MessageId::from_str_or_wrap("first@test").unwrap()),
            connection.body(MessageId::from_str_or_wrap("second@test").unwrap())
        );

        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.status().as_u16(), 220);
        assert_eq!(second.status().as_u16(), 222);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn cloned_typed_connection_shares_one_engine() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut pending = Vec::new();
            let mut scratch = [0_u8; 128];
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }

            assert_eq!(
                &pending[..],
                b"ARTICLE <clone-a@test>\r\nBODY <clone-b@test>\r\n"
            );

            stream
                .write_all(
                    b"220 1 <clone-a@test> article follows\r\nSubject: Clone\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
            stream
                .write_all(b"222 1 <clone-b@test> body follows\r\ntwo\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let clone = connection.clone();
        let (first, second) = tokio::join!(
            connection.article(MessageId::from_str_or_wrap("clone-a@test").unwrap()),
            clone.body(MessageId::from_str_or_wrap("clone-b@test").unwrap())
        );

        assert_eq!(first.unwrap().status().as_u16(), 220);
        assert_eq!(second.unwrap().status().as_u16(), 222);

        server.await.unwrap();
    }
}
