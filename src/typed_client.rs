use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::protocol::Request;
use crate::tail_buffer::{TailBuffer, TerminatorStatus};
use crate::{
    Article, ArticleParseError, ArticleSelector, ArticleTransfer, AuthInfoValue, GroupName,
    HeaderName, MessageId, NntpDate, NntpTime, RequestKind, StatusCode, Wildmat,
};

/// Options for the typed one-connection client prototype.
#[derive(Debug, Clone, Copy)]
pub struct TypedClientOptions {
    pub read_buffer_bytes: usize,
    pub nodelay: bool,
    pub socket_recv_buffer: usize,
    pub socket_send_buffer: usize,
    pub pipeline_depth: usize,
    pub response_mode: TypedClientResponseMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedClientResponseMode {
    Owned,
    Drained,
}

impl Default for TypedClientOptions {
    fn default() -> Self {
        Self {
            read_buffer_bytes: crate::CLIENT_READER_CAPACITY,
            nodelay: true,
            socket_recv_buffer: 0,
            socket_send_buffer: 0,
            pipeline_depth: 64,
            response_mode: TypedClientResponseMode::Owned,
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

    /// Send an ARTICLE request and return the completed request/response pair.
    pub async fn article_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request =
            Request::article(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
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

    /// Send a BODY request and return the completed request/response pair.
    pub async fn body_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::body(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
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

    /// Send a HEAD request and return the completed request/response pair.
    pub async fn head_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::head(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
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

    /// Send a STAT request and return the completed request/response pair.
    pub async fn stat_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::stat(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
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

    /// Send a LISTGROUP request and return the completed raw request/response pair.
    pub async fn listgroup_exchange(
        &self,
        group: impl AsRef<str>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let request = Request::listgroup(group).map_err(|_| TypedClientError::InvalidGroupName)?;
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
                .max(crate::tail_buffer::TERMINATOR_TAIL_SIZE)
        ]
        .into_boxed_slice();
        crate::read_greeting(&mut stream, &mut read_buffer).await?;
        let (reader, writer) = stream.into_split();
        let (request_tx, request_rx) = mpsc::channel(options.pipeline_depth.max(1));
        let (inflight_tx, inflight_rx) = mpsc::channel(options.pipeline_depth.max(1));
        let poisoned = Arc::new(Mutex::new(None));
        let read_chunk_bytes = read_buffer.len();
        let response_mode = options.response_mode;

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
            response_mode,
            inflight_rx,
            poisoned.clone(),
            writer_abort,
        ));

        Ok(Self {
            inner: Arc::new(ConnectionHandle {
                request_tx,
                response_mode,
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
        self.execute(Request::Article { message_id }).await
    }

    /// Send an ARTICLE request and return the completed request/response pair.
    pub async fn article_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Article { message_id }).await
    }

    /// Send a BODY request and return the owned response frame.
    pub async fn body(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Body { message_id }).await
    }

    /// Send a BODY request and return the completed request/response pair.
    pub async fn body_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Body { message_id }).await
    }

    /// Send a HEAD request and return the owned response frame.
    pub async fn head(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Head { message_id }).await
    }

    /// Send a HEAD request and return the completed request/response pair.
    pub async fn head_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Head { message_id }).await
    }

    /// Send a STAT request and return the owned response frame.
    pub async fn stat(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Stat { message_id }).await
    }

    /// Send a STAT request and return the completed request/response pair.
    pub async fn stat_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Stat { message_id }).await
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
        self.execute(Request::ListGroup { group }).await
    }

    /// Send a LISTGROUP request and return the completed request/response pair.
    pub async fn listgroup_exchange(
        &self,
        group: GroupName<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::ListGroup { group }).await
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
        if self.inner.response_mode != TypedClientResponseMode::Owned {
            return Err(TypedClientError::Io(io::Error::other(
                "owned response requested from drained typed connection",
            )));
        }
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

    pub(crate) async fn queue_request_drained(
        &self,
        request: Request<'static>,
    ) -> Result<PendingDrainedResponse, TypedClientError> {
        if self.inner.response_mode != TypedClientResponseMode::Drained {
            return Err(TypedClientError::Io(io::Error::other(
                "drained response requested from owned typed connection",
            )));
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .request_tx
            .send(QueuedRequest {
                request,
                response_tx,
            })
            .await
            .map_err(|_| TypedClientError::ConnectionClosed)?;

        Ok(PendingDrainedResponse {
            inner: self.inner.clone(),
            response_rx,
        })
    }

    pub(crate) async fn queue_request_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<PendingExchange, TypedClientError> {
        if self.inner.response_mode != TypedClientResponseMode::Owned {
            return Err(TypedClientError::Io(io::Error::other(
                "owned response requested from drained typed connection",
            )));
        }
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
    response_mode: TypedClientResponseMode,
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

#[derive(Debug)]
struct ResponseDecoder {
    kind: RequestKind,
    buffered_len: usize,
    status: Option<(StatusCode, usize)>,
    tail: TailBuffer,
}

impl ResponseDecoder {
    fn new(kind: RequestKind) -> Self {
        Self {
            kind,
            buffered_len: 0,
            status: None,
            tail: TailBuffer::default(),
        }
    }

    fn push(&mut self, buffer: &[u8]) -> Result<DecodeProgress, TypedClientError> {
        let chunk_start = self.buffered_len;
        self.buffered_len = buffer.len();

        let (status, status_end) = match self.status {
            Some(value) => value,
            None => {
                let Some(status_end) = status_line_end(buffer) else {
                    return Ok(DecodeProgress::NeedMore);
                };
                let status =
                    StatusCode::parse(buffer).ok_or(TypedClientError::InvalidStatusLine)?;
                self.status = Some((status, status_end));
                if !self.kind.expects_multiline_response(status) {
                    return Ok(DecodeProgress::Complete {
                        status,
                        consumed: status_end,
                    });
                }
                (status, status_end)
            }
        };

        let content_chunk_start = chunk_start.max(status_end);
        if content_chunk_start >= buffer.len() {
            return Ok(DecodeProgress::NeedMore);
        }

        let content_chunk = &buffer[content_chunk_start..];
        match self.tail.detect_terminator(content_chunk) {
            TerminatorStatus::FoundAt(end) => Ok(DecodeProgress::Complete {
                status,
                consumed: content_chunk_start + end,
            }),
            TerminatorStatus::NotFound => {
                self.tail.update(content_chunk);
                Ok(DecodeProgress::NeedMore)
            }
        }
    }
}

#[derive(Debug)]
enum DecodeProgress {
    NeedMore,
    Complete { status: StatusCode, consumed: usize },
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
            CompletedResponse::Drained(_) => Err(TypedClientError::Io(io::Error::other(
                "drained response returned to owned caller",
            ))),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PendingDrainedResponse {
    inner: Arc<ConnectionHandle>,
    response_rx: oneshot::Receiver<Result<CompletedRequest, SharedEngineError>>,
}

impl PendingDrainedResponse {
    pub(crate) async fn receive(self) -> Result<DrainedResponse, TypedClientError> {
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
            CompletedResponse::Drained(response) => Ok(response),
            CompletedResponse::Owned(_) => Err(TypedClientError::Io(io::Error::other(
                "owned response returned to drained caller",
            ))),
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
            CompletedResponse::Drained(_) => Err(TypedClientError::Io(io::Error::other(
                "drained response returned to exchange caller",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainedResponse {
    bytes_len: usize,
}

impl DrainedResponse {
    pub(crate) fn bytes_len(&self) -> usize {
        self.bytes_len
    }
}

#[derive(Debug)]
enum CompletedResponse {
    Owned(OwnedResponse),
    Drained(DrainedResponse),
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
    let mut write_buffer = Vec::with_capacity(crate::MAX_CLIENT_COMMAND_BYTES);

    while let Some(queued) = request_rx.recv().await {
        let kind = queued.request.kind();
        write_buffer.clear();
        queued.request.write_wire_to(&mut write_buffer);
        if let Err(err) = writer.write_all(&write_buffer).await {
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

async fn run_reader_task(
    mut reader: OwnedReadHalf,
    read_chunk_bytes: usize,
    response_mode: TypedClientResponseMode,
    mut inflight_rx: mpsc::Receiver<InFlightRequest>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
    writer_abort: tokio::task::AbortHandle,
) {
    let initial_capacity = match response_mode {
        TypedClientResponseMode::Owned => read_chunk_bytes.saturating_mul(2),
        TypedClientResponseMode::Drained => read_chunk_bytes.saturating_mul(4),
    };
    let mut pending_read = BytesMut::with_capacity(initial_capacity);

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
                        let response = match response_mode {
                            TypedClientResponseMode::Owned => {
                                CompletedResponse::Owned(OwnedResponse {
                                    kind,
                                    status,
                                    bytes: pending_read.split_to(consumed).freeze(),
                                })
                            }
                            TypedClientResponseMode::Drained => {
                                pending_read.advance(consumed);
                                CompletedResponse::Drained(DrainedResponse {
                                    bytes_len: consumed,
                                })
                            }
                        };
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

            pending_read.reserve(read_chunk_bytes);
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
        | TypedClientError::UnexpectedArticleResponse { .. } => SharedEngineError::ConnectionClosed,
    }
}

fn status_line_end(buffer: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', buffer).map(|index| index + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn decoder_completes_multiline_response_across_chunks() {
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
    fn decoder_completes_empty_multiline_response() {
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
                message_id: MessageId::from_str_or_wrap("pair@test").unwrap()
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
                message_id: MessageId::from_str_or_wrap("parts@test").unwrap()
            }
        );
        assert_eq!(response.kind(), RequestKind::Stat);
        assert_eq!(response.status().as_u16(), 223);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_list_help_capabilities_date_mode_reader_and_quit_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"LIST\r\n").await;
            stream.write_all(crate::LIST_RESPONSE).await.unwrap();
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
        let help = connection.help().await.unwrap();
        let capabilities = connection.capabilities().await.unwrap();
        let date = connection.date().await.unwrap();
        let mode_reader = connection.mode_reader().await.unwrap();
        let quit = connection.quit().await.unwrap();

        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(list.status().as_u16(), 215);
        assert_eq!(list.as_bytes(), crate::LIST_RESPONSE);

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
            assert_read_request(&mut stream, b"LISTGROUP alt.test\r\n").await;
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
        let listgroup = connection
            .listgroup(GroupName::from_owned("alt.test").unwrap())
            .await
            .unwrap();
        let last = connection.last().await.unwrap();
        let next = connection.next().await.unwrap();

        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.status().as_u16(), 211);
        assert_eq!(group.as_bytes(), crate::GROUP_RESPONSE);
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
            assert_read_request(&mut stream, b"XHDR Message-ID <headers@test>\r\n").await;
            stream.write_all(crate::XHDR_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let hdr = connection
            .hdr(
                HeaderName::from_owned("Subject").unwrap(),
                ArticleSelector::from_owned("1-10").unwrap(),
            )
            .await
            .unwrap();
        let xhdr = connection
            .xhdr(
                HeaderName::from_owned("Message-ID").unwrap(),
                ArticleSelector::from_owned("<headers@test>").unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(hdr.kind(), RequestKind::Hdr);
        assert_eq!(hdr.status().as_u16(), 225);
        assert_eq!(hdr.as_bytes(), crate::HDR_RESPONSE);
        assert_eq!(xhdr.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr.status().as_u16(), 225);
        assert_eq!(xhdr.as_bytes(), crate::XHDR_RESPONSE);

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
            assert_read_request(&mut stream, b"XOVER <overview@test>\r\n").await;
            stream.write_all(crate::XOVER_RESPONSE).await.unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let over = connection
            .over(ArticleSelector::from_owned("1-10").unwrap())
            .await
            .unwrap();
        let xover = connection
            .xover(ArticleSelector::from_owned("<overview@test>").unwrap())
            .await
            .unwrap();

        assert_eq!(over.kind(), RequestKind::Over);
        assert_eq!(over.status().as_u16(), 224);
        assert_eq!(over.as_bytes(), crate::OVER_RESPONSE);
        assert_eq!(xover.kind(), RequestKind::Xover);
        assert_eq!(xover.status().as_u16(), 224);
        assert_eq!(xover.as_bytes(), crate::XOVER_RESPONSE);

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
                message_id: MessageId::from_str_or_wrap("exchange@test").unwrap()
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
                message_id: MessageId::from_str_or_wrap("pair-surface@test").unwrap()
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
    async fn client_raw_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();
            assert_read_request(&mut stream, b"LIST\r\n").await;
            stream.write_all(crate::LIST_RESPONSE).await.unwrap();
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
        let capabilities = client.capabilities().await.unwrap();
        let exchange = client.date_exchange().await.unwrap();

        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(list.status().as_u16(), 215);
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
            assert_read_request(&mut stream, b"LISTGROUP alt.test\r\n").await;
            stream.write_all(crate::LISTGROUP_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"LAST\r\n").await;
            stream.write_all(crate::LAST_RESPONSE).await.unwrap();
            assert_read_request(&mut stream, b"NEXT\r\n").await;
            stream.write_all(crate::NEXT_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let group = client.group("alt.test").await.unwrap();
        let listgroup = client.listgroup("alt.test").await.unwrap();
        let last = client.last().await.unwrap();
        let next = client.next_exchange().await.unwrap();

        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.status().as_u16(), 211);
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
            assert_read_request(&mut stream, b"XHDR Message-ID <headers@test>\r\n").await;
            stream.write_all(crate::XHDR_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let hdr = client.hdr("Subject", "1-10").await.unwrap();
        let xhdr = client
            .xhdr_exchange("Message-ID", "<headers@test>")
            .await
            .unwrap();

        assert_eq!(hdr.kind(), RequestKind::Hdr);
        assert_eq!(hdr.status().as_u16(), 225);
        assert_eq!(
            xhdr.request()
                .header_query()
                .map(|(header, selector)| (header.as_str(), selector.as_str())),
            Some(("Message-ID", "<headers@test>"))
        );
        assert_eq!(xhdr.response().kind(), RequestKind::Xhdr);
        assert_eq!(xhdr.response().status().as_u16(), 225);

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
            assert_read_request(&mut stream, b"XOVER <overview@test>\r\n").await;
            stream.write_all(crate::XOVER_RESPONSE).await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let over = client.over("1-10").await.unwrap();
        let xover = client.xover_exchange("<overview@test>").await.unwrap();

        assert_eq!(over.kind(), RequestKind::Over);
        assert_eq!(over.status().as_u16(), 224);
        assert_eq!(
            xover
                .request()
                .overview_selector()
                .map(ArticleSelector::as_str),
            Some("<overview@test>")
        );
        assert_eq!(xover.response().kind(), RequestKind::Xover);
        assert_eq!(xover.response().status().as_u16(), 224);

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
            message_id: MessageId::from_str_or_wrap("direct@test").unwrap(),
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
