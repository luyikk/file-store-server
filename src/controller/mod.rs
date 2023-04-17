use crate::file_store_manager::{IFileStoreManager, IUserStore, UserStore, FILE_STORE_MANAGER};
use netxserver::prelude::{tcpserver::IPeer, *};
use std::sync::Arc;

#[build(FileStoreService)]
pub trait IFileStoreService {
    /// client connect
    #[tag(connect)]
    async fn connect(&self) -> anyhow::Result<()>;
    /// client disconnect
    #[tag(disconnect)]
    async fn disconnect(&self) -> anyhow::Result<()>;
    /// client peer session drop
    #[tag(closed)]
    async fn closed(&self) -> anyhow::Result<()>;
    /// push file
    ///
    /// filename:
    ///     file.xyz;
    ///     dict/file.xyz;
    ///
    /// size: file size u64
    ///
    /// hash: file BLAKE3
    ///
    /// return: file write key
    #[tag(1001)]
    async fn push(&self, filename: String, size: u64, hash: String) -> anyhow::Result<u64>;
    /// write data to file
    /// key: file push key
    /// data: file data
    #[tag(1002)]
    async fn write(&self, key: u64, data: Vec<u8>) -> anyhow::Result<()>;
    /// write data to file
    /// key: file push key
    /// offset: file offset write position
    /// data: file data
    #[tag(1003)]
    async fn write_offset(&self, key: u64, offset: u64, data: Vec<u8>);
    /// finish write
    #[tag(1004)]
    async fn push_finish(&self, key: u64) -> anyhow::Result<()>;
}

pub struct FileStoreService {
    token: NetxToken<Self>,
    file_store: Actor<UserStore>,
}

#[build_impl]
impl IFileStoreService for FileStoreService {
    #[inline]
    async fn connect(&self) -> anyhow::Result<()> {
        if let Some(weak) = self.token.get_peer().await? {
            if let Some(peer) = weak.upgrade() {
                log::info!(
                    "client addr:{} session {} connect",
                    peer.addr(),
                    self.token.get_session_id()
                )
            }
        }
        Ok(())
    }

    #[inline]
    async fn disconnect(&self) -> anyhow::Result<()> {
        if let Some(weak) = self.token.get_peer().await? {
            if let Some(peer) = weak.upgrade() {
                log::info!(
                    "client addr:{} session {} disconnect",
                    peer.addr(),
                    self.token.get_session_id()
                )
            }
        }
        Ok(())
    }

    #[inline]
    async fn closed(&self) -> anyhow::Result<()> {
        log::info!("client session {} closed", self.token.get_session_id());
        self.file_store.clear().await?;
        Ok(())
    }

    #[inline]
    async fn push(&self, filename: String, size: u64, hash: String) -> anyhow::Result<u64> {
        self.file_store.push(filename, size, hash).await
    }

    #[inline]
    async fn write(&self, key: u64, data: Vec<u8>) -> anyhow::Result<()> {
        self.file_store.write(key, &data).await
    }

    #[inline]
    async fn write_offset(&self, key: u64, offset: u64, data: Vec<u8>) {
        if let Err(err) = self.file_store.write_offset(key, offset, &data).await {
            log::error!("write_offset key:{key} offset:{offset}  error:{err}");
        }
    }

    #[inline]
    async fn push_finish(&self, key: u64) -> anyhow::Result<()> {
        self.file_store.finish(key).await
    }
}

pub struct ImplCreateController;
impl ICreateController for ImplCreateController {
    type Controller = FileStoreService;

    #[inline]
    fn create_controller(
        &self,
        token: NetxToken<Self::Controller>,
    ) -> anyhow::Result<Arc<Self::Controller>> {
        Ok(Arc::new(FileStoreService {
            token,
            file_store: FILE_STORE_MANAGER.get().unwrap().new_user_store(),
        }))
    }
}
