mod client_controller;

use crate::file_store_manager::{IFileStoreManager, IUserStore, UserStore, FILE_STORE_MANAGER};
use client_controller::*;
use netxserver::prelude::{tcpserver::IPeer, *};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    /// 0=file 1=directory
    pub file_type: u8,
    pub name: String,
    pub size: u64,
    pub create_time: SystemTime,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub create_time: SystemTime,
    pub b3: Option<String>,
    pub sha256: Option<String>,
    pub can_modify: bool,
}

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
    async fn push(
        &self,
        filename: String,
        size: u64,
        hash: String,
        overwrite: bool,
    ) -> anyhow::Result<u64>;
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
    /// lock the filenames can be push
    #[tag(1005)]
    async fn lock(
        &self,
        filenames: Vec<String>,
        overwrite: bool,
    ) -> anyhow::Result<(bool, Cow<'static, str>)>;

    /// check ready
    #[tag(1006)]
    async fn check_finish(&self, key: u64) -> anyhow::Result<bool>;

    /// show directory contents
    #[tag(1007)]
    async fn show_directory_contents(&self, path: PathBuf) -> anyhow::Result<Vec<Entry>>;

    /// get file info
    #[tag(1008)]
    async fn get_file_info(
        &self,
        path: PathBuf,
        blake3: bool,
        sha256: bool,
    ) -> anyhow::Result<FileInfo>;

    /// create pull file
    /// return pull file key
    #[tag(1009)]
    async fn create_pull(&self, path: PathBuf) -> anyhow::Result<u64>;

    /// read data
    #[tag(1010)]
    async fn read(&self, key: u64, offset: u64, block: usize) -> anyhow::Result<Vec<u8>>;

    /// start async read
    #[tag(1011)]
    async fn async_read(&self, key: u64, block: usize);

    /// finish write key
    #[tag(1012)]
    async fn finish_read_key(&self, key: u64);
}

pub struct FileStoreService {
    token: NetxToken<Self>,
    file_store: Actor<UserStore>,
}

#[build_impl]
impl IFileStoreService for FileStoreService {
    #[inline]
    async fn connect(&self) -> anyhow::Result<()> {
        if let Some(weak) = self.token.get_peer().await {
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
        if let Some(weak) = self.token.get_peer().await {
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
        FILE_STORE_MANAGER
            .get()
            .unwrap()
            .clear_lock(self.token.get_session_id())
            .await;
        Ok(())
    }

    #[inline]
    async fn push(
        &self,
        filename: String,
        size: u64,
        hash: String,
        overwrite: bool,
    ) -> anyhow::Result<u64> {
        self.file_store
            .push(filename, size, hash, overwrite, self.token.get_session_id())
            .await
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

    #[inline]
    async fn lock(
        &self,
        filenames: Vec<String>,
        overwrite: bool,
    ) -> anyhow::Result<(bool, Cow<'static, str>)> {
        self.file_store
            .lock(filenames, overwrite, self.token.get_session_id())
            .await
    }

    #[inline]
    async fn check_finish(&self, key: u64) -> anyhow::Result<bool> {
        self.file_store.check_finish(key).await
    }

    #[inline]
    async fn show_directory_contents(&self, path: PathBuf) -> anyhow::Result<Vec<Entry>> {
        self.file_store.show_directory_contents(path).await
    }

    #[inline]
    async fn get_file_info(
        &self,
        path: PathBuf,
        blake3: bool,
        sha256: bool,
    ) -> anyhow::Result<FileInfo> {
        self.file_store.get_file_info(path, blake3, sha256).await
    }

    #[inline]
    async fn create_pull(&self, path: PathBuf) -> anyhow::Result<u64> {
        self.file_store
            .create_pull(path, self.token.get_session_id())
            .await
    }

    #[inline]
    async fn read(&self, key: u64, offset: u64, block: usize) -> anyhow::Result<Vec<u8>> {
        self.file_store.read(key, offset, block).await
    }

    #[inline]
    async fn async_read(&self, key: u64, block: usize) {
        match self.file_store.get_read_fd(key).await {
            Ok(mut fd) => {
                if let Err(err) = fd.seek(SeekFrom::Start(0)).await {
                    log::error!("fd.seek error:{}", err);
                    return;
                }
                let client = impl_ref!(self.token=>IClientController);
                let mut data = vec![0; block];
                let mut offset = 0;
                while let Ok(size) = fd.read(&mut data).await {
                    if size > 0 && !self.token.is_disconnect().await {
                        client.write_file_by_key(key, offset, &data[0..size]).await;
                        offset += size as u64;
                    } else {
                        break;
                    }
                }
                log::debug!("async read key:{key} finish");
            }
            Err(err) => log::error!("get fd error:{}", err),
        }
    }

    #[inline]
    async fn finish_read_key(&self, key: u64) {
        self.file_store
            .finish_read_key(key, self.token.get_session_id())
            .await
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
