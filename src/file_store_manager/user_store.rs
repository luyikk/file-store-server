use crate::file_store_manager::IFileStoreManager;
use anyhow::{ensure, Context};
use aqueue::Actor;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWriteExt};

/// file store manager
pub struct UserStore {
    root: PathBuf,
    writes: HashMap<u64, FileWriteHandle>,
}

/// file write handle
pub struct FileWriteHandle {
    path: PathBuf,
    size: u64,
    sha256: String,
    fd: File,
}

impl UserStore {
    pub fn new(root: PathBuf) -> Actor<UserStore> {
        Actor::new(UserStore {
            root,
            writes: Default::default(),
        })
    }

    /// create push file
    #[inline]
    async fn push(&mut self, filename: String, size: u64, sha256: String) -> anyhow::Result<u64> {
        let path = self.root.join(&filename).canonicalize()?;
        ensure!(path.is_file(), "file name error:{}", filename);
        ensure!(
            path.as_path().starts_with(&self.root),
            "file path error:{}->{:?}",
            filename,
            path
        );
        ensure!(!path.exists(), "file already exist:{}", filename);
        let key = crate::FILE_STORE_MANAGER
            .get()
            .unwrap()
            .create_write_key(&path)
            .await?;
        let fd = File::create(path.with_extension("temp")).await?;
        self.writes.insert(
            key,
            FileWriteHandle {
                path,
                size,
                sha256,
                fd,
            },
        );
        Ok(key)
    }

    #[inline]
    async fn write(&mut self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        let handle = self
            .writes
            .get_mut(&key)
            .with_context(|| format!("not found write key:{}", key))?;
        handle.fd.seek(SeekFrom::Start(offset)).await?;
        handle.fd.write_all(data).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait IUserStore {
    /// create push file
    async fn push(&self, filename: String, size: u64, sha256: String) -> anyhow::Result<u64>;

    /// write file buff
    async fn write(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl IUserStore for Actor<UserStore> {
    #[inline]
    async fn push(&self, filename: String, size: u64, sha256: String) -> anyhow::Result<u64> {
        self.inner_call(|inner| async move { inner.get_mut().push(filename, size, sha256).await })
            .await
    }

    #[inline]
    async fn write(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().write(key, offset, data).await })
            .await
    }
}
