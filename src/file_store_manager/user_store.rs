use crate::file_store_manager::{IFileStoreManager, FILE_STORE_MANAGER};
use anyhow::{bail, ensure, Context};
use aqueue::Actor;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// file store manager
pub struct UserStore {
    root: PathBuf,
    writes: HashMap<u64, FileWriteHandle>,
}

/// file write handle
pub struct FileWriteHandle {
    path: PathBuf,
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
        fd.set_len(size).await?;
        log::debug!(
            "create push file:{} size:{} sha256:{}",
            path.to_string_lossy(),
            size,
            sha256
        );
        self.writes
            .insert(key, FileWriteHandle { path, sha256, fd });
        Ok(key)
    }

    /// write data to file
    #[inline]
    async fn write(&mut self, key: u64, data: &[u8]) -> anyhow::Result<()> {
        let handle = self
            .writes
            .get_mut(&key)
            .with_context(|| format!("not found write key:{}", key))?;
        handle.fd.write_all(data).await?;
        Ok(())
    }

    /// write data to file and set seek start offset
    #[inline]
    async fn write_offset(&mut self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        let handle = self
            .writes
            .get_mut(&key)
            .with_context(|| format!("not found write key:{}", key))?;
        handle.fd.seek(SeekFrom::Start(offset)).await?;
        handle.fd.write_all(data).await?;
        Ok(())
    }

    /// finish write file
    #[inline]
    async fn finish(&mut self, key: u64) -> anyhow::Result<()> {
        let path = {
            let mut handle = self
                .writes
                .remove(&key)
                .with_context(|| format!("not found key:{key}"))?;

            FILE_STORE_MANAGER
                .get()
                .unwrap()
                .finish_write_key(key)
                .await;

            handle.fd.seek(SeekFrom::Start(0)).await?;

            let c_sha256 = {
                use sha2::Digest;
                let mut sha = sha2::Sha256::new();
                let mut data = vec![0; 4096];
                while let Ok(len) = handle.fd.read(&mut data).await {
                    if len > 0 {
                        sha.update(&data[..len]);
                    } else {
                        break;
                    }
                }
                hex::encode(sha.finalize())
            };

            let t_sha256 = handle.sha256.clone();
            let path = handle.path.clone();
            drop(handle);
            log::debug!("eq c_sha256:{c_sha256} t_sha256:{t_sha256}");
            if c_sha256 != t_sha256 {
                tokio::fs::remove_file(path.with_extension("temp")).await?;
                bail!("sha256 error:{c_sha256} != {t_sha256}");
            }
            path
        };

        tokio::fs::rename(path.with_extension("temp"), &path).await?;
        log::debug!("file finish write:{}", path.to_string_lossy());

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait IUserStore {
    /// create push file
    async fn push(&self, filename: String, size: u64, sha256: String) -> anyhow::Result<u64>;
    /// write data to file
    async fn write(&self, key: u64, data: &[u8]) -> anyhow::Result<()>;
    /// write file buff
    async fn write_offset(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()>;
    /// finish write file
    async fn finish(&self, key: u64) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl IUserStore for Actor<UserStore> {
    #[inline]
    async fn push(&self, filename: String, size: u64, sha256: String) -> anyhow::Result<u64> {
        self.inner_call(|inner| async move { inner.get_mut().push(filename, size, sha256).await })
            .await
    }

    #[inline]
    async fn write(&self, key: u64, data: &[u8]) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().write(key, data).await })
            .await
    }

    #[inline]
    async fn write_offset(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        self.inner_call(
            |inner| async move { inner.get_mut().write_offset(key, offset, data).await },
        )
        .await
    }

    #[inline]
    async fn finish(&self, key: u64) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().finish(key).await })
            .await
    }
}
