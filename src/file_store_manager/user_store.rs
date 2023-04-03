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
    sha1: String,
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
    async fn push(&mut self, filename: String, size: u64, sha1: String) -> anyhow::Result<u64> {
        log::trace!("push:{filename}  size:{size}B  sha1:{sha1}");

        let path = self.root.join(&filename);
        log::trace!("save path:{}", path.to_string_lossy());
        ensure!(!path.exists(), "file already exist:{}", filename);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let fd = File::create(&path).await?;
        let c_path = path.canonicalize();
        tokio::fs::remove_file(&path).await?;
        drop(fd);
        let path = c_path?;

        if !path.starts_with(&self.root) {
            log::error!("file path error:{}->{:?}", filename, path);
            bail!("file path error:{}", filename)
        }

        let key = FILE_STORE_MANAGER
            .get()
            .unwrap()
            .create_write_key(&path)
            .await?;

        let fd = File::create(&path).await?;
        fd.set_len(size).await?;
        log::debug!("make push:{} key:{}", path.to_string_lossy(), key);
        self.writes.insert(key, FileWriteHandle { path, sha1, fd });
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
        log::debug!("key:{key} finish");

        let path = {
            let handle = self
                .writes
                .remove(&key)
                .with_context(|| format!("not found key:{key}"))?;

            FILE_STORE_MANAGER
                .get()
                .unwrap()
                .finish_write_key(key)
                .await;

            //handle.fd.seek(SeekFrom::Start(0)).await?;

            let c_sha1 = {
                use sha1::Digest;
                let mut sha = sha1::Sha1::new();
                let mut data = vec![0; 1024 * 1024];
                let mut file = File::open(&handle.path).await?;
                while let Ok(len) = file.read(&mut data).await {
                    if len > 0 {
                        sha.update(&data[..len]);
                    } else {
                        break;
                    }
                }
                hex::encode(sha.finalize())
            };

            let t_sha1 = handle.sha1.clone();
            let path = handle.path.clone();
            drop(handle);
            log::debug!("eq c_sha1:{c_sha1} t_sha1:{t_sha1}");
            if c_sha1 != t_sha1 {
                tokio::fs::remove_file(path).await?;
                bail!("sha1 error:{c_sha1} != {t_sha1}");
            }
            path
        };

        log::debug!("file finish write:{}", path.to_string_lossy());

        Ok(())
    }

    /// clear Incomplete files
    #[inline]
    async fn clear(&mut self) -> anyhow::Result<()> {
        let paths = self
            .writes
            .iter()
            .map(|(key, handler)| (*key, handler.path.clone()))
            .collect::<Vec<_>>();
        self.writes.clear();
        for (key, path) in paths {
            log::trace!("clear file:{key} {}", path.to_string_lossy());
            FILE_STORE_MANAGER
                .get()
                .unwrap()
                .finish_write_key(key)
                .await;
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait IUserStore {
    /// create push file
    async fn push(&self, filename: String, size: u64, sha1: String) -> anyhow::Result<u64>;
    /// write data to file
    async fn write(&self, key: u64, data: &[u8]) -> anyhow::Result<()>;
    /// write file buff
    async fn write_offset(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()>;
    /// finish write file
    async fn finish(&self, key: u64) -> anyhow::Result<()>;
    /// clear Incomplete files
    async fn clear(&self) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl IUserStore for Actor<UserStore> {
    #[inline]
    async fn push(&self, filename: String, size: u64, sha1: String) -> anyhow::Result<u64> {
        self.inner_call(|inner| async move { inner.get_mut().push(filename, size, sha1).await })
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
    #[inline]
    async fn clear(&self) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().clear().await })
            .await
    }
}
