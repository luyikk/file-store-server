use anyhow::{bail, Context};
use aqueue::Actor;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::file_store_manager::non_hasher::NonHasherBuilder;
use crate::file_store_manager::{IFileStoreManager, FILE_STORE_MANAGER};
use crate::service::io::get_path_prefix;

/// file store manager
pub struct UserStore {
    root: PathBuf,
    // because the key has already been calculated
    // no need to recalculate to improve query efficiency
    writes: HashMap<u64, FileWriteHandle, NonHasherBuilder>,
}

/// file write handle
pub struct FileWriteHandle {
    path: PathBuf,
    hash: String,
    fd: File,
}

impl UserStore {
    pub fn new(root: PathBuf) -> Actor<UserStore> {
        Actor::new(UserStore {
            root,
            writes: HashMap::with_hasher(NonHasherBuilder),
        })
    }

    /// create push file
    #[inline]
    async fn push(
        &mut self,
        filename: String,
        size: u64,
        hash: String,
        overwrite: bool,
    ) -> anyhow::Result<u64> {
        log::trace!("push:{filename}  size:{size}B  hash:{hash}");

        if filename.contains("../") {
            bail!("file name error:{} not include ../", filename)
        }

        let path = self.root.join(&filename);
        log::trace!("save path:{}", path.display());

        if path.exists() {
            if !overwrite {
                bail!("file already exist:{}", filename);
            } else {
                tokio::fs::remove_file(&path).await?;
            }
        }

        let create_parent = if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
                Some(parent.to_owned())
            } else {
                None
            }
        } else {
            None
        };

        let fd = File::create(&path).await?;
        let c_path = path.canonicalize();
        tokio::fs::remove_file(&path).await?;
        drop(fd);

        let path = {
            match c_path {
                Ok(path) => {
                    if let Some(prefix) = get_path_prefix(&path) {
                        if prefix.is_verbatim() {
                            if let std::path::Prefix::VerbatimDisk(u) = prefix {
                                log::trace!("path:{} is VerbatimDisk", path.display());
                                PathBuf::from(
                                    path.to_string_lossy()
                                        .into_owned()
                                        .strip_prefix(r#"\\?\"#)
                                        .with_context(|| {
                                            format!(r#"VerbatimDisk not is \\?\{}"#, u as char)
                                        })?,
                                )
                            } else {
                                if let Some(create_parent) = create_parent {
                                    if let Err(err) = tokio::fs::remove_dir(create_parent).await {
                                        log::error!("remove dir fail:{err}");
                                    }
                                }
                                bail!("error prefix:{:?}", prefix);
                            }
                        } else {
                            path
                        }
                    } else {
                        path
                    }
                }
                Err(err) => {
                    if let Some(create_parent) = create_parent {
                        if let Err(err) = tokio::fs::remove_dir(create_parent).await {
                            log::error!("remove dir fail:{err}");
                        }
                    }
                    bail!("file:{} canonicalize error:{err}", path.display());
                }
            }
        };

        if !path.starts_with(&self.root) {
            log::error!(
                "file path error root:{} file:{}->{}",
                &self.root.display(),
                filename,
                path.display()
            );

            if let Some(create_parent) = create_parent {
                if let Err(err) = tokio::fs::remove_dir(create_parent).await {
                    log::error!("remove dir fail:{err}");
                }
            }

            bail!("file path error:{}", filename)
        }

        let key = FILE_STORE_MANAGER
            .get()
            .unwrap()
            .create_write_key(&path)
            .await?;

        let fd = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await?;
        fd.set_len(size).await?;
        log::debug!("make push:{} key:{}", path.to_string_lossy(), key);
        self.writes.insert(key, FileWriteHandle { path, hash, fd });
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

    #[inline]
    fn get_path(&self, key: u64) -> Option<PathBuf> {
        self.writes.get(&key).map(|x| x.path.clone())
    }

    /// finish write file
    #[inline]
    async fn finish(&mut self, key: u64) -> anyhow::Result<()> {
        log::debug!("key:{key} finish");

        let path = {
            let mut handle = self
                .writes
                .remove(&key)
                .with_context(|| format!("not found key:{key}"))?;

            handle.fd.flush().await?;

            FILE_STORE_MANAGER
                .get()
                .unwrap()
                .finish_write_key(key)
                .await;

            //handle.fd.seek(SeekFrom::Start(0)).await?;

            let c_hash = {
                let mut sha = blake3::Hasher::new();
                let mut data = vec![0; 512 * 1024];
                let mut file = File::open(&handle.path).await?;
                while let Ok(len) = file.read(&mut data).await {
                    if len > 0 {
                        sha.update(&data[..len]);
                    } else {
                        break;
                    }
                }
                hex::encode(sha.finalize().as_bytes())
            };

            let t_hash = handle.hash.clone();
            let path = handle.path.clone();
            drop(handle);
            log::debug!("eq c_hash:{c_hash} t_hash:{t_hash}");
            if c_hash != t_hash {
                tokio::fs::remove_file(path).await?;
                bail!("BLAKE3  error:{c_hash} != {t_hash}");
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
    async fn push(
        &self,
        filename: String,
        size: u64,
        hash: String,
        overwrite: bool,
    ) -> anyhow::Result<u64>;
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
    async fn push(
        &self,
        filename: String,
        size: u64,
        hash: String,
        overwrite: bool,
    ) -> anyhow::Result<u64> {
        self.inner_call(|inner| async move {
            inner.get_mut().push(filename, size, hash, overwrite).await
        })
        .await
    }

    #[inline]
    async fn write(&self, key: u64, data: &[u8]) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().write(key, data).await })
            .await
    }

    #[inline]
    async fn write_offset(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        let path = self
            .inner_call(|inner| async move { inner.get().get_path(key) })
            .await
            .with_context(|| format!("not found key:{}", key))?;

        let mut fd = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        fd.seek(SeekFrom::Start(offset)).await?;
        fd.write_all(data).await?;
        fd.flush().await?;
        Ok(())
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
