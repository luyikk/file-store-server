use crate::controller::{Entry, FileInfo};
use anyhow::{bail, ensure, Context};
use aqueue::Actor;
use path_absolutize::Absolutize;
use std::borrow::Cow;
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
    // because the key has already been calculated
    // no need to recalculate to improve query efficiency
    reads: HashMap<u64, File, NonHasherBuilder>,
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
            reads: HashMap::with_hasher(NonHasherBuilder),
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
        session_id: i64,
    ) -> anyhow::Result<u64> {
        log::info!("push:{filename}  size:{size}B  hash:{hash}");

        let path = self.root.join(&filename);

        let path = if path.exists() {
            if !overwrite {
                bail!("file already exist:{}", filename);
            } else {
                path.absolutize()?.to_path_buf()
            }
        } else {
            path.absolutize()?.to_path_buf()
        };

        log::trace!("save path:{}", path.display());

        let path = remove_prefix(path.absolutize()?.to_path_buf())?;

        if !path.starts_with(&self.root) {
            log::error!(
                "file path not include root:{} file:{}->{}",
                &self.root.display(),
                filename,
                path.display()
            );
            bail!("file path illegal:{} not include root", filename)
        }

        let key = FILE_STORE_MANAGER
            .get()
            .unwrap()
            .create_write_key(&path, session_id)
            .await?;

        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

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

    #[inline]
    fn get_hash(&self, key: u64) -> Option<String> {
        self.writes.get(&key).map(|x| x.hash.clone())
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

        log::info!("file finish write:{}", path.to_string_lossy());

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

    /// Check if the filenames can be push
    #[inline]
    async fn lock(
        &self,
        filenames: Vec<String>,
        overwrite: bool,
        session_id: i64,
    ) -> anyhow::Result<(bool, Cow<'static, str>)> {
        let mut paths = Vec::with_capacity(filenames.len());
        for filename in filenames {
            let path = self.root.join(&filename);
            log::debug!("lock filename:{filename}");
            if path.exists() {
                if !overwrite {
                    return Ok((false, Cow::Owned(format!("file:{filename} already exist"))));
                } else {
                    let path = remove_prefix(path.absolutize()?.to_path_buf())?;
                    if !path.starts_with(&self.root) {
                        bail!("directory path illegal:{} not include root", filename)
                    }
                    paths.push(path);
                }
            }
        }

        FILE_STORE_MANAGER
            .get()
            .unwrap()
            .lock_write(&paths, session_id)
            .await?;

        Ok((true, Cow::Borrowed("success")))
    }

    #[inline]
    async fn show_directory_contents(&self, dir: PathBuf) -> anyhow::Result<Vec<Entry>> {
        let path = self.root.join(&dir);

        let path = if path.exists() {
            path.absolutize()?.to_path_buf()
        } else {
            bail!("dir:{} not found", dir.display());
        };

        ensure!(path.is_dir(), "path:{} not is dir", dir.display());

        let path = remove_prefix(path.absolutize()?.to_path_buf())?;

        if !path.starts_with(&self.root) {
            log::error!(
                "file path not include root:{} file:{}->{}",
                &self.root.display(),
                dir.display(),
                path.display()
            );
            bail!("file path illegal:{} not include root", dir.display())
        }

        let mut result = vec![];

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            let create_time = metadata.created()?;
            if metadata.is_dir() {
                result.push(Entry {
                    file_type: 1,
                    name: entry.file_name().to_string_lossy().to_string(),
                    size: 0,
                    create_time,
                })
            } else {
                result.push(Entry {
                    file_type: 0,
                    name: entry.file_name().to_string_lossy().to_string(),
                    size: metadata.len(),
                    create_time,
                })
            }
        }

        Ok(result)
    }

    #[inline]
    async fn get_file_info(
        &self,
        file: PathBuf,
        blake3: bool,
        sha256: bool,
    ) -> anyhow::Result<FileInfo> {
        let path = self.root.join(&file);

        let path = if path.exists() {
            path.absolutize()?.to_path_buf()
        } else {
            bail!("file:{} not found", file.display());
        };

        ensure!(path.is_file(), "path:{} not is file", file.display());

        let path = remove_prefix(path.absolutize()?.to_path_buf())?;

        if !path.starts_with(&self.root) {
            log::error!(
                "file path not include root:{} file:{}->{}",
                &self.root.display(),
                file.display(),
                path.display()
            );
            bail!("file path illegal:{} not include root", file.display())
        }

        //Because locking the written file to calculate hash is meaningless
        let lock_w = FILE_STORE_MANAGER.get().unwrap().is_lock_write(&path).await;

        let lock_r = FILE_STORE_MANAGER.get().unwrap().is_lock_read(&path).await;

        let metadata = path.metadata()?;

        let c_hash = {
            if blake3 && !lock_w {
                let mut sha = blake3::Hasher::new();
                let mut data = vec![0; 512 * 1024];
                let mut file = tokio::fs::OpenOptions::new().read(true).open(&path).await?;
                while let Ok(len) = file.read(&mut data).await {
                    if len > 0 {
                        sha.update(&data[..len]);
                    } else {
                        break;
                    }
                }
                Some(hex::encode(sha.finalize().as_bytes()))
            } else {
                None
            }
        };

        let sha256 = {
            if sha256 && !lock_w {
                use sha2::{Digest, Sha256};
                let mut sha = Sha256::default();
                let mut data = vec![0; 512 * 1024];
                let mut file = tokio::fs::OpenOptions::new().read(true).open(path).await?;
                while let Ok(len) = file.read(&mut data).await {
                    if len > 0 {
                        sha.update(&data[..len]);
                    } else {
                        break;
                    }
                }
                Some(hex::encode(sha.finalize()))
            } else {
                None
            }
        };

        Ok(FileInfo {
            name: file.file_name().unwrap().to_string_lossy().to_string(),
            size: metadata.len(),
            create_time: metadata.created()?,
            b3: c_hash,
            sha256,
            can_modify: !(lock_w || lock_r),
        })
    }

    #[inline]
    async fn create_pull(&mut self, file: PathBuf, session_id: i64) -> anyhow::Result<u64> {
        let path = self.root.join(&file);

        let path = if path.exists() {
            path.absolutize()?.to_path_buf()
        } else {
            bail!("file:{} not found", file.display());
        };

        ensure!(path.is_file(), "path:{} not is file", file.display());

        let path = remove_prefix(path.absolutize()?.to_path_buf())?;

        let fd = tokio::fs::OpenOptions::new().read(true).open(&path).await?;

        let key = FILE_STORE_MANAGER
            .get()
            .unwrap()
            .create_read_key(&path, session_id)
            .await?;

        self.reads.entry(key).or_insert(fd);

        Ok(key)
    }

    #[inline]
    async fn finish_read_key(&mut self, key: u64, session_id: i64) {
        self.reads.remove(&key);
        FILE_STORE_MANAGER
            .get()
            .unwrap()
            .finish_read_key(key, session_id)
            .await
    }

    #[inline]
    async fn get_read_file_fd(&self, key: u64) -> anyhow::Result<File> {
        Ok(self
            .reads
            .get(&key)
            .with_context(|| format!("not found fd from:{key}"))?
            .try_clone()
            .await?)
    }
}

/// remove prefix verbatim disk path tag
#[inline]
pub fn remove_prefix(path: PathBuf) -> anyhow::Result<PathBuf> {
    if let Some(prefix) = get_path_prefix(&path) {
        if prefix.is_verbatim() {
            if let std::path::Prefix::VerbatimDisk(u) = prefix {
                log::trace!("path:{} is VerbatimDisk", path.display());
                Ok(PathBuf::from(
                    path.to_string_lossy()
                        .into_owned()
                        .strip_prefix(r#"\\?\"#)
                        .with_context(|| format!(r#"VerbatimDisk not is \\?\{}"#, u as char))?,
                ))
            } else {
                bail!("error prefix:{:?}", prefix);
            }
        } else {
            Ok(path)
        }
    } else {
        Ok(path)
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
        session_id: i64,
    ) -> anyhow::Result<u64>;
    /// write data to file
    async fn write(&self, key: u64, data: &[u8]) -> anyhow::Result<()>;
    /// write file buff
    async fn write_offset(&self, key: u64, offset: u64, data: &[u8]) -> anyhow::Result<()>;
    /// check file is finish
    async fn check_finish(&self, key: u64) -> anyhow::Result<bool>;
    /// finish write file
    async fn finish(&self, key: u64) -> anyhow::Result<()>;
    /// clear Incomplete files
    async fn clear(&self) -> anyhow::Result<()>;
    /// Check if the filenames can be push
    async fn lock(
        &self,
        filenames: Vec<String>,
        overwrite: bool,
        session_id: i64,
    ) -> anyhow::Result<(bool, Cow<'static, str>)>;
    /// show director contents
    async fn show_directory_contents(&self, path: PathBuf) -> anyhow::Result<Vec<Entry>>;
    /// get file info
    async fn get_file_info(
        &self,
        path: PathBuf,
        blake3: bool,
        sha256: bool,
    ) -> anyhow::Result<FileInfo>;

    /// create pull file key
    async fn create_pull(&self, path: PathBuf, session_id: i64) -> anyhow::Result<u64>;
    /// read file data
    async fn read(&self, key: u64, offset: u64, block: usize) -> anyhow::Result<Vec<u8>>;
    /// finish file read
    async fn finish_read_key(&self, key: u64, session_id: i64);
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
        session_id: i64,
    ) -> anyhow::Result<u64> {
        self.inner_call(|inner| async move {
            inner
                .get_mut()
                .push(filename, size, hash, overwrite, session_id)
                .await
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
    async fn check_finish(&self, key: u64) -> anyhow::Result<bool> {
        let path = self
            .inner_call(|inner| async move { inner.get().get_path(key) })
            .await
            .with_context(|| format!("not found key:{}", key))?;

        let t_hash = self
            .inner_call(|inner| async move { inner.get().get_hash(key) })
            .await
            .with_context(|| format!("not found key:{}", key))?;

        let c_hash = {
            let mut sha = blake3::Hasher::new();
            let mut data = vec![0; 512 * 1024];
            let mut file = tokio::fs::OpenOptions::new().read(true).open(path).await?;
            while let Ok(len) = file.read(&mut data).await {
                if len > 0 {
                    sha.update(&data[..len]);
                } else {
                    break;
                }
            }
            hex::encode(sha.finalize().as_bytes())
        };

        Ok(t_hash == c_hash)
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

    #[inline]
    async fn lock(
        &self,
        filenames: Vec<String>,
        overwrite: bool,
        session_id: i64,
    ) -> anyhow::Result<(bool, Cow<'static, str>)> {
        self.inner_call(
            |inner| async move { inner.get().lock(filenames, overwrite, session_id).await },
        )
        .await
    }

    #[inline]
    async fn show_directory_contents(&self, path: PathBuf) -> anyhow::Result<Vec<Entry>> {
        unsafe { self.deref_inner().show_directory_contents(path).await }
    }
    #[inline]
    async fn get_file_info(
        &self,
        path: PathBuf,
        blake3: bool,
        sha256: bool,
    ) -> anyhow::Result<FileInfo> {
        unsafe { self.deref_inner().get_file_info(path, blake3, sha256).await }
    }
    #[inline]
    async fn create_pull(&self, path: PathBuf, session_id: i64) -> anyhow::Result<u64> {
        self.inner_call(|inner| async move { inner.get_mut().create_pull(path, session_id).await })
            .await
    }

    #[inline]
    async fn read(&self, key: u64, offset: u64, block: usize) -> anyhow::Result<Vec<u8>> {
        let mut fd = self
            .inner_call(|inner| async move { inner.get().get_read_file_fd(key).await })
            .await?;
        fd.seek(SeekFrom::Start(offset)).await?;
        let mut data = vec![0; block];
        let len = fd.read(&mut data).await?;
        data.truncate(len);
        Ok(data)
    }

    #[inline]
    async fn finish_read_key(&self, key: u64, session_id: i64) {
        self.inner_call(
            |inner| async move { inner.get_mut().finish_read_key(key, session_id).await },
        )
        .await
    }
}
