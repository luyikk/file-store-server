mod non_hasher;
mod user_store;

pub use user_store::*;

use crate::service::io::get_path_prefix;
use anyhow::{bail, ensure, Context};
use aqueue::Actor;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tokio::sync::OnceCell;

pub static FILE_STORE_MANAGER: OnceCell<Actor<FileStoreManager>> = OnceCell::const_new();

/// file store manager
pub struct FileStoreManager {
    root: PathBuf,
    writes: HashSet<u64>,
    locks: HashMap<u64, i64>,
}

impl FileStoreManager {
    pub fn new(root: PathBuf) -> anyhow::Result<Actor<FileStoreManager>> {
        let root_path = if root.is_absolute() && root.is_dir() {
            if !root.exists() {
                std::fs::create_dir_all(&root)?;
                log::trace!("create root dir:{:?}", root);
            }
            root
        } else {
            let mut current_exec_path = crate::service::io::get_current_exec_path()?;
            current_exec_path.push(root);

            if !current_exec_path.exists() {
                std::fs::create_dir_all(&current_exec_path)?;
                log::trace!("create root dir:{:?}", current_exec_path);
            }
            current_exec_path
        };

        let root_path = if let Some(prefix) = get_path_prefix(&root_path) {
            if prefix.is_verbatim() {
                if let std::path::Prefix::VerbatimDisk(u) = prefix {
                    log::trace!("path:{} is VerbatimDisk", root_path.display());
                    PathBuf::from(
                        root_path
                            .to_string_lossy()
                            .into_owned()
                            .strip_prefix(r#"\\?\"#)
                            .with_context(|| format!(r#"VerbatimDisk not is \\?\{}"#, u as char))?,
                    )
                } else {
                    bail!("error prefix:{:?}", prefix);
                }
            } else {
                root_path
            }
        } else {
            root_path
        };

        Ok(Actor::new(FileStoreManager {
            root: root_path,
            writes: Default::default(),
            locks: Default::default(),
        }))
    }

    /// create new store
    fn new_user_store(&self) -> Actor<UserStore> {
        UserStore::new(self.root.clone())
    }

    /// create file write key
    fn create_write_key(&mut self, filename: &Path, session_id: i64) -> anyhow::Result<u64> {
        let key = {
            let mut hasher = DefaultHasher::new();
            filename.hash(&mut hasher);
            hasher.finish()
        };
        ensure!(
            !self.writes.contains(&key),
            "file is being uploaded:{:?}",
            filename
        );

        if let Some(id) = self.locks.get(&key) {
            ensure!(*id == session_id, "file is being lock:{:?}", filename);
        }

        self.writes.insert(key);
        Ok(key)
    }

    /// finish write key
    fn finish_write_key(&mut self, key: u64) {
        self.writes.remove(&key);
        self.locks.remove(&key);
    }

    /// lock file
    fn lock(&mut self, paths: &[PathBuf], session_id: i64) -> anyhow::Result<()> {
        let mut keys = Vec::with_capacity(paths.len());
        for path in paths {
            let key = {
                let mut hasher = DefaultHasher::new();
                path.hash(&mut hasher);
                hasher.finish()
            };

            ensure!(
                !self.writes.contains(&key),
                "file is being uploaded:{:?}",
                path
            );

            if let Some(id) = self.locks.get(&key) {
                if *id != session_id {
                    bail!("file is being uploaded:{:?}", path)
                }
            }

            keys.push(key);
        }

        for key in keys {
            self.locks.insert(key, session_id);
        }

        Ok(())
    }

    /// clear user lock file
    fn clear_lock(&mut self, session_id: i64) {
        self.locks.retain(|_, v| *v != session_id)
    }
}

#[async_trait::async_trait]
pub trait IFileStoreManager {
    /// create new store
    fn new_user_store(&self) -> Actor<UserStore>;
    /// create file write key
    async fn create_write_key(&self, filename: &Path, session_id: i64) -> anyhow::Result<u64>;
    /// finish write key
    async fn finish_write_key(&self, key: u64);
    /// has file name
    async fn lock(&self, paths: &[PathBuf], session_id: i64) -> anyhow::Result<()>;
    /// clear user lock files
    async fn clear_lock(&self, session_id: i64);
}

#[async_trait::async_trait]
impl IFileStoreManager for Actor<FileStoreManager> {
    fn new_user_store(&self) -> Actor<UserStore> {
        unsafe { self.deref_inner().new_user_store() }
    }

    async fn create_write_key(&self, filename: &Path, session_id: i64) -> anyhow::Result<u64> {
        self.inner_call(
            |inner| async move { inner.get_mut().create_write_key(filename, session_id) },
        )
        .await
    }

    async fn finish_write_key(&self, key: u64) {
        self.inner_call(|inner| async move { inner.get_mut().finish_write_key(key) })
            .await
    }

    async fn lock(&self, paths: &[PathBuf], session_id: i64) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().lock(paths, session_id) })
            .await
    }

    async fn clear_lock(&self, session_id: i64) {
        self.inner_call(|inner| async move { inner.get_mut().clear_lock(session_id) })
            .await
    }
}
