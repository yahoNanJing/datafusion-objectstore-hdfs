// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Object store that represents the HDFS File System.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use hdfs::err::HdfsErr::FileNotFound;
use hdfs::hdfs::{FileStatus, HdfsErr, HdfsFs};
use hdfs::walkdir::HdfsWalkDir;
use itertools::Itertools;
use object_store::path;
use object_store::path::Path;
use object_store::{Error, GetResult, ListResult, ObjectMeta, ObjectStore, Result};

/// scheme for HDFS File System
pub static HDFS_SCHEME: &str = "hdfs";
/// scheme for HDFS Federation File System
pub static VIEWFS_SCHEME: &str = "viewfs";

#[derive(Debug)]
/// Hadoop File System as Object Store.
pub struct HadoopFileSystem;

/// Get object stores with its corresponding schemes
pub fn get_hadoop_object_stores() -> HashMap<String, Arc<dyn ObjectStore>> {
    let mut ret: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();

    let fs = Arc::new(HadoopFileSystem);
    ret.insert(HDFS_SCHEME.to_string(), fs.clone());
    ret.insert(VIEWFS_SCHEME.to_string(), fs);

    ret
}

impl HadoopFileSystem {
    /// Return filesystem path of the given location
    fn path_to_filesystem(location: &Path) -> String {
        let mut path_parts = location.parts();
        let head_part = path_parts
            .next()
            .expect(format!("There should be more than 1 parts in {}", location).as_ref());
        format!(
            "{}//{}",
            head_part.as_ref(),
            path_parts.map(|part| part.as_ref().to_owned()).join("/")
        )
    }
}

impl Display for HadoopFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HadoopFileSystem")
    }
}

#[async_trait]
impl ObjectStore for HadoopFileSystem {
    // Current implementation is very simple due to missing configs,
    // like whether able to overwrite, whether able to create parent directories, etc
    async fn put(&self, location: &Path, bytes: Bytes) -> object_store::Result<()> {
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let hdfs = get_hdfs(&location)?;
            let file = match hdfs.create(&location) {
                Ok(f) => f,
                Err(e) => {
                    return Err(to_error(e));
                }
            };

            file.write(bytes.as_ref()).map_err(to_error)?;

            Ok(())
        })
        .await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let location = HadoopFileSystem::path_to_filesystem(location);

        let blob: Bytes = maybe_spawn_blocking(move || {
            let hdfs = get_hdfs(&location)?;

            let file = hdfs.open(&location).map_err(to_error)?;

            let file_status = file.get_file_status().map_err(to_error)?;

            let to_read = file_status.len();
            let mut buf = vec![0; to_read];
            let read = file.read(buf.as_mut_slice()).map_err(to_error)?;
            assert_eq!(
                to_read as i32, read,
                "Read path {} with expected size {} and actual size {}",
                &location, to_read, read
            );

            Ok(buf.into())
        })
        .await?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(blob) }).boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let hdfs = get_hdfs(&location)?;

            let file = hdfs.open(&location).map_err(to_error)?;

            let to_read = range.end - range.start;
            let mut buf = vec![0; to_read];
            let read = file
                .read_with_pos(range.start as i64, buf.as_mut_slice())
                .map_err(to_error)?;
            assert_eq!(
                to_read as i32, read,
                "Read path {} from {} with expected size {} and actual size {}",
                &location, range.start, to_read, read
            );

            Ok(buf.into())
        })
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let hdfs = get_hdfs(&location)?;

            let file_status = hdfs.get_file_status(&location).map_err(to_error)?;
            Ok(convert_metadata(file_status))
        })
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let location = HadoopFileSystem::path_to_filesystem(location);

        maybe_spawn_blocking(move || {
            let hdfs = get_hdfs(&location)?;

            hdfs.delete(&location, false).map_err(to_error)?;

            Ok(())
        })
        .await
    }

    /// List all of the leaf files under the prefix path.
    /// It will recursively search leaf files whose depth is larger than 1
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        let prefix = prefix.expect("Prefix for hdfs should not be None");
        let walkdir = HdfsWalkDir::new(prefix.as_ref().to_owned())
            .map_err(to_error)?
            .min_depth(1);

        let s =
            walkdir.into_iter().flat_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.is_file())
                        .map(|entry| Ok(convert_metadata(entry))),
                }
            });

        // If no tokio context, return iterator directly as no
        // need to perform chunked spawn_blocking reads
        if tokio::runtime::Handle::try_current().is_err() {
            return Ok(futures::stream::iter(s).boxed());
        }

        // Otherwise list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
        let stream = futures::stream::try_unfold((s, buffer), |(mut s, mut buffer)| async move {
            if buffer.is_empty() {
                (s, buffer) = tokio::task::spawn_blocking(move || {
                    for _ in 0..CHUNK_SIZE {
                        match s.next() {
                            Some(r) => buffer.push_back(r),
                            None => break,
                        }
                    }
                    (s, buffer)
                })
                .await?;
            }

            match buffer.pop_front() {
                Some(Err(e)) => Err(e),
                Some(Ok(meta)) => Ok(Some((meta, (s, buffer)))),
                None => Ok(None),
            }
        });

        Ok(stream.boxed())
    }

    /// List files and directories directly under the prefix path.
    /// It will not recursively search leaf files whose depth is larger than 1
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().expect("Prefix for hdfs should not be None");

        maybe_spawn_blocking(move || {
            let walkdir = HdfsWalkDir::new(prefix.as_ref().to_owned())
                .map_err(to_error)?
                .min_depth(1)
                .max_depth(1);

            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    let is_directory = entry.is_directory();
                    let entry_location = Path::from(entry.name());

                    let mut parts = match entry_location.prefix_match(&prefix) {
                        Some(parts) => parts,
                        None => continue,
                    };

                    let common_prefix = match parts.next() {
                        Some(p) => p,
                        None => continue,
                    };

                    drop(parts);

                    if is_directory {
                        common_prefixes.insert(prefix.child(common_prefix));
                    } else {
                        objects.push(convert_metadata(entry));
                    }
                }
            }

            Ok(ListResult {
                next_token: None,
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
            })
        })
        .await
    }

    /// Copy an object from one path to another.
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = HadoopFileSystem::path_to_filesystem(from);
        let to = HadoopFileSystem::path_to_filesystem(to);

        maybe_spawn_blocking(move || {
            // We need to make sure the source exist
            let from_hdfs = get_hdfs(&from)?;
            if !from_hdfs.exist(&from) {
                return Err(Error::NotFound {
                    path: from.clone(),
                    source: Box::new(HdfsErr::FileNotFound(from.clone())),
                });
            }
            // Delete destination if exists
            let to_hdfs = get_hdfs(&to)?;
            if to_hdfs.exist(&to) {
                to_hdfs.delete(&to, false).map_err(to_error)?;
            }

            copy(from_hdfs, &from, to_hdfs, &to)
        })
        .await
    }

    /// It's only allowed for the same HDFS
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from = HadoopFileSystem::path_to_filesystem(from);
        let to = HadoopFileSystem::path_to_filesystem(to);

        maybe_spawn_blocking(move || {
            let hdfs = get_hdfs(&from)?;

            hdfs.rename(&from, &to).map_err(to_error)?;

            Ok(())
        })
        .await
    }

    /// Copy an object from one path to another, only if destination is empty.
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = HadoopFileSystem::path_to_filesystem(from);
        let to = HadoopFileSystem::path_to_filesystem(to);

        maybe_spawn_blocking(move || {
            let to_hdfs = get_hdfs(&to)?;
            if to_hdfs.exist(&to) {
                return Err(Error::AlreadyExists {
                    path: from,
                    source: Box::new(HdfsErr::FileAlreadyExists(to.clone())),
                });
            }

            let from_hdfs = get_hdfs(&from)?;
            copy(from_hdfs, &from, to_hdfs, &to)
        })
        .await
    }
}

/// Copy hdfs file from one filesystem to another. They may exist in different object store.
fn copy(from_hdfs: Arc<HdfsFs>, from: &str, to_hdfs: Arc<HdfsFs>, to: &str) -> Result<()> {
    hdfs::util::HdfsUtil::copy(from_hdfs.as_ref(), from, to_hdfs.as_ref(), to).map_err(to_error)?;

    Ok(())
}

/// Get hdfs based on its path url
fn get_hdfs(location: &str) -> Result<Arc<HdfsFs>> {
    hdfs::hdfs::get_hdfs_by_full_path(location).map_err(to_error)
}

/// Convert HDFS file status to ObjectMeta
pub fn convert_metadata(file: FileStatus) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(file.name()),
        last_modified: DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(file.last_modified(), 0),
            Utc,
        ),
        size: file.len(),
    }
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<FileStatus, HdfsErr>,
) -> Result<Option<FileStatus>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err {
            FileNotFound(_) => Ok(None),
            _ => Err(to_error(HdfsErr::Generic(
                "Fail to walk hdfs directory".to_owned(),
            ))),
        },
    }
}

/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }
}

fn to_error(err: HdfsErr) -> Error {
    match err {
        HdfsErr::FileNotFound(path) => Error::NotFound {
            path: path.clone(),
            source: Box::new(HdfsErr::FileNotFound(path)),
        },
        HdfsErr::FileAlreadyExists(path) => Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(HdfsErr::FileAlreadyExists(path)),
        },
        HdfsErr::InvalidUrl(path) => Error::InvalidPath {
            source: path::Error::InvalidPath {
                path: PathBuf::from(path),
            },
        },
        HdfsErr::CannotConnectToNameNode(namenode_uri) => Error::Generic {
            store: "HadoopFileSystem",
            source: Box::new(HdfsErr::CannotConnectToNameNode(namenode_uri)),
        },
        HdfsErr::Generic(err_str) => Error::Generic {
            store: "HadoopFileSystem",
            source: Box::new(HdfsErr::Generic(err_str)),
        },
    }
}
