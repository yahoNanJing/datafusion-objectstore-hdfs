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

pub mod util;

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use datafusion::assert_batches_eq;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::error::Result;
    use datafusion::physical_plan::file_format::FileScanConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use futures::StreamExt;
    use hdfs::hdfs::get_hdfs_by_full_path;

    use datafusion_objectstore_hdfs::object_store::hdfs::{convert_metadata, HadoopFileSystem};

    use crate::util::run_hdfs_test;

    #[tokio::test]
    async fn read_small_batches_from_hdfs() -> Result<()> {
        run_hdfs_test("alltypes_plain.parquet".to_string(), |filename_hdfs| {
            Box::pin(async move {
                let session_context =
                    SessionContext::with_config(SessionConfig::new().with_batch_size(2));
                let projection = None;
                let exec =
                    get_hdfs_exec(&session_context, filename_hdfs.as_str(), &projection, None)
                        .await?;
                let stream = exec.execute(0, session_context.task_ctx())?;

                let tt_batches = stream
                    .map(|batch| {
                        let batch = batch.unwrap();
                        assert_eq!(11, batch.num_columns());
                        assert_eq!(2, batch.num_rows());
                    })
                    .fold(0, |acc, _| async move { acc + 1i32 })
                    .await;

                assert_eq!(tt_batches, 4 /* 8/2 */);

                // test metadata
                assert_eq!(exec.statistics().num_rows, Some(8));
                assert_eq!(exec.statistics().total_byte_size, Some(671));

                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn parquet_query() {
        run_with_register_alltypes_parquet(|ctx| {
            Box::pin(async move {
                // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
                // so we need an explicit cast
                let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
                let actual = ctx.sql(sql).await?.collect().await?;
                let expected = vec![
                    "+----+-----------------------------------------+",
                    "| id | CAST(alltypes_plain.string_col AS Utf8) |",
                    "+----+-----------------------------------------+",
                    "| 4  | 0                                       |",
                    "| 5  | 1                                       |",
                    "| 6  | 0                                       |",
                    "| 7  | 1                                       |",
                    "| 2  | 0                                       |",
                    "| 3  | 1                                       |",
                    "| 0  | 0                                       |",
                    "| 1  | 1                                       |",
                    "+----+-----------------------------------------+",
                ];

                assert_batches_eq!(expected, &actual);

                Ok(())
            })
        })
        .await
        .unwrap()
    }

    async fn get_hdfs_exec(
        ctx: &SessionContext,
        file_name: &str,
        projection: &Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        register_hdfs_object_store(&ctx);

        let hdfs = get_hdfs_by_full_path(file_name).expect("HdfsFs not found");
        let file_status = hdfs
            .get_file_status(file_name)
            .expect("File status not found");
        let file_meta = convert_metadata(file_status);
        let file_partition = PartitionedFile {
            object_meta: file_meta.clone(),
            partition_values: vec![],
            range: None,
        };

        let format = ParquetFormat::default();
        let store = Arc::new(HadoopFileSystem) as _;
        let file_schema = format
            .infer_schema(&store, vec![file_meta.clone()].as_slice())
            .await
            .expect("Schema inference");
        let statistics = format
            .infer_stats(&store, file_schema.clone(), &file_meta)
            .await
            .expect("Stats inference");
        let file_groups = vec![vec![file_partition]];
        let exec = format
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::parse("hdfs://").unwrap(),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: vec![],
                },
                &[],
            )
            .await?;
        Ok(exec)
    }

    /// Run query after table registered with parquet file on hdfs
    pub async fn run_with_register_alltypes_parquet<F>(test_query: F) -> Result<()>
    where
        F: FnOnce(SessionContext) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>
            + Send
            + 'static,
    {
        run_hdfs_test("alltypes_plain.parquet".to_string(), |hdfs_file_uri| {
            Box::pin(async move {
                let ctx = SessionContext::new();
                register_hdfs_object_store(&ctx);
                let table_name = "alltypes_plain";
                println!(
                    "Register table {} with parquet file {}",
                    table_name, hdfs_file_uri
                );
                ctx.register_parquet(table_name, &hdfs_file_uri, ParquetReadOptions::default())
                    .await?;

                test_query(ctx).await
            })
        })
        .await
    }

    fn register_hdfs_object_store(ctx: &SessionContext) {
        ctx.runtime_env()
            .register_object_store("hdfs", "", Arc::new(HadoopFileSystem));
    }
}
