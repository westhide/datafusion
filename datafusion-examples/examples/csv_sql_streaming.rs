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

use datafusion::common::test_util::datafusion_test_data;
use datafusion::error::Result;
use datafusion::logical_expr::ExplainFormat;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results with streaming aggregation and streaming window
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion_test_data();

    ctx.register_csv("t", &format!("{testdata}/cars.csv"), CsvReadOptions::new())
        .await?;

    // let df = ctx.sql("select * from t").await?;
    let df = ctx.table("t").await?;

    df.clone()
        .explain_with_format(true, false, ExplainFormat::Tree)?
        .show()
        .await?;

    // df.show().await?;
    Ok(())
}
