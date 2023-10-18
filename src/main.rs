mod serde_utils;

use std::collections::HashMap;
use std::convert::{From, Into};
use std::sync::Arc;

use ::datafusion::{
    arrow::{
        array::*,
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    },
    common::parsers::CompressionTypeVariant,
    prelude::{ParquetReadOptions, SessionContext},
};
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::Fields;
use datafusion::dataframe::DataFrameWriteOptions;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Summary {
    pub count: u64,
    pub quantiles: Vec<f64>,
    pub sum: f64,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Set {
    pub values: Vec<f64>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Counter {
    pub value: f64,
}

impl Counter {
    pub fn field() -> Field {
        Field::new("counter", DataType::Float64, true)
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Gauge {
    pub value: f64,
}
impl Gauge {
    pub fn field() -> Field {
        Field::new("gauge", DataType::Float64, true)
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Distribution {
    pub samples: Vec<f64>,
    pub statistic: String,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Histogram {
    pub buckets: Vec<f64>,
    pub count: i64,
    pub sum: f64,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Tags(HashMap<String, Option<String>>);

impl Tags {
    pub fn field() -> Field {
        Field::new(
            "tags",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )
    }
    pub fn builder(capacity: usize) -> MapBuilder<StringBuilder, StringBuilder> {
        MapBuilder::new(
            None,
            StringBuilder::with_capacity(capacity, capacity * 32),
            StringBuilder::with_capacity(capacity, capacity * 32),
        )
    }

    pub fn append_to_builder(
        self,
        mut builder: MapBuilder<StringBuilder, StringBuilder>,
    ) -> MapBuilder<StringBuilder, StringBuilder> {
        self.0.into_iter().for_each(|(k, v)| {
            builder.keys().append_value(k);
            builder.values().append_option(v);
        });
        builder.append(true).unwrap();
        builder
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct VectorMetric {
    pub counter: Option<Counter>,
    pub distribution: Option<Distribution>,
    pub gauge: Option<Gauge>,
    pub histogram: Option<Histogram>,
    pub interval_ms: Option<u64>,
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub set: Option<Set>,
    pub summary: Option<Summary>,
    pub tags: Tags,
    #[serde(with = "crate::serde_utils::time")]
    pub timestamp: OffsetDateTime,
}

impl VectorMetric {
    pub fn fields() -> Fields {
        vec![
            Counter::field(),
            Gauge::field(),
            Field::new("interval_ms", DataType::UInt64, true),
            Field::new("kind", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Tags::field(),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
        ]
        .into()
    }

    pub fn to_record_batch(records: Vec<VectorMetric>) -> RecordBatch {
        let capacity = records.len();
        let mut counter_value = Vec::with_capacity(capacity);
        let mut gauge_value = Vec::with_capacity(capacity);
        let mut interval_ms = Vec::with_capacity(capacity);
        let mut kind = Vec::with_capacity(capacity);
        let mut name = Vec::with_capacity(capacity);
        let mut namespace = Vec::with_capacity(capacity);
        let mut timestamp = Vec::with_capacity(capacity);

        let mut tags_builder = Tags::builder(capacity);

        for record in records {
            counter_value.push(record.counter.map(|c| c.value));
            gauge_value.push(record.gauge.map(|g| g.value));
            interval_ms.push(record.interval_ms);
            kind.push(record.kind);
            name.push(record.name);
            namespace.push(record.namespace);
            timestamp.push(record.timestamp.unix_timestamp());

            tags_builder = record.tags.append_to_builder(tags_builder);
        }

        let tags = tags_builder.finish();

        let array: Vec<Arc<dyn Array>> = vec![
            Arc::new(Float64Array::from(counter_value)),
            Arc::new(Float64Array::from(gauge_value)),
            Arc::new(UInt64Array::from(interval_ms)),
            Arc::new(StringArray::from(kind)),
            Arc::new(StringArray::from(name)),
            Arc::new(StringArray::from(namespace)),
            Arc::new(tags),
            Arc::new(TimestampSecondArray::from(timestamp)),
        ];

        let schema = VectorMetric::fields();

        RecordBatch::try_new(Arc::new(Schema::new(schema)), array).unwrap()
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let json = r#"
        [
            {
                "counter": { "value": 0 },
                "interval_ms": 10000,
                "kind": "incremental",
                "name": "node_boost.response.details",
                "namespace": "qn",
                "tags": {
                    "account": "bf9b9fcb-f648-452e-8631-a15d35e76fa0",
                    "chain": "aptos",
                    "host": "aptos-mainnet-edge-ora-iad-a00",
                    "instance": "ed6fa011-b7c3-4022-9765-c7a5e20418f0",
                    "network": "aptos-mainnet",
                    "plan": "b3_discover",
                    "status": "400",
                    "transport": "post"
                },
                "timestamp": "2023-10-16T18:42:00Z"
            },
            {
                "gauge": { "value": 29 },
                "kind": "absolute",
                "name": "node_boost.response.time.max",
                "namespace": "qn",
                "tags": {
                    "account": "946ce1a6-631e-42f6-bca0-fc91a0e9d9c2",
                    "chain": "aptos",
                    "host": "aptos-mainnet-edge-ora-iad-a00",
                    "httpmethod": "GET",
                    "instance": "8abe202f-6eb0-4f77-b1fb-7a89db02b2ac",
                    "method": "/v1/blocks/by_height/{block_height}",
                    "network": "aptos-mainnet",
                    "plan": "b3_scale",
                    "service": "nodenb",
                    "transport": "http"
                },
                "timestamp": "2023-10-16T18:42:00Z"
            },
            {
                "gauge": { "value": 6 },
                "kind": "absolute",
                "name": "node_boost.response.time.median",
                "namespace": "qn",
                "tags": {
                    "account": "946ce1a6-631e-42f6-bca0-fc91a0e9d9c2",
                    "chain": "aptos",
                    "host": "aptos-mainnet-edge-ora-iad-a00",
                    "httpmethod": "GET",
                    "instance": "8abe202f-6eb0-4f77-b1fb-7a89db02b2ac",
                    "method": "/v1/blocks/by_height/{block_height}",
                    "network": "aptos-mainnet",
                    "plan": "b3_scale",
                    "service": "nodenb",
                    "transport": "http"
                },
                "timestamp": "2023-10-16T18:42:00Z"
            },
            {
                "gauge": { "value": 27 },
                "kind": "absolute",
                "name": "node_boost.response.time.95percentile",
                "namespace": "qn",
                "tags": {
                    "account": "946ce1a6-631e-42f6-bca0-fc91a0e9d9c2",
                    "chain": "aptos",
                    "host": "aptos-mainnet-edge-ora-iad-a00",
                    "httpmethod": "GET",
                    "instance": "8abe202f-6eb0-4f77-b1fb-7a89db02b2ac",
                    "method": "/v1/blocks/by_height/{block_height}",
                    "network": "aptos-mainnet",
                    "plan": "b3_scale",
                    "service": "nodenb",
                    "transport": "http"
                },
                "timestamp": "2023-10-16T18:42:00Z"
            }
        ]
    "#;

    let records: Vec<VectorMetric> = serde_json::from_str(json).unwrap();

    let batch = VectorMetric::to_record_batch(records);

    let schema = Schema::new(VectorMetric::fields());
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "metrics",
        "data/metrics",
        ParquetReadOptions::default()
            .table_partition_cols(vec![(
                "time_bucket".to_string(),
                // DataType::Timestamp(TimeUnit::Second, None),
                DataType::Utf8,
            )])
            .schema(&schema),
    )
    .await
    .unwrap();
    ctx.register_batch("batch", batch).unwrap();

    let write_options = DataFrameWriteOptions::default()
        .with_compression(CompressionTypeVariant::ZSTD)
        .with_single_file_output(false);

    let _batches = ctx
        .sql(
            r#"
            SELECT *, DATE_TRUNC('DAY', timestamp) AS time_bucket FROM batch
            "#,
        )
        .await
        .unwrap()
        .write_table("metrics", write_options)
        .await
        .unwrap();

    ctx.register_parquet(
        "metrics2",
        "data/metrics",
        ParquetReadOptions::default()
            .table_partition_cols(vec![(
                "time_bucket".to_string(),
                DataType::Timestamp(TimeUnit::Second, None),
                // DataType::Utf8,
            )])
            .schema(&schema),
    )
    .await
    .unwrap();
    let df = ctx
        .sql("SELECT * FROM metrics2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("{}", pretty_format_batches(&df).unwrap());
    Ok(())
}

pub fn serialize<T>(payload: &T) -> std::io::Result<String>
where
    T: Serialize,
{
    Ok(serde_json::to_string(&payload).unwrap())
}
