use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Price {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub asset: String,
    pub price: f64,
}

impl Price {
    pub fn new(
        id: Uuid,
        asset: String,
        price: f64,
    ) -> Self {
        let now = Utc::now();

        Self {
            id,
            created_at: now,
            updated_at: now,
            asset,
            price,
        }
    }

    pub fn create_table_sql() -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS test_prices (
            id UUID,
            created_at DateTime64(3, 'UTC'),
            updated_at DateTime64(3, 'UTC'),
            asset LowCardinality(String),
            price Float64,
        ) ENGINE = ReplacingMergeTree(updated_at)
         PARTITION BY toYYYYMM(created_at)
         ORDER BY (asset)
         SETTINGS index_granularity = 8192
        "#
    }

    pub fn table_name() -> &'static str {
        "test_prices"
    }

    pub fn column_names() -> Vec<&'static str> {
        vec![
            "id",
            "created_at",
            "updated_at",
            "asset",
            "price",
        ]
    }

    pub fn to_row(&self) -> (Vec<&'static str>, Vec<String>) {
        let columns = Self::column_names();

        let values = vec![
            format!("'{}'", self.id.to_string()),
            format!("'{}'", self.created_at.to_rfc3339()),
            format!("'{}'", self.updated_at.to_rfc3339()),
            format!("'{}'", self.asset),
            self.price.to_string(),
        ];

        (columns, values)
    }

    pub fn insert_query(&self) -> String {
        let (columns, values) = self.to_row();
        let columns_str = columns.join(", ");
        let values_str = values.join(", ");

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::table_name(),
            columns_str,
            values_str
        )
    }

    pub fn batch_insert_query(prices: &[Price]) -> String {
        if prices.is_empty() {
            return String::new();
        }

        let columns = Self::column_names();
        let columns_str = columns.join(", ");

        let mut query = format!(
            "INSERT INTO {} ({}) VALUES ",
            Self::table_name(),
            columns_str
        );

        let values_parts: Vec<String> = prices
            .iter()
            .map(|price| {
                let (_, values) = price.to_row();
                format!("({})", values.join(", "))
            })
            .collect();

        query.push_str(&values_parts.join(", "));
        query
    }
}
