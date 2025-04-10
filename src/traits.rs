pub trait Model {
    type T;

    fn create_table_sql() -> &'static str;
    fn table_name() -> &'static str;
    fn column_names() -> Vec<&'static str>;
    fn to_row(&self) -> (Vec<&'static str>, Vec<String>);
    fn insert_query(&self) -> String;
    fn batch_insert_query(items: &[Self::T]) -> String;
    fn build_query(where_clause: Option<&str>, limit: Option<u64>, offset: Option<u64>) -> String;
}
