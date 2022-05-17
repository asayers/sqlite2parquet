use crate::Result;
use parquet::basic::*;
use rusqlite::Connection;
use std::fmt;

/// Infer a parquet schema to use for this dataset.
///
/// The goal here is to produce the schema which best fits the presented data.
/// This is a different goal from the sqlite schema, which must allow for
/// intermediate and future states.  For example: when you first insert rows
/// into your sqlite database, some of the fields are missing.  Later, you
/// go over and fill in the missing values.  According the the sqlite schema,
/// the columns are theoretically nullable; but _in fact_ there are no nulls.
/// `sqlite2parquet` will infer that these columns are required.
pub fn infer_schema(conn: &Connection, table: &str) -> Result<Vec<Column>> {
    let mut infos = vec![];
    let mut table_info = conn.prepare(&format!("SELECT * FROM pragma_table_info('{}')", table))?;
    let mut iter = table_info.query([])?;
    while let Some(row) = iter.next()? {
        let name: String = row.get(1)?;
        let sql_type: String = row.get(2)?;
        let sql_type = sql_type.to_uppercase();

        // If the schema says it's "NOT NULL" then we know there are no nulls.
        // If the schema allows nulls then we should check to see if there
        // actually are any in the data.
        let required: bool = row.get(3)?
            || conn.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {name} IS NULL"),
                [],
                |x| {
                    let x: i64 = x.get(0)?;
                    Ok(x == 0)
                },
            )?;
        let repetition = if required {
            Repetition::REQUIRED
        } else {
            Repetition::OPTIONAL
        };
        let physical_type = match sql_type.as_str() {
            "BOOL" => Type::BOOLEAN,
            "DATE" => Type::INT32,
            "TIME" => Type::INT64,
            "DATETIME" | "TIMESTAMP" => Type::INT64,
            "UUID" => Type::FIXED_LEN_BYTE_ARRAY,
            "INTERVAL" => Type::FIXED_LEN_BYTE_ARRAY,
            "INTEGER" | "BIGINT" | "INT64" => Type::INT64,
            "INT" | "SMALLINT" | "INT32" => Type::INT32,
            x if x.starts_with("INT") => Type::INT64, // Fall back to i64 for other integral types
            "TEXT" | "CHAR" | "VARCHAR" => Type::BYTE_ARRAY,
            "BLOB" | "BINARY" | "VARBINARY" => Type::BYTE_ARRAY,
            "JSON" | "BSON" => Type::BYTE_ARRAY,
            "FLOAT" => Type::FLOAT,
            "REAL" | "DOUBLE" => Type::DOUBLE,
            x => {
                eprintln!("Unknown type: {}", x);
                Type::BYTE_ARRAY
            }
        };
        let length = match sql_type.as_str() {
            "UUID" => 16,
            "INTERVAL" => 12,
            _ => 0,
        };
        let logical_type = match sql_type.as_str() {
            "TEXT" | "CHAR" | "VARCHAR" => Some(LogicalType::String),
            "DATE" => Some(LogicalType::Date),
            "TIME" => Some(LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::NANOS(parquet_format::NanoSeconds::new()),
            }),
            "DATETIME" | "TIMESTAMP" => Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS(parquet_format::NanoSeconds::new()),
            }),
            "UUID" => Some(LogicalType::Uuid),
            "JSON" => Some(LogicalType::Json),
            "BSON" => Some(LogicalType::Bson),
            _ => None,
        };

        // TODO: Try to figure out when to do DELTA_BINARY_PACKED and when
        // to leave it as RLE
        let encoding = None;
        let dictionary = match sql_type.as_str() {
            "TEXT" | "CHAR" | "VARCHAR" => true,
            _ => false,
        };
        let query = format!("SELECT {} FROM {}", name, table);
        let info = Column {
            name,
            physical_type,
            logical_type,
            length,
            repetition,
            encoding,
            dictionary,
            query,
        };
        infos.push(info);
    }
    Ok(infos)
}

pub struct Column {
    pub name: String,
    pub repetition: Repetition,
    pub physical_type: Type,
    pub length: i32,
    pub logical_type: Option<LogicalType>,
    pub encoding: Option<Encoding>,
    pub dictionary: bool,
    pub query: String,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:20} {} {:20}{}{}{}{} {}",
            self.name,
            self.repetition,
            self.physical_type,
            if self.length == 0 {
                "".to_string()
            } else {
                format!("[{}]", self.length)
            },
            if let Some(x) = &self.logical_type {
                format!(" ({:?})", x)
            } else {
                "".to_string()
            },
            if let Some(x) = &self.encoding {
                format!(" ({:?})", x)
            } else {
                "".to_string()
            },
            if self.dictionary { "+dictionary" } else { "" },
            self.query,
        )
    }
}

impl Column {
    pub fn as_parquet(&self) -> Result<parquet::schema::types::Type> {
        Ok(
            parquet::schema::types::Type::primitive_type_builder(&self.name, self.physical_type)
                .with_logical_type(self.logical_type.clone())
                .with_repetition(self.repetition)
                .with_length(self.length)
                .build()?,
        )
    }
}
