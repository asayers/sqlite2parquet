use crate::Result;
use parquet::basic::*;
use rusqlite::Connection;
use std::fmt;

pub fn infer_schema(conn: &Connection, table: &str) -> Result<Vec<ColInfo>> {
    let mut infos = vec![];
    let mut table_info = conn.prepare(&format!("SELECT * FROM pragma_table_info('{}')", table))?;
    let mut iter = table_info.query([])?;
    while let Some(row) = iter.next()? {
        let name: String = row.get(1)?;
        let sql_type: String = row.get(2)?;
        let required: bool = row.get(3)?;
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
            x => panic!("Unknown type: {}", x),
        };
        let length = match sql_type.as_str() {
            "UUID" => 16,
            "INTERVAL" => 12,
            _ => 0,
        };
        let logical_type = match sql_type.as_str() {
            "TEXT" | "CHAR" | "VARCHAR" => Some(LogicalType::STRING(StringType::new())),
            "DATE" => Some(LogicalType::DATE(DateType::new())),
            "TIME" => Some(LogicalType::TIME(TimeType::new(
                false,
                TimeUnit::NANOS(parquet_format::NanoSeconds::new()),
            ))),
            "DATETIME" | "TIMESTAMP" => Some(LogicalType::TIMESTAMP(TimestampType::new(
                true,
                TimeUnit::NANOS(parquet_format::NanoSeconds::new()),
            ))),
            "UUID" => Some(LogicalType::UUID(UUIDType::new())),
            "JSON" => Some(LogicalType::JSON(JsonType::new())),
            "BSON" => Some(LogicalType::BSON(BsonType::new())),
            _ => None,
        };
        let repetition = if required {
            Repetition::REQUIRED
        } else {
            Repetition::OPTIONAL
        };
        let encoding = match name.as_str() {
            // TODO: Try to figure out when to do DELTA_BINARY_PACKED and when to leave it as RLE
            _ => None,
        };
        let info = ColInfo {
            name,
            physical_type,
            logical_type,
            length,
            repetition,
            encoding,
        };
        infos.push(info);
    }

    println!("{}:", table);
    for col in &infos {
        println!("    {}", col);
    }

    Ok(infos)
}

pub struct ColInfo {
    pub name: String,
    pub repetition: Repetition,
    pub physical_type: Type,
    pub length: i32,
    pub logical_type: Option<LogicalType>,
    pub encoding: Option<Encoding>,
}

impl fmt::Display for ColInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:20} {} {:20}{}{}{}",
            self.name,
            self.repetition,
            self.physical_type.to_string(),
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
        )
    }
}

impl ColInfo {
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
