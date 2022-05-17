use crate::Result;
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
pub fn infer_schema(conn: &Connection, table: &str, n_rows: u64) -> Result<Vec<Column>> {
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

        let infer_integer = || {
            let max: i64 =
                conn.query_row(&format!("SELECT MAX({name}) FROM {table}"), [], |x| {
                    x.get(0)
                })?;
            if max <= i64::from(i32::MAX) {
                anyhow::Ok(PhysicalType::Int32)
            } else {
                anyhow::Ok(PhysicalType::Int64)
            }
        };
        let physical_type = match sql_type.as_str() {
            "BOOL" => PhysicalType::Boolean,
            "DATE" => PhysicalType::Int32,
            "TIME" => PhysicalType::Int64,
            "DATETIME" | "TIMESTAMP" => PhysicalType::Int64,
            "UUID" => PhysicalType::FixedLenByteArray(16),
            "INTERVAL" => PhysicalType::FixedLenByteArray(12),
            "BIGINT" | "SMALLINT" | "NUM" | "NUMBER" => infer_integer()?,
            x if x.starts_with("INT") => infer_integer()?,
            "TEXT" | "CHAR" | "VARCHAR" => PhysicalType::ByteArray,
            "BLOB" | "BINARY" | "VARBINARY" => PhysicalType::ByteArray,
            "JSON" | "BSON" => PhysicalType::ByteArray,
            "FLOAT" => PhysicalType::Float,
            "REAL" | "DOUBLE" => PhysicalType::Double,
            x => {
                eprintln!("Unknown type: {}", x);
                PhysicalType::ByteArray
            }
        };
        let logical_type = match sql_type.as_str() {
            "TEXT" | "CHAR" | "VARCHAR" => Some(LogicalType::String),
            "DATE" => Some(LogicalType::Date),
            "TIME" => Some(LogicalType::Time(TimeType {
                utc: false,
                unit: TimeUnit::Nanos,
            })),
            "DATETIME" | "TIMESTAMP" => Some(LogicalType::Timestamp(TimeType {
                utc: true,
                unit: TimeUnit::Nanos,
            })),
            "UUID" => Some(LogicalType::Uuid),
            "JSON" => Some(LogicalType::Json),
            "BSON" => Some(LogicalType::Bson),
            _ => None,
        };

        // TODO: Try to figure out when to do DELTA_BINARY_PACKED and when
        // to leave it as RLE
        let encoding = None;

        // Sample 1000 rows randomly and check how many of them are unique
        let n_sample = 1000.min(n_rows);
        let n_unique: u64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT {name}) FROM \
                    (SELECT {name} FROM {table} ORDER BY RANDOM() LIMIT 1000)"
            ),
            [],
            |x| x.get(0),
        )?;
        let dictionary = n_sample / n_unique >= 2;

        let query = format!("SELECT {} FROM {} ORDER BY rowid", name, table);
        let info = Column {
            name,
            physical_type,
            logical_type,
            required,
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
    pub required: bool,
    pub physical_type: PhysicalType,
    pub logical_type: Option<LogicalType>,
    pub encoding: Option<Encoding>,
    pub dictionary: bool,
    pub query: String,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    // We don't use Int96
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Encoding {
    Plain,
    Rle,
    BitPacked,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RleDictionary,
    ByteStreamSplit,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum LogicalType {
    String,
    Map,
    List,
    Enum,
    Date,
    Time(TimeType),
    Timestamp(TimeType),
    Json,
    Bson,
    Uuid,
    Unknown,
    Integer { bit_width: i8, is_signed: bool },
    // Decimal {
    //     scale: i32,
    //     precision: i32,
    // },
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct TimeType {
    pub utc: bool,
    pub unit: TimeUnit,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:20} {:5} {:20}{}{}{} {}",
            self.name,
            self.required,
            self.physical_type,
            if let Some(x) = &self.logical_type {
                format!(" ({})", x)
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

impl fmt::Display for PhysicalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PhysicalType::Boolean => write!(f, "Boolean"),
            PhysicalType::Int32 => write!(f, "Int32"),
            PhysicalType::Int64 => write!(f, "Int64"),
            PhysicalType::Float => write!(f, "Float"),
            PhysicalType::Double => write!(f, "Double"),
            PhysicalType::ByteArray => write!(f, "ByteArray"),
            PhysicalType::FixedLenByteArray(length) => write!(f, "ByteArray[{length}]"),
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogicalType::String => f.write_str("String"),
            LogicalType::Map => f.write_str("Map"),
            LogicalType::List => f.write_str("List"),
            LogicalType::Enum => f.write_str("Enum"),
            LogicalType::Date => f.write_str("Date"),
            LogicalType::Time(x) => write!(f, "Time ({x})"),
            LogicalType::Timestamp(x) => write!(f, "Timestamp ({x})"),
            LogicalType::Json => f.write_str("Json"),
            LogicalType::Bson => f.write_str("Bson"),
            LogicalType::Uuid => f.write_str("Uuid"),
            LogicalType::Unknown => f.write_str("Unknown"),
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => write!(
                f,
                "Integer ({bit_width}-bit, {})",
                if *is_signed { "signed" } else { "unsigned" }
            ),
            // LogicalType::Decimal { scale, precision } => {
            //     format!("Decimal ({scale}, {precision})")
            // }
        }
    }
}

impl fmt::Display for TimeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}, {}",
            self.unit,
            if self.utc { "UTC" } else { "local" },
        )
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            TimeUnit::Millis => "ms",
            TimeUnit::Micros => "μs",
            TimeUnit::Nanos => "ns",
        })
    }
}

impl LogicalType {
    fn as_parquet(&self) -> parquet::basic::LogicalType {
        match *self {
            LogicalType::String => parquet::basic::LogicalType::String,
            LogicalType::Map => parquet::basic::LogicalType::Map,
            LogicalType::List => parquet::basic::LogicalType::List,
            LogicalType::Enum => parquet::basic::LogicalType::Enum,
            LogicalType::Date => parquet::basic::LogicalType::Date,
            LogicalType::Time(x) => parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: x.utc,
                unit: x.unit.as_parquet(),
            },
            LogicalType::Timestamp(x) => parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: x.utc,
                unit: x.unit.as_parquet(),
            },
            LogicalType::Json => parquet::basic::LogicalType::Json,
            LogicalType::Bson => parquet::basic::LogicalType::Bson,
            LogicalType::Uuid => parquet::basic::LogicalType::Uuid,
            LogicalType::Unknown => parquet::basic::LogicalType::Unknown,
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => parquet::basic::LogicalType::Integer {
                bit_width,
                is_signed,
            },
        }
    }
}

impl TimeUnit {
    fn as_parquet(&self) -> parquet::basic::TimeUnit {
        match self {
            TimeUnit::Millis => {
                parquet::basic::TimeUnit::MILLIS(parquet_format::MilliSeconds::new())
            }
            TimeUnit::Micros => {
                parquet::basic::TimeUnit::MICROS(parquet_format::MicroSeconds::new())
            }
            TimeUnit::Nanos => parquet::basic::TimeUnit::NANOS(parquet_format::NanoSeconds::new()),
        }
    }
}

impl Column {
    pub fn as_parquet(&self) -> Result<parquet::schema::types::Type> {
        let repetition = match self.required {
            true => parquet::basic::Repetition::REQUIRED,
            false => parquet::basic::Repetition::OPTIONAL,
        };
        let (physical_type, length) = match self.physical_type {
            PhysicalType::Boolean => (parquet::basic::Type::BOOLEAN, 0),
            PhysicalType::Int32 => (parquet::basic::Type::INT32, 0),
            PhysicalType::Int64 => (parquet::basic::Type::INT64, 0),
            PhysicalType::Float => (parquet::basic::Type::FLOAT, 0),
            PhysicalType::Double => (parquet::basic::Type::DOUBLE, 0),
            PhysicalType::ByteArray => (parquet::basic::Type::BYTE_ARRAY, 0),
            PhysicalType::FixedLenByteArray(length) => {
                (parquet::basic::Type::FIXED_LEN_BYTE_ARRAY, length)
            }
        };
        let logical_type = self.logical_type.map(|x| x.as_parquet());
        Ok(
            parquet::schema::types::Type::primitive_type_builder(&self.name, physical_type)
                .with_logical_type(logical_type)
                .with_repetition(repetition)
                .with_length(length)
                .build()?,
        )
    }

    pub fn encoding(&self) -> Option<parquet::basic::Encoding> {
        Some(match self.encoding? {
            Encoding::Plain => parquet::basic::Encoding::PLAIN,
            Encoding::Rle => parquet::basic::Encoding::RLE,
            Encoding::BitPacked => parquet::basic::Encoding::BIT_PACKED,
            Encoding::DeltaBinaryPacked => parquet::basic::Encoding::DELTA_BINARY_PACKED,
            Encoding::DeltaLengthByteArray => parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DeltaByteArray => parquet::basic::Encoding::DELTA_BYTE_ARRAY,
            Encoding::RleDictionary => parquet::basic::Encoding::RLE_DICTIONARY,
            Encoding::ByteStreamSplit => parquet::basic::Encoding::BYTE_STREAM_SPLIT,
        })
    }
}
