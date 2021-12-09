use parquet::data_type::*;
use rusqlite::types::Value;

/// Like rusqlite::FromSql, but we make our own because of the orphan rule
pub trait FromSqlite: Sized {
    fn from_sqlite(x: Value) -> Self;
}
impl FromSqlite for bool {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Integer(x) => x == 1,
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
impl FromSqlite for i32 {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Integer(x) => i32::try_from(x).unwrap(),
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
impl FromSqlite for i64 {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Integer(x) => i64::try_from(x).unwrap(),
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
impl FromSqlite for Int96 {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Integer(_) => todo!(),
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
impl FromSqlite for f32 {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Real(x) => x as f32,
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
impl FromSqlite for f64 {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Real(x) => x,
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
impl FromSqlite for ByteArray {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Integer(x) => ByteArray::from(Vec::from(x.to_string())),
            Value::Real(x) => ByteArray::from(Vec::from(x.to_string())),
            Value::Text(x) => ByteArray::from(Vec::from(x)),
            Value::Blob(x) => ByteArray::from(x),
            Value::Null => unreachable!("Nulls are handled separately"),
        }
    }
}
impl FromSqlite for FixedLenByteArray {
    fn from_sqlite(x: Value) -> Self {
        match x {
            Value::Text(_) => todo!(),
            Value::Blob(_) => todo!(),
            Value::Null => unreachable!("Nulls are handled separately"),
            _ => panic!(),
        }
    }
}
