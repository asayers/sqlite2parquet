use anyhow::anyhow;
use parquet::data_type::*;
use rusqlite::types::ValueRef;

/// Like rusqlite::FromSql, but we make our own because of the orphan rule
pub trait FromSqlite: Sized {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self>;
}
impl FromSqlite for bool {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Integer(x) => Ok(x == 1),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to a bool!")),
        }
    }
}
impl FromSqlite for i32 {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Integer(x) => Ok(i32::try_from(x)?),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to a i32")),
        }
    }
}
impl FromSqlite for i64 {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Integer(x) => Ok(x),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to an i64!")),
        }
    }
}
impl FromSqlite for Int96 {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Integer(_) => todo!(),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to a Int96")),
        }
    }
}
impl FromSqlite for f32 {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Real(x) => Ok(x as f32),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to a f32!")),
        }
    }
}
impl FromSqlite for f64 {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Real(x) => Ok(x),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to a f64!")),
        }
    }
}
impl FromSqlite for ByteArray {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Integer(x) => Ok(ByteArray::from(Vec::from(x.to_string()))),
            ValueRef::Real(x) => Ok(ByteArray::from(Vec::from(x.to_string()))),
            ValueRef::Text(x) => Ok(ByteArray::from(Vec::from(x))),
            ValueRef::Blob(x) => Ok(ByteArray::from(Vec::from(x))),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
        }
    }
}
impl FromSqlite for FixedLenByteArray {
    fn from_sqlite(x: ValueRef) -> anyhow::Result<Self> {
        match x {
            ValueRef::Text(_) => todo!(),
            ValueRef::Blob(_) => todo!(),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => Err(anyhow!("Can't convert {x:?} to a FixedLenByteArray!")),
        }
    }
}
