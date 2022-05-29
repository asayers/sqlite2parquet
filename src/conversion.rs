use parquet::data_type::*;
use rusqlite::types::ValueRef;

/// Like rusqlite::FromSql, but we make our own because of the orphan rule
pub trait FromSqlite: Sized {
    fn from_sqlite(x: ValueRef) -> Self;
}
impl FromSqlite for bool {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Integer(x) => x == 1,
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to a bool!"),
        }
    }
}
impl FromSqlite for i32 {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Integer(x) => i32::try_from(x).unwrap(),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to a i32"),
        }
    }
}
impl FromSqlite for i64 {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Integer(x) => x,
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to an i64!"),
        }
    }
}
impl FromSqlite for Int96 {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Integer(_) => todo!(),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to a Int96"),
        }
    }
}
impl FromSqlite for f32 {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Real(x) => x as f32,
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to a f32!"),
        }
    }
}
impl FromSqlite for f64 {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Real(x) => x,
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to a f64!"),
        }
    }
}
impl FromSqlite for ByteArray {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Integer(x) => ByteArray::from(Vec::from(x.to_string())),
            ValueRef::Real(x) => ByteArray::from(Vec::from(x.to_string())),
            ValueRef::Text(x) => ByteArray::from(Vec::from(x)),
            ValueRef::Blob(x) => ByteArray::from(Vec::from(x)),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
        }
    }
}
impl FromSqlite for FixedLenByteArray {
    fn from_sqlite(x: ValueRef) -> Self {
        match x {
            ValueRef::Text(_) => todo!(),
            ValueRef::Blob(_) => todo!(),
            ValueRef::Null => unreachable!("Nulls are handled separately"),
            _ => panic!("Can't convert {x:?} to a FixedLenByteArray!"),
        }
    }
}
