// Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0.
//! Streaming Parquet decoding only. Filtering, aggregation and joins stay in SQL.

use chrono::{DateTime, Duration, NaiveDate};
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType},
    file::reader::{FileReader, SerializedFileReader},
    record::{reader::RowIter, Field},
    schema::types::Type,
};
use serde_json::{json, Value};
use std::{collections::HashSet, error::Error, fs::File};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const MAX_COLUMNS: usize = 4096;
const MAX_ROW_BYTES: usize = 16 * 1024 * 1024;

pub struct ParquetScan {
    source: File,
    root: Type,
    schema: String,
    timestamp_units: Vec<Option<TimeUnit>>,
    selected: Vec<usize>,
    iterator: Option<RowIter<'static>>,
    total_rows: i64,
    count_position: i64,
}

#[cfg(unix)]
pub fn parquet_open(fd: i32) -> Result<Box<ParquetScan>> {
    use std::os::fd::FromRawFd;
    // The C++ caller has already opened and validated this descriptor. Do not
    // reopen by filename: doing so would reintroduce a symlink TOCTOU window.
    let duplicate = unsafe { libc::dup(fd) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let file = unsafe { File::from_raw_fd(duplicate) };
    open_file(file)
}

#[cfg(not(unix))]
pub fn parquet_open(_fd: i32) -> Result<Box<ParquetScan>> {
    Err("Parquet file SQL currently requires Unix file descriptors".into())
}

fn open_file(file: File) -> Result<Box<ParquetScan>> {
    let reader = SerializedFileReader::new(file.try_clone()?)?;
    let metadata = reader.metadata().file_metadata();
    let root = metadata.schema_descr().root_schema().clone();
    let fields = root.get_fields();
    if fields.is_empty() || fields.len() > MAX_COLUMNS || metadata.num_rows() < 0 {
        return Err("invalid Parquet column or row count".into());
    }
    let mut schema = Vec::new();
    let mut timestamp_units = Vec::new();
    let mut names = HashSet::new();
    for field in fields {
        let basic = field.get_basic_info();
        if !field.is_primitive()
            || !basic.has_repetition()
            || basic.repetition() == Repetition::REPEATED
            || field.name().is_empty()
            || field.name().len() > 1024
            || field.name().contains('\0')
            || !names.insert(field.name().to_lowercase())
        {
            return Err("Parquet requires distinct, flat primitive columns".into());
        }
        let logical = basic.logical_type_ref();
        let mut unit = None;
        let kind = match logical {
            Some(LogicalType::Date) => "date",
            Some(LogicalType::Timestamp(timestamp)) => {
                unit = Some(timestamp.unit);
                "datetime"
            }
            Some(LogicalType::Integer(integer)) if integer.bit_width == 64 && !integer.is_signed => "varchar",
            Some(LogicalType::Decimal(_)) | Some(LogicalType::Time(_)) => {
                return Err("unsupported Parquet logical type".into());
            }
            _ => match field.get_physical_type() {
                PhysicalType::BOOLEAN => "boolean",
                PhysicalType::INT32 | PhysicalType::INT64 => "bigint",
                PhysicalType::FLOAT | PhysicalType::DOUBLE => "double",
                PhysicalType::BYTE_ARRAY => "varchar",
                _ => return Err("unsupported Parquet physical type".into()),
            },
        };
        schema.push(json!({"name": field.name(), "type": kind,
            "nullable": basic.repetition() == Repetition::OPTIONAL}));
        timestamp_units.push(unit);
    }
    let total_rows = metadata.num_rows();
    let selected = (0..fields.len()).collect();
    let mut scan = Box::new(ParquetScan {
        source: file, root, schema: serde_json::to_string(&schema)?, timestamp_units,
        selected, iterator: None, total_rows, count_position: 0,
    });
    scan.parquet_rewind()?;
    Ok(scan)
}

impl ParquetScan {
    pub fn parquet_schema(&self) -> String {
        self.schema.clone()
    }

    pub fn parquet_project(&mut self, columns: &[usize]) -> Result<()> {
        let mut seen = HashSet::new();
        for &index in columns {
            if index >= self.root.get_fields().len() || !seen.insert(index) {
                return Err("invalid Parquet projection".into());
            }
        }
        self.selected = columns.to_vec();
        self.parquet_rewind()
    }

    pub fn parquet_rewind(&mut self) -> Result<()> {
        self.count_position = 0;
        self.iterator = None;
        if !self.selected.is_empty() {
            let fields = self.selected.iter().map(|&index| self.root.get_fields()[index].clone()).collect();
            let projection = Type::group_type_builder(self.root.name()).with_fields(fields).build()?;
            let reader = SerializedFileReader::new(self.source.try_clone()?)?;
            self.iterator = Some(RowIter::from_file_into(Box::new(reader))
                .project(Some(projection))?.with_batch_size(64));
        }
        Ok(())
    }

    pub fn parquet_next(&mut self) -> Result<String> {
        if self.selected.is_empty() {
            if self.count_position >= self.total_rows { return Ok(String::new()); }
            self.count_position += 1;
            return Ok("[]".to_owned());
        }
        let iterator = self.iterator.as_mut().ok_or("Parquet scan is not initialized")?;
        let Some(row) = iterator.next() else { return Ok(String::new()); };
        let row = row?;
        let mut cells = Vec::new();
        for (index, (_, field)) in row.get_column_iter().enumerate() {
            if index >= self.selected.len() { return Err("Parquet column count mismatch".into()); }
            let unit = self.timestamp_units[self.selected[index]];
            cells.push(field_value(field, unit)?);
        }
        if cells.len() != self.selected.len() { return Err("Parquet column count mismatch".into()); }
        let encoded = serde_json::to_string(&cells)?;
        if encoded.len() > MAX_ROW_BYTES { return Err("Parquet row exceeds 16 MiB".into()); }
        Ok(encoded)
    }
}

fn timestamp(value: i64) -> Result<Value> {
    let date = DateTime::from_timestamp_micros(value).ok_or("Parquet timestamp out of range")?;
    Ok(Value::String(date.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
}

fn field_value(field: &Field, unit: Option<TimeUnit>) -> Result<Value> {
    let result = match field {
        Field::Null => Value::Null,
        Field::Bool(value) => json!(value),
        Field::Byte(value) => json!(value), Field::Short(value) => json!(value),
        Field::Int(value) => json!(value),
        Field::Long(value) if unit.is_some() => {
            let micros = match unit {
                Some(TimeUnit::MILLIS) => value.checked_mul(1000).ok_or("timestamp overflow")?,
                Some(TimeUnit::NANOS) => value.div_euclid(1000),
                _ => *value,
            };
            timestamp(micros)?
        }
        Field::Long(value) => json!(value),
        Field::UByte(value) => json!(value), Field::UShort(value) => json!(value),
        Field::UInt(value) => json!(value), Field::ULong(value) => json!(value.to_string()),
        Field::Float(value) if value.is_finite() => json!(value),
        Field::Double(value) if value.is_finite() => json!(value),
        Field::Str(value) if value.len() <= MAX_ROW_BYTES => json!(value),
        Field::Bytes(value) if value.len() <= MAX_ROW_BYTES => json!(std::str::from_utf8(value.data())?),
        Field::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("invalid epoch")?
                .checked_add_signed(Duration::days(*days as i64)).ok_or("Parquet date out of range")?;
            json!(date.format("%Y-%m-%d").to_string())
        }
        Field::TimestampMillis(value) => timestamp(value.checked_mul(1000).ok_or("timestamp overflow")?)?,
        Field::TimestampMicros(value) => timestamp(*value)?,
        _ => return Err("unsupported or invalid Parquet value".into()),
    };
    Ok(result)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use parquet::{basic::Compression, data_type::{ByteArray, ByteArrayType, Int64Type},
        file::{properties::WriterProperties, writer::SerializedFileWriter}, schema::parser::parse_message_type};
    use std::{os::fd::AsRawFd, sync::Arc};

    #[test]
    fn snappy_nullable_multiple_row_groups_projection_and_rescan() -> Result<()> {
        let path = std::env::temp_dir().join(format!("seekdb_parquet_{}.parquet", std::process::id()));
        let schema = Arc::new(parse_message_type("message schema { REQUIRED INT64 id; OPTIONAL BINARY text (UTF8); }")?);
        let props = Arc::new(WriterProperties::builder().set_compression(Compression::SNAPPY).build());
        let mut writer = SerializedFileWriter::new(File::create(&path)?, schema, props)?;
        for offset in [0, 3] {
            let mut group = writer.next_row_group()?;
            let mut ids = group.next_column()?.ok_or("missing id column")?;
            ids.typed::<Int64Type>().write_batch(&[offset, offset+1, offset+2], None, None)?;
            ids.close()?;
            let mut text = group.next_column()?.ok_or("missing text column")?;
            text.typed::<ByteArrayType>().write_batch(&[ByteArray::from("hello"), ByteArray::from("你好")], Some(&[1,0,1]), None)?;
            text.close()?;
            group.close()?;
        }
        writer.close()?;
        let file = File::open(&path)?;
        let mut scan = parquet_open(file.as_raw_fd())?;
        let schema: Value = serde_json::from_str(&scan.parquet_schema())?;
        assert_eq!(schema[0]["type"], "bigint");
        scan.parquet_project(&[1])?;
        assert_eq!(scan.parquet_next()?, "[\"hello\"]");
        assert_eq!(scan.parquet_next()?, "[null]");
        assert_eq!(scan.parquet_next()?, "[\"你好\"]");
        assert_eq!(scan.parquet_next()?, "[\"hello\"]");
        scan.parquet_project(&[0])?;
        for i in 0..6 { assert_eq!(scan.parquet_next()?, format!("[{i}]")); }
        assert_eq!(scan.parquet_next()?, "");
        scan.parquet_project(&[])?;
        for _ in 0..6 { assert_eq!(scan.parquet_next()?, "[]"); }
        assert_eq!(scan.parquet_next()?, "");
        drop(scan);
        drop(file);
        std::fs::remove_file(path)?;
        Ok(())
    }
}
