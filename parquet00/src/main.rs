use std::{fs, path::Path, rc::Rc};

use parquet::{
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};

fn main() {
let path = Path::new("sample.parquet");

let message_type = "
  message schema {
    REQUIRED INT32 b;
  }
";
let schema = Rc::new(parse_message_type(message_type).unwrap());
let props = Rc::new(WriterProperties::builder().build());
let file = fs::File::create(&path).unwrap();
let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
let mut row_group_writer = writer.next_row_group().unwrap();
while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
    // ... write values to a column writer
    row_group_writer.close_column(col_writer).unwrap();
}
writer.close_row_group(row_group_writer).unwrap();
writer.close().unwrap();

let bytes = fs::read(&path).unwrap();
assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
}
