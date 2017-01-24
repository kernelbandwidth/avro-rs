// values.rs
//
// (c) 2017 James Crooks
//
// Avro Value types for ad-hoc data

use super::codec::{AvroCodec, ByteStream};

pub enum AvroValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Record(AvroRecord),
    Fixed(AvroFixed),
    Enum(AvroEnum),
    Array(AvroArray),
    Map(AvroMap),
    Union(AvroUnion),
}

pub struct AvroRecord {

}

impl AvroRecord {

}

impl AvroCodec for AvroRecord {
   fn encode(&self) -> Vec<u8> {
        unimplemented!();
   }

   fn decode(bytes: &mut ByteStream) -> Option<Self> {
        unimplemented!();
   }
}

pub struct AvroFixed {
    size: usize,
    data: Vec<u8>,
}

impl AvroFixed {
    pub fn new(size: usize) -> AvroFixed {
        AvroFixed {
            size: size,
            data: Vec::with_capacity(size),
        }
    }

    pub fn with_data(size: usize, data: Vec<u8>) -> Option<AvroFixed> {
        let bytes = data.into_iter().take(size).collect::<Vec<u8>>();
        if bytes.len() == size {
            Some(AvroFixed {
                size: size,
                data: bytes,
            })
        } else {
            None
        }
    }

    pub fn set_data(self, data: Vec<u8>) -> Option<AvroFixed> {
        let bytes = data.into_iter().take(self.size).collect::<Vec<u8>>();
        if bytes.len() == self.size {
            Some(AvroFixed { data: bytes, .. self })
        } else {
            None
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

pub struct AvroEnum {

}

pub struct AvroArray {

}

pub struct AvroMap {

}

pub struct AvroUnion {

}
