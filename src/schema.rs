// schema.rs
//
// (c) 2017 James Crooks
//
// Schema object for en-/de-coding ad-hoc Avro objects

use super::codec::{AvroCodec};
use super::values::AvroValue;

pub struct Schema {
    avro_schema: AvroSchema
}

impl Schema {
    pub fn from_avsc(schema: &str) -> Option<Schema> {
        None
    }
}

pub struct Encoder {
    schema: Schema
}

impl Encoder {
    pub fn new(schema: Schema) -> Encoder {
        Encoder {
            schema: schema,
        }
    }

    pub fn from_avsc(schema: &str) -> Option<Encoder> {
        Schema::from_avsc(schema).map(Encoder::new)
    }
}

pub struct Decoder {
    schema: Schema
}

impl Decoder {
    pub fn new(schema: Schema) -> Decoder {
        Decoder {
            schema: schema,
        }
    }

    pub fn from_avsc(schema: &str) -> Option<Decoder> {
        Schema::from_avsc(schema).map(Decoder::new)
    }
}

enum AvroSchema {
    Record(RecordSchema),
    Enum(EnumSchema),
    Fixed(FixedSchema),
}

enum AvroType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record(RecordSchema),
    Enum(EnumSchema),
    Fixed(FixedSchema),
    Array(ArraySchema),
    Map(MapSchema),
    Union(UnionSchema),
}

struct Field {
    name: String,
    value: AvroType,
    default: Option<Box<AvroValue>>,
}

struct RecordSchema {
    fields: Vec<Field>,
}

struct EnumSchema {
    symbols: Vec<String>,
}

struct FixedSchema {
    size: usize,
}

struct ArraySchema {
    typ: Box<AvroType>
}

struct MapSchema {
    vtype: Box<AvroType>
}

struct UnionSchema {
    types: Vec<AvroType>
}

