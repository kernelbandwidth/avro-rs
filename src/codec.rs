// avro.rs
//
// Avro Codec for primitive types
// and Avro arrays (as vectors) and Avro maps
// (as HashMap<String, T>)
// (c) 2016 James Crooks

use std::iter::Iterator;
use std::collections::HashMap;
use std::mem;

pub type ByteStream = Iterator<Item = u8>;

pub trait AvroCodec: Sized {
    fn encode(&self) -> Vec<u8>;
    fn decode(&mut ByteStream) -> Option<Self>;
}

impl AvroCodec for i32 {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        if *self == 0 {
            return vec![0u8];
        }

        let mut vint = ((*self << 1) ^ (*self >> 31)) as u32;

        let mut encoded = Vec::new();

        while vint != 0 {
            let byte = (vint | 0x80) as u8;
            encoded.push(byte);
            vint = vint >> 7;
        }

        if let Some(last) = encoded.pop() {
            encoded.push(last ^ 0x80);
        }

        encoded
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<Self> {
        let mut vint: u32 = 0;
        let mut count = 0;
        loop {
            if let Some(byte) = bytes.next() {
                vint = vint | (((byte & 0x7F) as u32) << (7 * count));
                count += 1;
                if byte & 0x80 == 0 {
                    break;
                }
            } else {
                return None;
            }
        }

        if vint & 0x1 == 1 {
            // LSB is set => negative number
            Some((vint >> 1) as i32 ^ -1i32) // flip last bit and two's complement
        } else {
            Some((vint >> 1) as i32)
        }
    }
}

impl AvroCodec for i64 {
    fn encode(&self) -> Vec<u8> {
        if *self == 0 {
            return vec![0u8];
        }

        let mut vint = ((*self << 1) ^ (*self >> 63)) as u64;

        let mut encoded = Vec::new();

        while vint != 0 {
            let byte = (vint | 0x80) as u8;
            encoded.push(byte);
            vint = vint >> 7;
        }

        if let Some(last) = encoded.pop() {
            encoded.push(last ^ 0x80);
        }

        encoded
    }

    fn decode(bytes: &mut ByteStream) -> Option<i64> {
        let mut vint: u64 = 0;
        let mut count = 0;
        loop {
            if let Some(byte) = bytes.next() {
                vint = vint | (((byte & 0x7F) as u64) << (7 * count));
                count += 1;
                if byte & 0x80 == 0 {
                    break;
                }
            } else {
                return None;
            }
        }

        if vint & 0x1 == 1 {
            // LSB is set => negative number
            Some((vint >> 1) as i64 ^ -1i64)
        } else {
            Some((vint >> 1) as i64)
        }
    }
}

impl AvroCodec for usize {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        if *self == 0 {
            return vec![0u8];
        }
        let mut vint = *self << 1; // This drops the MSB. Better keep collections under 2^63 items!
        let mut encoded = Vec::new();

        while vint != 0 {
            let byte = (vint | 0x80) as u8;
            encoded.push(byte);
            vint = vint >> 7;
        }

        if let Some(last) = encoded.pop() {
            encoded.push(last ^ 0x80);
        }

        encoded
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<Self> {
        i32::decode(bytes).map(|x| x.abs() as usize)
    }
}

impl AvroCodec for f32 {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        // Unsafe for performance
        // This use is safe, since we are just turning a 4-byte
        // object into an array of exactly 4 bytes
        unsafe { mem::transmute::<f32, [u8; 4]>(*self).to_vec() }
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<f32> {
        match (bytes.next(), bytes.next(), bytes.next(), bytes.next()) {
            (Some(b1), Some(b2), Some(b3), Some(b4)) => 
                Some(unsafe { mem::transmute::<[u8; 4], f32>([b1, b2, b3, b4]) }),
            _ => None
        }
    }
}

impl AvroCodec for f64 {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        // Unsafe for performance
        // This use is safe, since we are just turning an 8-byte
        // object into an array of exactly 8 bytes
        unsafe { mem::transmute::<f64, [u8; 8]>(*self).to_vec() }
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<f64> {
        match (bytes.next(), bytes.next(), bytes.next(), bytes.next(),
               bytes.next(), bytes.next(), bytes.next(), bytes.next()) {
            (Some(b1), Some(b2), Some(b3), Some(b4),
             Some(b5), Some(b6), Some(b7), Some(b8)) => {
                Some(unsafe 
                     { mem::transmute::<[u8; 8], f64>([b1, b2, b3, b4, b5, b6, b7, b8]) 
                     })
            },
            _ => None
        }
    }
}

impl AvroCodec for String {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        let mut bytes = self.as_bytes().to_vec();
        let mut len = bytes.len().encode();
        let mut encoded: Vec<u8> = Vec::with_capacity(bytes.len() + len.len());
        encoded.append(&mut len);
        encoded.append(&mut bytes);
        encoded
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<Self> {
        let len = if let Some(len) = usize::decode(bytes) {
            len
        } else { 
            return None;
        };

        let strdata: Vec<u8> = bytes.take(len).collect();
        if strdata.len() < len {
            return None;
        }

        String::from_utf8(strdata).ok()
    }
}

impl AvroCodec for bool {
    fn encode(&self) -> Vec<u8> {
        match *self {
            true => vec![0x1],
            false => vec![0x0]
        }
    }

    fn decode(bytes: &mut ByteStream) -> Option<bool> {
        match bytes.next() {
            Some(0u8) => Some(false),
            Some(1u8) => Some(true),
            _ => None
        }
    }
}

impl AvroCodec for u8 {
    fn encode(&self) -> Vec<u8> {
        vec![*self]
    }

    fn decode(bytes: &mut ByteStream) -> Option<u8> {
        bytes.next()
    }
}

impl<T> AvroCodec for Vec<T> where T: AvroCodec {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.append(&mut self.len().encode());
        self.iter().fold(encoded, |mut acc, item| {
            let mut encoded = item.encode();
            acc.append(&mut encoded);
            acc
        })
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<Self> {
        let mut len = if let Some(len) = usize::decode(bytes) {
            len
        } else {
            return None;
        };

        let mut ret = Vec::with_capacity(len);
        while len > 0 {
            if let Some(elem) = T::decode(bytes) {
                ret.push(elem);
            } else {
                return None;
            }
            len -= 1;
        }

        Some(ret)
    }
}

impl <T> AvroCodec for HashMap<String, T> where T: AvroCodec {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        if self.is_empty() {
            return vec![0x0];
        }

        let mut encoded = Vec::new();
        encoded.append(&mut self.len().encode());
        for (key, value) in self.iter() {
            encoded.append(&mut key.encode());
            encoded.append(&mut value.encode());
        }
        encoded.push(0x0u8);
        encoded
    }

    #[inline]
    fn decode(bytes: &mut ByteStream) -> Option<Self> {
        let mut len = match usize::decode(bytes) {
            Some(0) => return Some(HashMap::new()),
            Some(len) => len,
            None => return None,
        };

        let mut decoded = HashMap::with_capacity(len);
        while len > 0 {
            match (String::decode(bytes), T::decode(bytes)) {
                (Some(key), Some(value)) => decoded.insert(key, value),
                _ => return None,
            };
            len -= 1;
        }

        Some(decoded)
    }
}

#[cfg(test)]
mod tests {
    pub use super::AvroCodec;
    use std::{f32, f64};
    use std::collections::HashMap;

    #[test]
    fn test_i32_codec() {
        assert_eq!(0i32.encode(), vec![0u8]);
        assert_eq!(1i32.encode(), vec![2u8]);
        assert_eq!((-1i32).encode(), vec![1u8]);
        assert_eq!(i32::max_value().encode(),
                   vec![0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_eq!(i32::min_value().encode(),
                   vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_eq!(i32::max_value(),
                i32::decode(&mut i32::max_value()
                            .encode()
                            .into_iter()).unwrap());
        assert_eq!(i32::min_value(),
                i32::decode(&mut i32::min_value()
                            .encode()
                            .into_iter()).unwrap());
    }

    #[test]
    fn test_i64_codec() {
        assert_eq!(0i64.encode(), vec![0u8]);
        assert_eq!(1i64.encode(), vec![2u8]);
        assert_eq!((-1i64).encode(), vec![1u8]);
        assert_eq!(i64::max_value().encode(),
                vec![0xFE, 0xFF, 0xFF, 0xFF, 0xFF,
                     0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
        assert_eq!(i64::min_value().encode(),
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                     0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
        assert_eq!(i64::max_value(),
                i64::decode(&mut i64::max_value()
                            .encode()
                            .into_iter()).unwrap());
        assert_eq!(i64::min_value(),
                i64::decode(&mut i64::min_value()
                            .encode()
                            .into_iter()).unwrap());
    }

    #[test]
    fn test_usize_codec() {
        assert_eq!(2usize.encode(), vec![4u8]);
        assert_eq!(2usize, usize::decode(&mut vec![4u8]
                                         .into_iter()).unwrap());
        assert_eq!(2usize, usize::decode(&mut vec![3u8]
                                         .into_iter()).unwrap());
    }

    #[test]
    fn test_f32_codec() {
        assert_eq!(0f32, f32::decode(&mut 0f32.encode()
                                     .into_iter()).unwrap());
        assert_eq!(f32::MIN,
                f32::decode(&mut f32::MIN.encode()
                            .into_iter()).unwrap());
        assert_eq!(f32::MAX,
                f32::decode(&mut f32::MAX.encode()
                            .into_iter()).unwrap());
    }

    #[test]
    fn test_f64_codec() {
        assert_eq!(0f64, f64::decode(&mut 0f64.encode()
                                     .into_iter()).unwrap());
        assert_eq!(f64::MIN,
                   f64::decode(&mut f64::MIN.encode()
                               .into_iter()).unwrap());
        assert_eq!(f64::MAX,
                   f64::decode(&mut f64::MAX.encode()
                               .into_iter()).unwrap());
    }

    #[test]
    fn test_vec_i32_codec() {
        assert_eq!(Vec::<i32>::new().encode(), vec![0x0]);
        assert_eq!(vec![2i32].encode(), vec![0x2, 0x4]);
        assert_eq!(Vec::<i32>::decode(&mut vec![0x2, 0x4]
                                      .into_iter()).unwrap(), 
                   vec![2i32]);
        assert_eq!(Vec::<i32>::decode(&mut vec![0x0]
                                      .into_iter()).unwrap(), 
                   vec![]);
        assert_eq!(Vec::<i32>::decode(&mut vec![0x1, 0x1]
                                      .into_iter()).unwrap(), 
                   vec![-1i32]);
    }

    #[test]
    fn test_vec_f32_codec() {
        assert_eq!(Vec::<f32>::new().encode(), vec![0x0]);
        assert_eq!(vec![0f32, f32::MAX, f32::MIN],
                   Vec::<f32>::decode(&mut vec![0f32, f32::MAX, f32::MIN]
                                      .encode()
                                      .into_iter()).unwrap())
    }

    #[test]
    fn test_string_codec() {
        assert_eq!(String::from("abcde").encode(), 
                   vec![0x0A, 0x61, 0x62, 0x63, 0x64, 0x65]);
        assert_eq!(String::from("abcde"),
        String::decode(&mut vec![0x0A, 0x61, 0x62, 0x63, 0x64, 0x65]
                       .into_iter()).unwrap());
        assert_eq!(String::from(""), 
                   String::decode(&mut vec![0x0].into_iter()).unwrap());
    }

    #[test]
    fn test_vec_string_codec() {
        assert_eq!(Vec::<String>::new().encode(), vec![0x0]);
        assert_eq!(vec![String::from("This"), String::from("is"), 
                   String::from("a"), String::from("test.")],
                   Vec::<String>::decode(&mut vec![
                                         String::from("This"),
                                         String::from("is"),
                                         String::from("a"),
                                         String::from("test.")]
                                         .encode()
                                         .into_iter()).unwrap());
    }

    #[test]
    fn test_bool_codec() {
        assert_eq!(true.encode(), vec![0x1]);
        assert_eq!(false.encode(), vec![0x0]);
        assert_eq!(true, bool::decode(&mut vec![0x1].into_iter()).unwrap());
        assert_eq!(false, bool::decode(&mut vec![0x0].into_iter()).unwrap());
        assert_eq!(None, bool::decode(&mut vec![0x2].into_iter()));
    }

    #[test]
    fn test_byte_codec() {
        assert_eq!(0xFFu8.encode(), vec![0xFFu8]);
        assert_eq!(0xFFu8, u8::decode(&mut vec![0xFFu8].into_iter()).unwrap());
        assert_eq!(0xFFu8, u8::decode(&mut 0xFFu8.encode().into_iter()).unwrap());
    }

    #[test]
    fn test_byte_vec_codec() {
        assert_eq!(vec![0xFFu8].encode(), vec![0x02, 0xFFu8]);
        assert_eq!(Vec::<u8>::new().encode(), vec![0x0]);
        assert_eq!(vec![0xFFu8], Vec::<u8>::decode(&mut vec![0x02, 0xFFu8].into_iter()).unwrap());
        assert_eq!(vec![0xFFu8, 0xAF, 0x0],
                   Vec::<u8>::decode(&mut vec![0xFFu8, 0xAF, 0x0].encode().into_iter()).unwrap());
    }

    #[test]
    fn test_map_codec() {
        assert_eq!(HashMap::<String, i32>::new().encode(), vec![0x0]);
        assert_eq!(HashMap::<String, i32>::decode(
                &mut HashMap::<String, i32>::new().encode().into_iter())
                .unwrap(),
                HashMap::<String, i32>::new());
        let mut test_map = HashMap::<String, i32>::new();
        test_map.insert(String::from("test"), 1);
        assert_eq!(test_map, HashMap::<String, i32>::decode(
                &mut test_map.encode().into_iter()).unwrap());
    }
}
