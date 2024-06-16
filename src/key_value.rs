use crate::key_value::io::BufReader;
use core::panic;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::{fmt, result, usize};

#[derive(Debug)]
pub enum KvError {
    Io(io::Error),
    _NotFound,
    _Serialization(String),
    Other(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvError::Io(ref err) => write!(f, "IO error: {}", err),
            KvError::_NotFound => write!(f, "Key not found"),
            KvError::_Serialization(ref msg) => write!(f, "Serialization error: {}", msg),
            KvError::Other(ref msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::Io(err)
    }
}

impl From<std::array::TryFromSliceError> for KvError {
    fn from(_err: std::array::TryFromSliceError) -> KvError {
        KvError::Other("Failed to convert bytes".to_owned())
    }
}

// Simplified to use KvError
type Result<T> = result::Result<T, KvError>;

pub struct KvStore {
    path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KvKey(Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvValue(Vec<u8>);

#[warn(dead_code)]
pub struct Pair {
    pub key: KvKey,
    pub value: KvValue,
}

impl Pair {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key: KvKey(key),
            value: KvValue(value),
        }
    }
}

impl From<Vec<u8>> for KvKey {
    fn from(item: Vec<u8>) -> Self {
        KvKey(item)
    }
}

impl From<Vec<u8>> for KvValue {
    fn from(item: Vec<u8>) -> Self {
        KvValue(item)
    }
}

impl KvKey {
    pub fn new(data: Vec<u8>) -> Self {
        KvKey(data)
    }

    pub fn _as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl KvValue {
    pub fn new(data: Vec<u8>) -> Self {
        KvValue(data)
    }

    pub fn _as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl KvStore {
    pub fn new(path_str: String) -> Self {
        let path = std::path::Path::new(&path_str);
        let prefix = path.parent().unwrap();
        std::fs::create_dir_all(prefix).unwrap();
        Self { path: path_str }
    }

    pub fn set(&self, key: KvKey, value: KvValue) -> Result<()> {
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        println!("got here");
        println!("{:?}", file);
        file.write_all(&(key.0.len() as u32).to_le_bytes())?;
        file.write_all(&key.0)?;
        file.write_all(&(value.0.len() as u32).to_le_bytes())?;
        file.write_all(&value.0)?;
        Ok(())
    }

    pub fn get(&self, key: KvKey) -> Result<Option<Pair>> {
        let file = BufReader::new(File::open(&self.path)?);

        // let mut buffer = Vec::new();

        // Read the entire file into a byte vector
        // file.read_to_end(&mut buffer)?;

        process_buffer(file, key)
        // Split the buffer into lines by newline byte and process each line
        // let mut start = 0;
        // for (index, &item) in buffer.iter().enumerate() {
        //     if item == b'\n' {
        //         // Process the line from start to index
        //         match process_line(&buffer[start..index], key.clone()) {
        //             Ok(pair) => match pair {
        //                 Some(pair_val) => return Ok(Some(pair_val)),
        //                 None => (),
        //             },
        //             Err(e) => return Err(e),
        //         }
        //
        //         // Update start to the next character after the newline
        //         start = index + 1;
        //     }
        // }
        //
        // // Don't forget to process the last line if the file doesn't end with a newline
        // if start < buffer.len() {
        //     match process_line(&buffer[start..], key.clone()) {
        //         Ok(pair) => match pair {
        //             Some(pair_val) => return Ok(Some(pair_val)),
        //             None => (),
        //         },
        //         Err(e) => return Err(e),
        //     }
        // }
        //
        // Simulated reading logic (pseudo code)
        // if key matches return value
        // else continue
        // Ok(None) // Placeholder
    }

    pub fn _delete(&self, _key: KvKey) {
        todo!("Implement delete functionality")
    }
}

fn process_buffer(buf: BufReader<File>, key: KvKey) -> Result<Option<Pair>> {
    println!("reading buffer");
    let mut index: usize = 0;

    let mut key_len: usize = 0;
    let mut key_length_byte_arr = [0u8; 8];
    let mut key_data: Vec<u8> = Vec::new();

    let mut value_len: usize = 0;
    let mut value_length_byte_arr = [0u8; 8];
    let mut value_data: Vec<u8> = Vec::new();

    let mut bytes_iter = buf.bytes();

    while let Some(byte_result) = bytes_iter.next() {
        match byte_result {
            Ok(byte) => {
                println!("{} - {:x}", index, byte);
                match index {
                    // read the key length data || first 4 bytes
                    _ if index < 4 => key_length_byte_arr[index] = byte,
                    //
                    4 => {
                        key_len = usize::from_le_bytes(key_length_byte_arr);
                        key_data = Vec::with_capacity(key_len);
                        key_data.push(byte);
                    }
                    // must be after the other check to avoid
                    _ if index < 4 + key_len => {
                        key_data.push(byte);
                    }
                    _ if 4 + key_len <= index && index < 4 + key_len + 4 => {
                        value_length_byte_arr[index - 4 - key_len] = byte;
                    }
                    _ if index == 4 + key_len + 4 => {
                        value_len = usize::from_le_bytes(value_length_byte_arr);
                        // now that we know how much we have to skip in case the key does not match
                        // we compare against the key we are looking for. Then we can easily move
                        // forward in the iterator
                        if key != KvKey::new(key_data.clone()) {
                            index = 0;
                            key_length_byte_arr = [0u8; 8];
                            // -1 for the element we are currently processing
                            // -1 again for the next call in the following iteration
                            if value_len == 0 {
                                continue;
                            } else if value_len == 1 {
                                bytes_iter.nth(value_len - 1);
                            }
                            bytes_iter.nth(value_len - 1 - 1);
                            continue;
                        }
                        value_data = Vec::with_capacity(value_len);
                        value_data.push(byte);
                    }
                    _ if 4 + key_len + 4 < index && index < 4 + key_len + 4 + value_len => {
                        value_data.push(byte)
                    }
                    _ if 4 + key_len + 4 + value_len <= index => {
                        // we only reach this step if the key matched otherwise we willskip this
                        // step anyways
                        return Ok(Some(Pair::new(key_data, value_data)));
                    }
                    _ => {
                        println!("{}", index);
                        println!("{:x}", byte);
                        panic!("unknown error")
                    }
                }
                index += 1
            }
            Err(e) => return Err(KvError::from(e)),
        }
    }

    Ok(None)
    // todo!()
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn it_works() {
//         let db = KvStore::new();
//         let key = vec![1, 1, 1, 1];
//         let value = vec![1, 2, 3, 4];
//
//         db.set(key.clone(), value.clone());
//         assert_eq!(db.get(key.clone()), Some(value));
//         db.delete(key.clone());
//         assert_eq!(db.get(key), None);
//     }
//
//     #[test]
//     fn find_nothing() {
//         let db = KvStore::new();
//         let key = vec![1, 1, 1, 1];
//         let value = vec![1, 2, 3, 4];
//
//         let key_to_try = vec![0, 0, 0, 0];
//         db.set(key.clone(), value.clone());
//         assert_ne!(db.get(key_to_try.clone()), Some(value.clone()));
//         assert_eq!(db.get(key.clone()), Some(value.clone()));
//         db.delete(key.clone());
//         assert_eq!(db.get(key), None);
//     }
// }
