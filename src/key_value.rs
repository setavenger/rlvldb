use crate::key_value::io::BufReader;
use core::panic;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::{fmt, result};

#[derive(Debug)]
pub enum KvError {
    Io(io::Error),
    NotFound,
    Serialization(String),
    Other(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvError::Io(ref err) => write!(f, "IO error: {}", err),
            KvError::NotFound => write!(f, "Key not found"),
            KvError::Serialization(ref msg) => write!(f, "Serialization error: {}", msg),
            KvError::Other(ref msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::Io(err)
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
    key: KvKey,
    value: KvValue,
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

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl KvValue {
    pub fn new(data: Vec<u8>) -> Self {
        KvValue(data)
    }

    pub fn as_slice(&self) -> &[u8] {
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

    pub fn get(&self, key: KvKey) -> Result<Option<KvValue>> {
        let mut file = BufReader::new(File::open(&self.path)?);

        let mut buffer = Vec::new();

        // Read the entire file into a byte vector
        file.read_to_end(&mut buffer)?;

        // Split the buffer into lines by newline byte and process each line
        let mut start = 0;
        for (index, &item) in buffer.iter().enumerate() {
            if item == b'\n' {
                // Process the line from start to index
                process_line(&buffer[start..index], key.clone());

                // Update start to the next character after the newline
                start = index + 1;
            }
        }

        // Don't forget to process the last line if the file doesn't end with a newline
        if start < buffer.len() {
            process_line(&buffer[start..], key.clone());
        }

        fn process_line(line: &[u8], key: KvKey) {
            if line.len() < 8 {
                panic!("line was shorter than 8 bytes, key and value length were both not written.")
            }
            let mut index: usize = 0;

            // todo change when varints are introduced
            let mut len_range: [u8; 4] = line[index..4].try_into().unwrap();
            let len_key = u32::from_le_bytes(len_range);

            index += len_key;

            // Here you can handle the bytes as needed
            println!("Read line with {} bytes", line.len());
        }
        // Simulated reading logic (pseudo code)
        // if key matches return value
        // else continue
        Ok(None) // Placeholder
    }

    pub fn delete(&self, _key: KvKey) {
        todo!("Implement delete functionality")
    }
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
