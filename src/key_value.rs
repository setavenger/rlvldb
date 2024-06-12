use crate::key_value::io::BufReader;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Write};
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

#[warn(dead_code)]
pub struct Pair {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Pair {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

pub trait GetSetter {
    fn new(path: String) -> KvStore;

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    fn delete(&self, key: Vec<u8>);
}

impl GetSetter for KvStore {
    fn new(path: String) -> KvStore {
        KvStore { path }
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        file.write_all(&(key.len() as u32).to_le_bytes())?;
        file.write_all(&key)?;
        file.write_all(&(value.len() as u32).to_le_bytes())?;
        file.write_all(&value)?;
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let file = BufReader::new(File::open(&self.path)?);

        for line in file.lines() {
            continue;
        }
        // Simulated reading logic (pseudo code)
        // if key matches return value
        // else continue
        Ok(None) // Placeholder
    }

    fn delete(&self, key: Vec<u8>) {
        todo!("not implemented yet")
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
