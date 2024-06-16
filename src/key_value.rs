use io::BufReader;
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

impl fmt::Display for KvKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            match write!(f, "{:02x}", byte) {
                Ok(_) => continue,
                Err(e) => panic!("{}", e),
            }
        }
        Ok(())
    }
}

impl fmt::Display for KvValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            match write!(f, "{:02x}", byte) {
                Ok(_) => continue,
                Err(e) => panic!("{}", e),
            }
        }
        Ok(())
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
        file.write_all(&(key.0.len() as u32).to_le_bytes())?;
        file.write_all(&key.0)?;
        file.write_all(&(value.0.len() as u32).to_le_bytes())?;
        file.write_all(&value.0)?;
        Ok(())
    }

    pub fn get(&self, key: KvKey) -> Result<Option<Pair>> {
        let file = BufReader::new(File::open(&self.path)?);
        process_buffer(file, key)
    }

    pub fn _delete(&self, _key: KvKey) {
        todo!("Implement delete functionality")
    }
}

fn process_buffer(buf: BufReader<File>, key: KvKey) -> Result<Option<Pair>> {
    let mut index: usize = 0;

    let mut key_len: usize = 0;
    let mut key_length_byte_arr = [0u8; 8];
    let mut key_data: Vec<u8> = Vec::new();

    let mut value_len: usize = 0;
    let mut value_length_byte_arr = [0u8; 8];
    let mut value_data: Vec<u8> = Vec::new();

    let mut bytes_iter = buf.bytes();

    // we use this to determine whether the a key was skipped due to no match. This only is
    // relevant for the check outside of the loop for the last element. We initialise as true for
    // the case that the buffer is empty or too short
    let mut skipped: bool = true;

    while let Some(byte_result) = bytes_iter.next() {
        match byte_result {
            Ok(byte) => {
                // println!("{} - {:x}", index, byte);
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
                            skipped = true;
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
                        skipped = false;
                        value_data = Vec::with_capacity(value_len);
                        value_data.push(byte);
                    }
                    _ if 4 + key_len + 4 < index && index < 4 + key_len + 4 + value_len => {
                        value_data.push(byte)
                    }
                    _ if 4 + key_len + 4 + value_len == index => {
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
    if skipped {
        Ok(None)
    } else {
        Ok(Some(Pair::new(key_data, value_data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_several_keys() {
        let store = KvStore::new("/tmp/data-test/rlvldb-test".to_owned());

        let key1 = KvKey::new(vec![1, 1]);
        let key2 = KvKey::new(vec![0xaa]);
        let key3 = KvKey::new(vec![12, 34]);
        let key4 = KvKey::new(vec![1, 3]);

        let val1 = KvValue::new(vec![81, 32]);
        let val2 = KvValue::new(vec![16u8; 300]);
        let val3 = KvValue::new(vec![255, 15]);
        let val4 = KvValue::new(vec![0x77; 15]);

        let result = store.set(key1.clone(), val1.clone());
        match result {
            Ok(_) => (),
            Err(e) => panic!("{:?}", e),
        }
        let result = store.set(key2.clone(), val2.clone());
        match result {
            Ok(_) => (),
            Err(e) => panic!("{:?}", e),
        }
        let result = store.set(key3.clone(), val3.clone());
        match result {
            Ok(_) => (),
            Err(e) => panic!("{:?}", e),
        }
        let result = store.set(key4.clone(), val4.clone());
        match result {
            Ok(_) => (),
            Err(e) => panic!("{:?}", e),
        };

        // get a key after a single key
        validate_results_should_find(&store, &key3, &val3);
        // get last key
        validate_results_should_find(&store, &key4, &val4);
        // just the first element
        validate_results_should_find(&store, &key1, &val1);

        // key not found should return None
        let fake_key = KvKey::new(vec![255u8, 255u8, 255u8]);
        match store.get(fake_key.clone()) {
            Ok(Some(pair)) => {
                println!("searched key: {}", fake_key);
                println!("found key: {}", pair.key);
                println!("found value: {}", pair.value);
                panic!("nothing should have been found here")
            }
            Ok(None) => (), // do nothing we want None as result
            Err(e) => panic!("Error retrieving value for key: {:?}", e),
        };
    }

    #[test]
    fn empty_db() {
        todo!();
    }

    fn validate_results_should_find(store: &KvStore, key: &KvKey, target_val: &KvValue) {
        match store.get(key.clone()) {
            Ok(Some(pair)) => {
                assert_eq!(pair.key, key.clone());
                assert_eq!(pair.value, target_val.clone());
            }
            Ok(None) => panic!("No value found for key: {}", key),
            Err(e) => panic!("Error retrieving value for key: {:?}", e),
        };
    }
}
