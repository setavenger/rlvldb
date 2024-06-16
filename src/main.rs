mod key_value;

// use crate::key_value::Pair;

fn main() {
    let store = key_value::KvStore::new("./data-test/rlvldb-test".to_owned());

    let key1 = key_value::KvKey::new(vec![1, 1]);
    let key2 = key_value::KvKey::new(vec![1, 2]);
    let key3 = key_value::KvKey::new(vec![1, 3]);

    let val1 = key_value::KvValue::new(vec![81, 32]);
    let val2 = key_value::KvValue::new(vec![16u8; 300]);
    let val3 = key_value::KvValue::new(vec![255, 15]);
    let result = store.set(key1.clone(), val1.clone());
    match result {
        Ok(_) => println!("went fine"),
        Err(e) => println!("{:?}", e),
    }
    let result = store.set(key2.clone(), val2.clone());
    match result {
        Ok(_) => println!("went fine"),
        Err(e) => println!("{:?}", e),
    }
    let result = store.set(key3.clone(), val3.clone());
    match result {
        Ok(_) => println!("went fine"),
        Err(e) => println!("{:?}", e),
    }

    match store.get(key3.clone()) {
        Ok(Some(pair)) => println!("Found value for key: {:?}", pair.value),
        Ok(None) => println!("No value found for key"),
        Err(e) => println!("Error retrieving value for key: {:?}", e),
    };
}
