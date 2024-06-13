mod key_value;

// use crate::key_value::Pair;

fn main() {
    println!("Hello, world!");

    let store = key_value::KvStore::new("./data-test/rlvldb-test".to_owned());

    let key = key_value::KvKey::new(vec![1, 1]);
    let val = key_value::KvValue::new(vec![81, 32]);
    let result = store.set(key.clone(), val.clone());
    match result {
        Ok(_) => println!("went fine"),
        Err(e) => println!("{:?}", e),
    }
    let result = store.set(key.clone(), val.clone());
    match result {
        Ok(_) => println!("went fine"),
        Err(e) => println!("{:?}", e),
    }
    let result = store.set(key.clone(), val.clone());
    match result {
        Ok(_) => println!("went fine"),
        Err(e) => println!("{:?}", e),
    }
}
