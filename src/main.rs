mod key_value;

use crate::key_value::GetSetter;

fn main() {
    println!("Hello, world!");

    let store = key_value::KvStore::new("/tmp/rlvldb-test".to_owned());

    let _set = store.set(vec![1, 1], vec![81, 32]);
}
