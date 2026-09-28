use indexmap::IndexMap;
use std::hash::Hash;

/// A bounded cache that evicts the least recently used entry first.
///
/// Keys use Rust's `Hash` and `Eq` semantics, not JavaScript `Map`'s
/// SameValueZero semantics. Object keys are not automatically compared by
/// identity; use a key type whose `Hash` and `Eq` implementations provide the
/// desired identity semantics.
pub struct LruCache<K, V> {
    cache: IndexMap<K, V>,
    max_size: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash,
{
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: IndexMap::new(),
            max_size,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let index = self.cache.get_index_of(key)?;
        self.cache.move_index(index, self.cache.len() - 1);
        self.cache.get(key)
    }

    pub fn set(&mut self, key: K, value: V) {
        if self.cache.contains_key(&key) {
            let _ = self.cache.shift_remove(&key);
        } else if self.cache.len() >= self.max_size {
            let _ = self.cache.shift_remove_index(0);
        }
        self.cache.insert(key, value);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::LruCache;
    use serde_json::{Value, json};

    #[test]
    fn stores_and_retrieves_values_including_falsy_ones() {
        let mut cache = LruCache::new(10);
        cache.set("zero", json!(0));
        cache.set("empty", json!(""));
        cache.set("false", json!(false));
        cache.set("null", Value::Null);

        assert_eq!(cache.get(&"zero"), Some(&json!(0)));
        assert_eq!(cache.get(&"empty"), Some(&json!("")));
        assert_eq!(cache.get(&"false"), Some(&json!(false)));
        assert_eq!(cache.get(&"null"), Some(&Value::Null));
    }

    #[test]
    fn returns_none_for_a_missing_key() {
        let mut cache = LruCache::<&str, i32>::new(2);
        assert_eq!(cache.get(&"nope"), None);
    }

    #[test]
    fn evicts_the_least_recently_used_entry_when_over_capacity() {
        let mut cache = LruCache::new(2);
        cache.set("a", 1);
        cache.set("b", 2);
        cache.set("c", 3);

        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn promotes_an_entry_on_get_so_it_survives_eviction() {
        let mut cache = LruCache::new(2);
        cache.set("a", 1);
        cache.set("b", 2);
        assert_eq!(cache.get(&"a"), Some(&1));
        cache.set("c", 3);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn promotes_a_falsy_valued_entry_on_get_so_it_survives_eviction() {
        let mut cache = LruCache::new(2);
        cache.set("a", 0);
        cache.set("b", 2);
        assert_eq!(cache.get(&"a"), Some(&0));
        cache.set("c", 3);

        assert_eq!(cache.get(&"a"), Some(&0));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn updates_an_existing_key_without_evicting_another_entry() {
        let mut cache = LruCache::new(2);
        cache.set("a", 1);
        cache.set("b", 2);
        cache.set("b", 99);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&99));
    }

    #[test]
    fn clear_empties_the_cache() {
        let mut cache = LruCache::new(2);
        cache.set("a", 1);
        cache.set("b", 2);
        cache.clear();

        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), None);
    }
}
