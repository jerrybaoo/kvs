use anyhow::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> Result<String>;

    fn set(&self, key: String, value: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}
