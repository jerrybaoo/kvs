use anyhow::Result;

pub trait KvsEngine {
    fn get(&mut self, key: String) -> Result<String>;

    fn set(&mut self, key: String, value: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}
