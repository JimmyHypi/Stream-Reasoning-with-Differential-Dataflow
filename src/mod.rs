#[deny(missing_docs)]
pub(crate) trait Loader {
    type Output;
    fn load() -> Self::Output;
    fn parse(file_name: &str) -> Vec<String> {
        
    }
}
