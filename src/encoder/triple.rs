// Trait used by some encoder components for the triple behavior
pub trait Triple<T> {
    fn s(&self) -> &T;
    fn p(&self) -> &T;
    fn o(&self) -> &T;
}

// Implementation for basic types

impl<T> Triple<T> for (T, T, T) {
    fn s(&self) -> &T {
        &self.0
    }
    fn p(&self) -> &T {
        &self.1
    }
    fn o(&self) -> &T {
        &self.2
    }
}
