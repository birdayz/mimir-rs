//! Ported from github.com/grafana/dskit/multierror/multierror.go.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

pub type BoxError = Arc<dyn StdError + Send + Sync + 'static>;

/// Collects multiple errors and collapses them into a single error if non-empty.
#[derive(Debug, Clone, Default)]
pub struct MultiError {
    errors: Vec<BoxError>,
}

impl MultiError {
    pub fn new(errs: impl IntoIterator<Item = Option<BoxError>>) -> Self {
        let mut merr = Self::default();
        for err in errs.into_iter().flatten() {
            merr.add(err);
        }
        merr
    }

    pub fn add(&mut self, err: BoxError) {
        if let Some(nested) = err.downcast_ref::<NonNilMultiError>() {
            self.errors.extend(nested.0.iter().cloned());
        } else {
            self.errors.push(err);
        }
    }

    pub fn add_opt(&mut self, err: Option<BoxError>) {
        if let Some(e) = err {
            self.add(e);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn len(&self) -> usize {
        self.errors.len()
    }

    pub fn err(&self) -> Option<NonNilMultiError> {
        if self.errors.is_empty() {
            None
        } else {
            Some(NonNilMultiError(self.errors.clone()))
        }
    }

    pub fn into_err(self) -> Option<NonNilMultiError> {
        if self.errors.is_empty() {
            None
        } else {
            Some(NonNilMultiError(self.errors))
        }
    }
}

/// Non-empty multi-error implementing `std::error::Error`.
#[derive(Debug, Clone)]
pub struct NonNilMultiError(pub Vec<BoxError>);

impl NonNilMultiError {
    pub fn errors(&self) -> &[BoxError] {
        &self.0
    }

    /// Recursively walks contained errors and their `source()` chains to find the first matching predicate.
    pub fn is_matching<F>(&self, pred: &F) -> bool
    where
        F: Fn(&(dyn StdError + 'static)) -> bool,
    {
        self.0.iter().any(|e| error_chain_matches(e.as_ref(), pred))
    }

    /// Recursively walks contained errors and their `source()` chains to check for type `E`.
    pub fn is_type<E: StdError + 'static>(&self) -> bool {
        self.downcast_first::<E>().is_some()
    }

    /// Recursively walks contained errors and their `source()` chains to find the first `&E`.
    pub fn downcast_first<E: StdError + 'static>(&self) -> Option<&E> {
        for e in &self.0 {
            if let Some(found) = downcast_in_chain::<E>(e.as_ref()) {
                return Some(found);
            }
        }
        None
    }
}

fn error_chain_matches<F>(err: &(dyn StdError + 'static), pred: &F) -> bool
where
    F: Fn(&(dyn StdError + 'static)) -> bool,
{
    let mut cur: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(nested) = e.downcast_ref::<NonNilMultiError>() {
            if nested.is_matching(pred) {
                return true;
            }
        }
        if pred(e) {
            return true;
        }
        cur = e.source();
    }
    false
}

fn downcast_in_chain<'a, E: StdError + 'static>(err: &'a (dyn StdError + 'static)) -> Option<&'a E> {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(found) = e.downcast_ref::<E>() {
            return Some(found);
        }
        if let Some(nested) = e.downcast_ref::<NonNilMultiError>() {
            if let Some(found) = nested.downcast_first::<E>() {
                return Some(found);
            }
        }
        cur = e.source();
    }
    None
}

impl fmt::Display for NonNilMultiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() > 1 {
            write!(f, "{} errors: ", self.0.len())?;
        }
        for (i, err) in self.0.iter().enumerate() {
            if i != 0 {
                f.write_str("; ")?;
            }
            write!(f, "{err}")?;
        }
        Ok(())
    }
}

impl StdError for NonNilMultiError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.first().map(|e| e.as_ref() as &(dyn StdError + 'static))
    }
}
