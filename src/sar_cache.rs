//! A cache for a singleton value that asynchronously refreshes after a 
//! set duration in a spawned task. The code in this module is adapted 
//! from source code in crate [async_refresh].
//!
//! [async_refresh]: <https://docs.rs/async-refresh/latest/async_refresh/index.html>

use parking_lot::RwLock;
use std::{fmt::Debug, future::Future, marker::Sync, sync::Arc};
use tokio::time::{sleep, Duration, Instant};
use tracing::{info, warn};

/// "Singleton Async Refresh Cache"
pub struct SarCache<T, E> {
    inner: Arc<RwLock<SarCacheState<T, E>>>,
}

impl<T, E> Clone for SarCache<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Cache internal state.
struct SarCacheState<T, E> {
    /// The most recently updated value.
    pub value: Arc<T>,
    /// The ts when the most recent value was updated.
    updated: Instant,
    /// The error message from the last attempted refresh. This
    /// value is set to `None` on each successful refresh.
    last_err: Option<Arc<E>>,
}

impl<T, E> Clone for SarCacheState<T, E> {
    fn clone(&self) -> Self {
        SarCacheState {
            value: self.value.clone(),
            updated: self.updated,
            last_err: self.last_err.clone(),
        }
    }
}

impl<T, E> SarCache<T, E>
where
    T: Send + 'static + Sync,
    E: Debug + Send + 'static + Sync,
{
    pub async fn new<Fut, MkFut>(
        cached_name: String,
        duration: Duration,
        mut mk_fut: MkFut,
    ) -> Result<SarCache<T, E>, E>
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        MkFut: FnMut(bool) -> Fut + Send + 'static,
    {
        let init = SarCacheState {
            value: Arc::new(mk_fut(false).await?),
            updated: Instant::now(),
            last_err: None,
        };
        let refresh = SarCache {
            inner: Arc::new(RwLock::new(init)),
        };
        let weak = Arc::downgrade(&refresh.inner);
        tokio::spawn(async move {
            loop {
                sleep(duration).await;
                let arc = match weak.upgrade() {
                    None => break,
                    Some(arc) => arc,
                };

                match mk_fut(true).await {
                    Err(e) => {
                        warn!(
                            subject="sar.cache",
                            category="sar.cache",
                            cached_name=?cached_name,
                            error=?e,
                            last_err=?arc.read().last_err,
                            last_update_elapsed_secs=arc.read().updated.elapsed().as_secs(),
                            "failed cache refresh"
                        );

                        arc.write().last_err = Some(Arc::new(e));
                    }
                    Ok(t) => {
                        let mut lock = arc.write();
                        lock.value = Arc::new(t);
                        lock.updated = Instant::now();
                        lock.last_err = None;
                        info!(
                            subject="sar.cache",
                            category="sar.cache",
                            cached_name=?cached_name,
                            last_update_elapsed_secs=?lock.updated.elapsed().as_secs(),
                            "successful cache refresh"
                        );
                    }
                }
            }
        });

        Ok(refresh)
    }

    /// Get most recent value.
    pub fn get(&self) -> Arc<T> {
        self.inner.read().value.clone()
    }

    /// Get the ts of the most recent successful update.
    pub fn get_updated(&self) -> Instant {
        self.inner.read().updated
    }

    /// Error message from the last attempted refresh (if present).
    /// Field `last_err` is set to 'None' on each successful refresh.
    pub fn get_last_err(&self) -> Option<Arc<E>> {
        self.inner.read().last_err.clone()
    }
}


#[cfg(test)]
mod tests {
    use std::{convert::Infallible, sync::Arc};
    use super::*;

    use parking_lot::RwLock;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn cache_refresh() {
        let counter = Arc::new(RwLock::new(0u32));
        let counter_clone = counter.clone();
        let make_fut = move |_| {
            let counter_clone = counter_clone.clone();
            async move {
                let mut lock = counter_clone.write();
                *lock += 1;
                Ok::<u32, Infallible>(*lock)
            }
        };
        let dur = Duration::from_millis(1);
        let x = SarCache::new("foofresh".to_string(), dur, make_fut)
            .await
            .unwrap();
        assert_eq!(*x.get(), 1);
        for _ in 0..10u32 {
            sleep(dur).await;
            assert_eq!(*x.get(), *counter.read());
        }
    }
}
