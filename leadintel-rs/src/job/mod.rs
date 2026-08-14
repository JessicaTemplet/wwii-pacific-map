//! Job queue infrastructure — producer, consumer, and retry scheduler.
//!
//! Three sub-modules:
//!   producer  — enqueues jobs (writes to Redis)
//!   consumer  — dequeues and executes jobs (BRPOPLPUSH loop)
//!   scheduler — moves retry-due jobs back onto the queue (ZRANGEBYSCORE loop)

pub mod consumer;
pub mod producer;
pub mod scheduler;
