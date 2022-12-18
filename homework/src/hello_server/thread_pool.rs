//! Thread pool that joins all thread when dropped.

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    _id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Receiver<Job>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver.recv();

            match message {
                Ok(Job(job)) => {
                    println!("Worker {id} got a job; executing.");

                    job();
                }
                Err(_) => {
                    println!("Worker {id} disconnected; shutting down.");
                    break;
                }
            }
        });

        Worker {
            _id: id,
            thread: Some(thread),
        }
    }
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.  NOTE: that the thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        if let Some(droped_thread) = self.thread.take() {
            droped_thread.join().unwrap();
        }
    }
}

/// Internal data structure for tracking the current job status. This is shared by the worker
/// closures via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Mutex<usize>,
    empty_condvar: Condvar,
}

impl ThreadPoolInner {
    fn new() -> Self {
        ThreadPoolInner {
            job_count: Mutex::new(0),
            empty_condvar: Condvar::new(),
        }
    }

    /// Increment the job count.
    fn start_job(&self) {
        *self.job_count.lock().unwrap() += 1;
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        *self.job_count.lock().unwrap() -= 1;
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        loop {
            let curr_count = self.job_count.lock().unwrap();
            if curr_count.eq(&0) {
                break;
            }
            println!("Current Job Count : {}", curr_count);
        }
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    _workers: Vec<Worker>,
    job_sender: Option<Sender<Job>>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads. Panics if the size is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (job_sender, receiver) = unbounded();

        let receiver = Arc::new(receiver);

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        let pool_inner = Arc::new(ThreadPoolInner::new());

        ThreadPool {
            _workers: workers,
            job_sender: Some(job_sender),
            pool_inner,
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let inner_pool = self.pool_inner.clone();
        self.pool_inner.start_job();
        let job = Job(Box::new(move || {
            f();
            inner_pool.finish_job();
        }));

        if let Some(sender) = &self.job_sender {
            sender.send(job).unwrap();
        }
    }

    /// Block the current thread until all jobs in the pool have been executed.
    ///
    /// NOTE: This method has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        println!("Start Join");
        self.pool_inner.wait_empty()
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        drop(self.job_sender.take());

        for worker in &mut self._workers {
            println!("Shutting down worker {}", worker._id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
