use cfg_if::cfg_if;
use parking_lot::{Condvar, Mutex, ReentrantMutex};
use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    num::NonZeroUsize,
    panic,
    pin::Pin,
    process::abort,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicIsize, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

// TODO: Job Allocator
type WorkerQueue = crossbeam_deque::Worker<Job>;
type WorkerStealer = crossbeam_deque::Stealer<Job>;
type Sleep = Arc<(Mutex<bool>, Condvar)>;

#[derive(Clone)]
pub struct JobPool {
    parallelism: NonZeroUsize,
    inner: Arc<Inner>,
}

impl JobPool {
    pub fn new(parallelism: NonZeroUsize) -> Self {
        #[cfg(feature = "tracing")]
        tracing::debug!(?parallelism, "Initializing new JobPool");

        let inner = Arc::new(Inner::new(parallelism));
        Self { parallelism, inner }
    }

    pub fn join<RL: Send, RR: Send>(
        &self,
        a: impl FnOnce() -> RL + Send,
        b: impl FnOnce() -> RR + Send,
    ) -> (RL, RR) {
        let c = JobHandle::default();
        let mut a = InlineJob::new(a);
        let mut b = InlineJob::new(b);

        unsafe {
            let mut a = a.as_job();
            a.add_child_handle(&c);
            self.enqueue_job(a);
            let mut b = b.as_job();
            b.add_child_handle(&c);
            self.enqueue_job(b);
            self.wait(c);
        }
        (
            a.result.get_mut().take().unwrap(),
            b.result.get_mut().take().unwrap(),
        )
    }

    pub fn map_reduce<T, Res>(
        &self,
        data: impl Iterator<Item = T>,
        init: impl Fn() -> Res + Send + Sync,
        map: impl Fn(T) -> Res + Send + Sync,
        reduce: impl Fn(Res, Res) -> Res + Send + Sync,
    ) -> Res
    where
        T: Send + Sync,
        Res: Send,
    {
        let map = &map;
        let init = &init;
        let reduce = &reduce;
        // NOTE: jobs must be pinned before enqueueing them so we must collect them before enqueue
        //
        let jobs = data
            .map(|t| InlineJob::new(move || map(t)))
            .collect::<Vec<_>>();
        let root = JobHandle::default();
        unsafe {
            for j in jobs.iter() {
                let mut job = j.as_job();
                job.add_child_handle(&root);
                self.enqueue_job(job);
            }
            self.wait(root);
        }

        unsafe fn reduce_recursive<T, Res>(
            js: &JobPool,
            list: &[InlineJob<T, Res>],
            init: &(impl Fn() -> Res + Send + Sync),
            reduce: &(impl Fn(Res, Res) -> Res + Send + Sync),
            max_depth: usize,
        ) -> Res
        where
            T: Send + Sync + FnOnce() -> Res,
            Res: Send,
        {
            unsafe {
                match list.len() {
                    0 => init(),
                    1 => {
                        let res = list[0].result.get();
                        (&mut *res).take().unwrap()
                    }
                    _ => {
                        if max_depth > 0 {
                            // Split the range in two and reduce them in parallel
                            //
                            // For some reason using js.join() results in stack-overflow for relatively
                            // small number of jobs. I failed to find the reason
                            //
                            // this "inline-join" has been tested with up to 10 million jobs
                            //
                            let (lhs, rhs) = list.split_at(list.len() / 2);
                            let lhs_job = InlineJob::new(move || {
                                reduce_recursive(js, lhs, init, reduce, max_depth - 1)
                            });
                            let lhs = js.enqueue_job(lhs_job.as_job());
                            let rhs = reduce_recursive(js, rhs, init, reduce, max_depth - 1);
                            js.wait(lhs);
                            let lhs = (&mut *lhs_job.result.get()).take().unwrap();
                            reduce(lhs, rhs)
                        } else {
                            let a = list[0].result.get();
                            let mut result = (&mut *a).take().unwrap();
                            for job in &list[1..] {
                                let res = job.result.get();
                                let res = (&mut *res).take().unwrap();
                                result = reduce(result, res);
                            }
                            result
                        }
                    }
                }
            }
        }
        // TODO:
        // have some sort of gateway setup where we immediately reduce incoming results from the map
        // operation?
        unsafe {
            reduce_recursive(
                self,
                &jobs,
                init,
                reduce,
                // _can_ create up to 2^x recursive jobs, so caution is required
                self.parallelism().get().ilog2() as usize,
            )
        }
    }

    pub fn scope<'a>(&'a self, f: impl FnOnce(Scope<'a>) + Send) {
        let scope = Scope {
            pool: self,
            root: Default::default(),
        };
        f(scope);
    }

    fn enqueue_job(&self, job: Job) -> JobHandle {
        unsafe {
            let res = job.as_handle();
            with_thread_index(|id| {
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    id = id,
                    data = tracing::field::debug(job.data),
                    "Enqueueing job"
                );
                if job.ready() {
                    self.inner.runnable_queues[id].push(job);
                    // wake up a worker
                    self.inner.sleep.1.notify_one();
                } else {
                    (&mut *self.inner.wait_lists[id].get()).push(job);
                }
                res
            })
        }
    }

    pub fn enqueue_future<F: std::future::Future + Send>(&self, f: F) -> JobHandle {
        let job = FutureJob::new(f);
        unsafe {
            let job = job.into_job();
            self.enqueue_job(job)
        }
    }

    pub fn enqueue_graph<T: Send + AsJob>(&self, graph: HomogeneousJobGraph<T>) -> JobHandle {
        let jobs: Vec<_> = graph.get_jobs().into_iter().collect();
        let data = graph.jobs;
        let root = BoxedJob::new(move || {
            // take ownership of the data
            // dropping it when the final, root, job is executed
            let _d = data;
        });
        unsafe {
            let root = root.into_job();
            for mut job in jobs {
                job.add_child(&root);
                self.enqueue_job(job);
            }

            self.enqueue_job(root)
        }
    }

    /// Run the provided job graph, blocking until all jobs have finished
    pub fn run_graph<T: Send + AsJob>(&self, graph: &HomogeneousJobGraph<T>) {
        let jobs = graph.get_jobs();
        let root = InlineJob::new(|| {});
        unsafe {
            let root = root.as_job();
            for mut job in jobs {
                job.add_child(&root);
                self.enqueue_job(job);
            }
            self.wait(self.enqueue_job(root))
        }
    }

    pub fn wait(&self, job: JobHandle) {
        unsafe {
            with_thread_index(|id| {
                let q = &*self.inner.runnable_queues as *const _;
                let s = &*self.inner.runnable_stealers as *const _;
                let wait_list = NonNull::new(self.inner.wait_lists[id].get()).unwrap();
                let mut tmp_exec = Executor::new(
                    id,
                    QueueArray(q),
                    StealerArray(s),
                    wait_list,
                    Arc::clone(&self.inner.sleep),
                );
                while !job.done() {
                    if tmp_exec.run_once().is_err() {
                        // busy wait so `wait` returns asap
                        std::hint::spin_loop();
                    }
                }
            });
        }
    }

    pub fn parallelism(&self) -> NonZeroUsize {
        self.parallelism
    }
}

impl Default for JobPool {
    fn default() -> Self {
        unsafe {
            let parallelism =
                std::thread::available_parallelism().unwrap_or(NonZeroUsize::new_unchecked(1));

            Self::new(parallelism)
        }
    }
}

struct Inner {
    threads: Vec<JoinHandle<()>>,
    runnable_queues: Pin<Box<[WorkerQueue]>>,
    runnable_stealers: Pin<Box<[WorkerStealer]>>,
    /// threads may only access their own waiting_queues
    wait_lists: Pin<Box<[UnsafeCell<Vec<Job>>]>>,
    sleep: Sleep,
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

impl Drop for Inner {
    fn drop(&mut self) {
        {
            *self.sleep.0.lock() = true;
            self.sleep.1.notify_all();
        }
        for j in self.threads.drain(..) {
            j.join().unwrap_or(());
        }
    }
}

thread_local! {
    static THREAD_INDEX: UnsafeCell<usize> = const { UnsafeCell::new(0) };
}

// FIXME: should have a ZERO_LOCK per JobPool instead of globally
static ZERO_LOCK: ReentrantMutex<()> = ReentrantMutex::new(());

fn with_thread_index<R>(f: impl FnOnce(usize) -> R) -> R {
    let id = unsafe { THREAD_INDEX.with(|id| *id.get()) };
    // unitinialized threads use the the default queues
    let _lock = if id == 0 {
        Some(ZERO_LOCK.lock())
    } else {
        None
    };
    f(id)
}

/// Context for a worker thread
struct Executor {
    id: usize,
    steal_id: usize,
    queues: QueueArray,
    stealer: StealerArray,
    wait_list: NonNull<Vec<Job>>,
    sleep: Sleep,
}

unsafe impl Send for Executor {}
unsafe impl Sync for Executor {}

impl Executor {
    fn new(
        id: usize,
        queues: QueueArray,
        stealer: StealerArray,
        wait_list: NonNull<Vec<Job>>,
        sleep: Sleep,
    ) -> Self {
        Self {
            id,
            steal_id: id,
            queues,
            stealer,
            wait_list,
            sleep,
        }
    }

    /// # Safety
    ///
    /// Caller must ensure that the thread is joined before the queues are destroyed
    unsafe fn worker_thread(&mut self) {
        unsafe {
            THREAD_INDEX.with(move |tid| {
                *tid.get() = self.id;
                // busy wait for a few iterations in case a new job is enqueued right before this
                // thread would sleep
                let mut fails = 0;
                loop {
                    while fails < 8 {
                        match self.run_once() {
                            Err(_) => {
                                fails += 1;
                                spin_loop();
                            }
                            Ok(_) => fails = 0,
                        }
                    }
                    fails = 0;
                    let (lock, cv) = &*self.sleep;
                    let mut l = lock.lock();
                    // TODO: config wait time
                    cv.wait_for(&mut l, Duration::from_millis(10));
                    if *l {
                        break;
                    }
                }
            });
        }
    }

    /// # Safety
    ///
    /// Caller must ensure that the queues outlive run_once
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip_all, level = "trace", fields(executor_id))
    )]
    unsafe fn run_once(&mut self) -> Result<(), RunError> {
        unsafe {
            let queues = &*self.queues.0;
            let stealers = &*self.stealer.0;
            let executor_id = self.id;
            if let Some(mut job) = queues[executor_id].pop() {
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    executor_id = executor_id,
                    data = tracing::field::debug(job.data),
                    "Executing job"
                );
                match job.execute() {
                    ExecutionState::Done => {}
                    ExecutionState::Reenqueue => {
                        queues[executor_id].push(job);
                    }
                }
                return Ok(());
            }
            // if pop fails try to steal from another thread
            let qlen = queues.len();
            let next_id = move |i: &mut Self| {
                i.steal_id = (i.steal_id + 1) % qlen;
            };
            loop {
                let mut retry = false;
                for _ in 0..queues.len() {
                    if self.steal_id == executor_id {
                        next_id(self);
                        continue;
                    }
                    let stealer = &stealers[self.steal_id];
                    match stealer.steal_batch(&queues[self.id]) {
                        crossbeam_deque::Steal::Success(_) => {
                            // do not increment the id if steal succeeds
                            // next time this executor is out of jobs try stealing from the same queue
                            // first
                            return Ok(());
                        }
                        crossbeam_deque::Steal::Empty => {}
                        crossbeam_deque::Steal::Retry => {
                            retry = true;
                        }
                    }
                    next_id(self);
                }
                if !retry {
                    break;
                }
            }
            // if stealing fails too try to promote waiting items
            let wait_list = self.wait_list.as_mut();
            let mut promoted = false;
            for i in (0..wait_list.len()).rev() {
                debug_assert!(!wait_list[i].done());
                if wait_list[i].ready() {
                    let job = wait_list.swap_remove(i);
                    #[cfg(feature = "tracing")]
                    let data = job.data;
                    queues[executor_id].push(job);
                    self.sleep.1.notify_one();
                    promoted = true;

                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        executor_id = executor_id,
                        data = tracing::field::debug(data),
                        "Promoted job to runnable"
                    );
                }
            }
            promoted.then_some(()).ok_or(RunError::StealFailed)
        }
    }
}

#[derive(Debug, Clone)]
enum RunError {
    StealFailed,
}

struct QueueArray(*const [WorkerQueue]);
struct StealerArray(*const [WorkerStealer]);
unsafe impl Send for QueueArray {}
unsafe impl Send for StealerArray {}

impl Inner {
    pub fn new(workers: NonZeroUsize) -> Self {
        let workers = workers.get();
        let mut queues = Vec::with_capacity(workers + 1);
        for _ in 0..=workers {
            queues.push(WorkerQueue::new_fifo());
        }
        let queues = Pin::new(queues.into_boxed_slice());
        let stealers = Pin::new(
            queues
                .iter()
                .map(|q| q.stealer())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let q = &*queues as *const _;
        let s = &*stealers as *const _;
        let sleep = Arc::default();
        let mut result = Self {
            sleep: Arc::clone(&sleep),
            runnable_queues: queues,
            runnable_stealers: stealers,
            threads: Vec::with_capacity(workers + 1),
            wait_lists: Pin::new(
                (0..=workers)
                    .map(|_| Default::default())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
        };
        // the main thread is also used a worker on wait points
        for i in 1..workers {
            let arr = QueueArray(q);
            let st = StealerArray(s);
            let wait_list = NonNull::new(result.wait_lists[i].get()).unwrap();
            let mut worker = Executor::new(i, arr, st, wait_list, Arc::clone(&sleep));
            result.threads.push(
                std::thread::Builder::new()
                    .name(format!("cecs worker {i}"))
                    .spawn(move || unsafe {
                        if let Err(err) = panic::catch_unwind(panic::AssertUnwindSafe(move || {
                            worker.worker_thread();
                        })) {
                            eprintln!("cecs worker {i} paniced: {err:?}\naborting");
                            abort();
                        }
                    })
                    .expect("Failed to create worker thread"),
            );
        }
        // the last id is assigned to the main thread (current thread)
        // id 0 is reserved for foreign threads entering the jobsystem
        THREAD_INDEX.with(move |tid| unsafe {
            *tid.get() = workers;
        });
        result
    }
}

lazy_static::lazy_static!(
    pub static ref JOB_POOL: JobPool = {
        Default::default()
    };
);

type Todos = Arc<AtomicIsize>;

#[derive(Default, Debug, Clone)]
pub struct JobHandle {
    tasks_left: Todos,
}

impl std::future::Future for JobHandle {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.done() {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

impl JobHandle {
    pub fn done(&self) -> bool {
        let left = self.tasks_left.load(Ordering::Relaxed);
        debug_assert!(left >= 0, "tasks_left should be non-negative {left}");
        left <= 0
    }
}

pub trait AsJob: Send {
    unsafe fn execute(instance: *const ()) -> ExecutionState;
}

pub enum ExecutionState {
    Done,
    Reenqueue,
}

// Executor should deal in jobs
// The public API should be job graphs
#[derive(Debug)]
pub(crate) struct Job {
    tasks_left: Todos,
    children: Vec<Todos>,
    func: unsafe fn(*const ()) -> ExecutionState,
    data: *const (),
}

unsafe impl Send for Job {}

impl Job {
    /// # Safety
    ///
    /// Caller must ensure that `data` outlives the Job
    unsafe fn new<T: AsJob>(data: *const T) -> Self {
        Self {
            tasks_left: Todos::new(1.into()),
            children: Vec::new(),
            func: T::execute,
            data: data.cast(),
        }
    }

    #[must_use]
    pub fn execute(&mut self) -> ExecutionState {
        debug_assert!(self.ready());
        unsafe {
            let res = (self.func)(self.data);
            if let ExecutionState::Done = res {
                self.data = std::ptr::null();
                for dep in self.children.iter() {
                    dep.fetch_sub(1, Ordering::Relaxed);
                }
                self.tasks_left.fetch_sub(1, Ordering::Release);
            }
            res
        }
    }

    pub fn done(&self) -> bool {
        let left = self.tasks_left.load(Ordering::Relaxed);
        debug_assert!(left >= 0);
        left <= 0 && self.data.is_null()
    }

    pub fn ready(&self) -> bool {
        let left = self.tasks_left.load(Ordering::Relaxed);
        debug_assert!(left >= 0);
        left == 1 && !self.data.is_null()
    }

    pub fn add_child(&mut self, child: &Job) {
        debug_assert!(!self.done());
        debug_assert!(!child.done());
        self.children.push(Arc::clone(&child.tasks_left));
        child.tasks_left.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_child_handle(&mut self, child: &JobHandle) {
        debug_assert!(!self.done());
        self.children.push(Arc::clone(&child.tasks_left));
        child.tasks_left.fetch_add(1, Ordering::Relaxed);
    }

    fn as_handle(&self) -> JobHandle {
        JobHandle {
            tasks_left: Arc::clone(&self.tasks_left),
        }
    }
}

#[derive(Default)]
pub enum JobResult<R> {
    Done(R),
    Panic,
    #[default]
    Pending,
    Reenqueue,
}

impl<R> JobResult<R> {
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    pub fn unwrap(self) -> R {
        match self {
            JobResult::Done(r) => r,
            JobResult::Panic => panic!("Job paniced"),
            JobResult::Reenqueue | JobResult::Pending => panic!("Job is pending"),
        }
    }
}

pub struct InlineJob<F, R>
where
    F: FnOnce() -> R,
{
    inner: UnsafeCell<Option<F>>,
    result: UnsafeCell<JobResult<R>>,
}

unsafe impl<F, R> Send for InlineJob<F, R> where F: FnOnce() -> R {}
unsafe impl<F, R> Sync for InlineJob<F, R> where F: FnOnce() -> R {}

impl<F, R> InlineJob<F, R>
where
    F: FnOnce() -> R,
{
    pub fn new(inner: F) -> Self {
        Self {
            inner: UnsafeCell::new(Some(inner)),
            result: Default::default(),
        }
    }

    /// # Safety caller must ensure that the instance outlives the job
    pub(crate) unsafe fn as_job(&self) -> Job
    where
        F: Send,
        R: Send,
    {
        unsafe { Job::new(self) }
    }
}

unsafe fn execute_job<R: Send>(f: impl FnOnce() -> R + Send) -> JobResult<R> {
    match panic::catch_unwind(panic::AssertUnwindSafe(f)) {
        Ok(res) => JobResult::Done(res),
        Err(err) => {
            cfg_if!(
                if #[cfg(feature = "tracing")] {
                    tracing::error!("Job panic: {err:?}");
                } else {
                    eprintln!("Job panic: {err:?}");
                }
            );

            JobResult::Panic
        }
    }
}

impl<F: FnOnce() -> R + Send, R: Send> AsJob for InlineJob<F, R> {
    unsafe fn execute(instance: *const ()) -> ExecutionState {
        unsafe {
            let instance: *const Self = instance.cast();
            let instance = &*instance;
            let inner = (&mut *instance.inner.get()).take();
            let result = execute_job(inner.unwrap());
            std::ptr::write(instance.result.get(), result);
            ExecutionState::Done
        }
    }
}

pub struct BoxedJob<F> {
    inner: F,
}

impl<F> AsJob for BoxedJob<F>
where
    F: FnOnce() + Send,
{
    unsafe fn execute(instance: *const ()) -> ExecutionState {
        unsafe {
            let instance: Box<Self> = Box::from_raw(instance.cast_mut().cast());
            execute_job(instance.inner);
            ExecutionState::Done
        }
    }
}

impl<F> BoxedJob<F> {
    pub fn new(inner: F) -> Box<Self> {
        Box::new(Self { inner })
    }

    /// # Safety caller must ensure that the job is executed exactly once
    /// The job takes ownership of self
    pub(crate) unsafe fn into_job(self: Box<Self>) -> Job
    where
        F: FnOnce() + Send,
    {
        unsafe { Job::new(Box::into_raw(self)) }
    }
}

pub struct Scope<'a> {
    pool: &'a JobPool,
    root: JobHandle,
}

impl<'a> Drop for Scope<'a> {
    fn drop(&mut self) {
        if !self.root.done() {
            self.pool.wait(self.root.clone());
        }
    }
}

pub struct FutureJob<F> {
    inner: Pin<Box<F>>,
}

impl<F> AsJob for FutureJob<F>
where
    F: std::future::Future + Send,
{
    unsafe fn execute(instance: *const ()) -> ExecutionState {
        unsafe {
            let mut instance: Box<Self> = Box::from_raw(instance.cast_mut().cast());
            // let instance: &mut Self = &mut *instance.cast_mut().cast();
            let f = || {
                futures_lite::future::block_on(futures_lite::future::poll_once(&mut instance.inner))
            };
            let result = match panic::catch_unwind(panic::AssertUnwindSafe(f)) {
                Ok(res) => match res {
                    Some(_) => JobResult::Done(res),
                    None => {
                        Box::leak(instance);
                        JobResult::Reenqueue
                    }
                },
                Err(err) => {
                    cfg_if!(
                        if #[cfg(feature = "tracing")] {
                            tracing::error!("Job panic: {err:?}");
                        } else {
                            eprintln!("Job panic: {err:?}");
                        }
                    );

                    JobResult::Panic
                }
            };
            match result {
                JobResult::Reenqueue => ExecutionState::Reenqueue,
                _ => ExecutionState::Done,
            }
        }
    }
}

impl<F> FutureJob<F> {
    pub fn new(inner: F) -> Box<Self> {
        Box::new(Self {
            inner: Box::pin(inner),
        })
    }

    /// # Safety caller must ensure that the job is executed exactly once
    /// The job takes ownership of self
    pub(crate) unsafe fn into_job(self: Box<Self>) -> Job
    where
        F: std::future::Future + Send,
    {
        unsafe { Job::new(Box::into_raw(self)) }
    }
}

impl<'a> Scope<'a> {
    pub fn new(pool: &'a JobPool) -> Self {
        Self {
            pool,
            root: Default::default(),
        }
    }

    pub fn spawn(&self, task: impl FnOnce(Scope<'a>) + Send) {
        let child_scope = Scope {
            pool: self.pool,
            root: Default::default(),
        };
        let job = BoxedJob::new(move || {
            task(child_scope);
        });
        unsafe {
            let mut job = job.into_job();
            job.add_child_handle(&self.root);
            self.pool.enqueue_job(job);
        }
    }
}

pub struct HomogeneousJobGraph<T> {
    jobs: Vec<T>,
    edges: Vec<[usize; 2]>,
}

impl<T: Clone> Clone for HomogeneousJobGraph<T> {
    fn clone(&self) -> Self {
        Self {
            jobs: self.jobs.clone(),
            edges: self.edges.clone(),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for HomogeneousJobGraph<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HomogeneousJobGraph")
            .field("jobs", &self.jobs)
            .field("edges", &self.edges)
            .finish_non_exhaustive()
    }
}

impl<T> HomogeneousJobGraph<T>
where
    T: AsJob,
{
    pub fn new(data: impl Into<Vec<T>>) -> Self {
        let data = data.into();
        Self {
            jobs: data,
            edges: Default::default(),
        }
    }

    pub fn add_dependency(&mut self, parent: usize, child: usize) {
        debug_assert_ne!(parent, child);
        self.edges.push([parent, child]);
    }

    fn get_jobs<'a>(&'a self) -> impl IntoIterator<Item = Job> + 'a {
        // # Safety
        //
        // The jobs are only valid while this graph is not modified or dropped
        //
        unsafe {
            let mut jobs = self.jobs.iter().map(|d| Job::new(d)).collect::<Vec<_>>();
            for [parent, child] in self.edges.iter().copied() {
                debug_assert_ne!(parent, child);
                let parent = &mut *jobs.as_mut_ptr().add(parent);
                let child = &*jobs.as_ptr().add(child);
                parent.add_child(child);
            }
            jobs
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{World, query::Query};

    use super::*;

    #[test]
    #[cfg_attr(feature = "tracing", tracing_test::traced_test)]
    fn join_test() {
        let pool = JobPool::default();

        let a = 42;
        let b = 69;

        let (a, b) = pool.join(move || a + 1, move || b + 1);

        assert_eq!(a, 43);
        assert_eq!(b, 70);
    }

    #[test]
    #[cfg_attr(feature = "tracing", tracing_test::traced_test)]
    fn scope_test() {
        let pool = JobPool::default();

        let a = AtomicIsize::new(0);
        let b = AtomicIsize::new(0);

        pool.scope(|s| {
            s.spawn(|_s| {
                a.fetch_add(1, Ordering::Relaxed);
            });
            s.spawn(|_s| {
                b.fetch_add(1, Ordering::Relaxed);
            });
        });

        assert_eq!(a.load(Ordering::Relaxed), 1);
        assert_eq!(b.load(Ordering::Relaxed), 1);
    }

    #[test]
    #[cfg_attr(feature = "tracing", tracing_test::traced_test)]
    fn map_reduce_test() {
        let pool = JobPool::default();

        /// make sure that some jobs queue up
        fn throttle() {
            std::thread::sleep(Duration::from_micros(1));
        }

        const N_JOBS: i32 = 10_000;

        let result = pool.map_reduce(
            (0..N_JOBS).map(|_| 1),
            || 0i32,
            |i| i * 2i32,
            |a, b| {
                throttle();
                a + b
            },
        );

        assert_eq!(result, N_JOBS * 2);
    }

    #[test]
    fn test_running_future_on_the_jobsystem() {
        let js = JobPool::default();

        let result = Arc::new(Mutex::new(0i32));

        let handle = {
            let result = result.clone();
            js.enqueue_future(async move {
                futures_lite::future::yield_now().await;
                *result.lock().unwrap() = futures_lite::future::ready(42).await;
            })
        };

        futures_lite::future::block_on(handle);

        assert_eq!(*result.lock().unwrap(), 42);
    }

    #[test]
    fn test_optional_par_iter() {
        let mut world = World::new(1024);

        world
            .run_system(|mut cmd: crate::prelude::Commands| {
                for i in 0..1024 {
                    let cmd = cmd.spawn().insert("foo");
                    if i % 2 == 0 {
                        cmd.insert(i);
                    }
                }
            })
            .unwrap();

        world
            .run_stage(
                crate::prelude::SystemStage::new("test")
                    .with_system(|q: Query<(&i32, Option<&&'static str>)>| {
                        let count = AtomicIsize::new(0);
                        q.par_for_each(|_| {
                            count.fetch_add(1, Ordering::Relaxed);
                        });

                        assert_eq!(count.load(Ordering::Relaxed), 512);
                    })
                    .with_system(|mut q: Query<(&mut i32, Option<&mut &'static str>)>| {
                        let count = AtomicIsize::new(0);
                        q.par_for_each_mut(|_| {
                            count.fetch_add(1, Ordering::Relaxed);
                        });

                        assert_eq!(count.load(Ordering::Relaxed), 512);
                    })
                    .build(),
            )
            .unwrap();
    }
}
