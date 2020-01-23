
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadPool implements Executor {

    /*-------- Static fields -----------*/
    public enum Priority {
        LOW,
        MEDIUM,
        HIGH;
    }

    private static class Task<T> implements Comparable<ThreadPool.Task<?>> {

        private final int priority;
        private final Callable<T> job;
        private final TaskFuture future = new TaskFuture();

        private T returnValue;
        private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.NEW);
        private Exception exeption;

        Task(Callable<T> job, int priority) {
            this.job = job;
            this.priority = priority;
        }

        void doTask() {
            try {
                if (state.compareAndSet(TaskState.NEW, TaskState.RUNNING)) {
                    returnValue = job.call();
                    state.set(TaskState.DONE);
                }
            } catch (Exception e) {
                state.set(TaskState.EXCEPTION);
                exeption = e;
            }
            future.taskLock.lock();
            try {
                future.finished.signalAll();
            } finally {
                future.taskLock.unlock();
            }
        }

        private enum TaskState {
            NEW,
            RUNNING,
            CANCELLED,
            EXCEPTION,
            DONE;
        }

        @Override
        public int compareTo(Task<?> o) {

            return (o.priority - this.priority);
        }

        class TaskFuture implements Future<T> {

            private final Lock taskLock = new ReentrantLock();
            private final Condition finished = taskLock.newCondition();

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return (state.compareAndSet(TaskState.NEW, TaskState.CANCELLED));
            }

            @Override
            public boolean isCancelled() {
                return (state.get() == TaskState.CANCELLED);
            }

            @Override
            public boolean isDone() {
                return ((state.get() != TaskState.NEW) && (state.get() != TaskState.RUNNING));
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                taskLock.lock();
                try {
                    while (!isDone()) {
                        finished.await();
                    }
                } finally {
                    taskLock.unlock();
                }
                
                stateCheck();
                return (returnValue);
            }

            @Override
            public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                long timeOutNanos = unit.toNanos(timeout);
                taskLock.lock();
                try {
                    while (!isDone()) {
                        if ((timeOutNanos = finished.awaitNanos(timeOutNanos)) <= 0) {
                            throw new TimeoutException("get timed out");
                        }
                    }
                    
                } finally {
                    taskLock.unlock();
                }
                
                stateCheck();
                return (returnValue);
            }
        }

        private void stateCheck() throws ExecutionException {
            switch (state.get()) {
                case EXCEPTION:
                    throw new ExecutionException(exeption);
                case CANCELLED:
                    throw new CancellationException();
            }
        }
    }

    /*---------- fields -----------*/
    private final WaitableQueueCV<Task<?>> waitableQueue = new WaitableQueueCV<>();
    private final Semaphore pauseSem = new Semaphore(1);
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);
    private CountDownLatch barrier;
    private int nThreads;

    /*--------- Methods -----------*/
 /* CTOR */
    public ThreadPool(int nThreads) {
        this.nThreads = nThreads;
        generateNewWorkerThreads(nThreads);
    }

    @Override
    public void execute(Runnable command) {
        submit(toCallable(command, null), Priority.MEDIUM);
    }

    public Future<?> submit(Runnable job, Priority p) {
        return (submit(toCallable(job, null), p));
    }

    public <T> Future<T> submit(Runnable job, Priority p, T result) {
        return (submit(toCallable(job, result), p));
    }

    public <T> Future<T> submit(Callable<T> job) {
        return (submit(job, Priority.MEDIUM));
    }

    public <T> Future<T> submit(Callable<T> job, Priority p) {
        Objects.requireNonNull(job, "submitted null task");
        Objects.requireNonNull(p, "priority cannot be null");
        shutdounCheck();
        Task<T> task = new Task<>(job, p.ordinal());
        waitableQueue.enqueue(task);
        return (task.future);
    }

    public void setNumOfThreads(int nThreads) {
        if (pauseSem.tryAcquire()) {
            int threadDiff = nThreads - this.nThreads;
            if (threadDiff >= 0) {
                generateNewWorkerThreads(threadDiff);
            } else {
                enterSpecialTasks(generateKillJob(), Priority.HIGH.ordinal() + 1, Math.abs(threadDiff));
            }

            this.nThreads = nThreads;
            pauseSem.release();
        }

    }

    public void pause() {
        shutdounCheck();
        if (pauseSem.tryAcquire()) {
            enterSpecialTasks(gneratePauseJob(), Priority.HIGH.ordinal() + 1, nThreads);
        }
    }

    public void resume() {
        shutdounCheck();
        if (!pauseSem.tryAcquire()) {
            pauseSem.release(nThreads + 1);
        } else {
            pauseSem.release();
        }
    }

    public void shutdown() {
        if (!isShutDown.get()) {
            isShutDown.set(true);
            resume();
            barrier = new CountDownLatch(nThreads);
            enterSpecialTasks(generateKillJob(), Priority.LOW.ordinal() - 1, nThreads);
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (!isShutDown.get()) {return (false);}
        return (barrier.await(timeout, unit));
    }

    /**
     * ***************** healper methods ******************
     */
    private <T> Callable<T> toCallable(Runnable run, T returnVal) {
        Objects.requireNonNull(run, "cannot submit null task");
        return (() -> {
            run.run();
            return returnVal;
        });
    }

    private void shutdounCheck() {
        if (isShutDown.get()) {
            throw new RejectedExecutionException();
        }
    }

    private void generateNewWorkerThreads(int threadNum) {
        ArrayList<WorkerThread> threadList = new ArrayList<>();
        for (int i = 0; i < threadNum; ++i) {
            threadList.add(new WorkerThread());
        }
        threadList.forEach(WorkerThread::start);
    }

    private Callable<?> generateKillJob() {
        return (()-> {
            if (Thread.currentThread() instanceof WorkerThread) {
                ((WorkerThread)(Thread.currentThread())).killThread();
            }
            
            return null;
        });
    }

    private Callable<?> gneratePauseJob() {
        return (()-> {
            pauseSem.acquire();
            return null;
        });
    }

    private void enterSpecialTasks(Callable<?> job, int priority, int numTasks) {
        for (int i = 0; i < numTasks; ++i) {
            waitableQueue.enqueue(new Task<>(job, priority));
        }
    }

    private class WorkerThread extends Thread {

        private boolean shouldRun = true;

        @Override
        public void run() {
            while (shouldRun) {
                try {
                    waitableQueue.dequeue().doTask();
                } catch (InterruptedException ex) {
                    throw new AssertionError(ex);
                }
            }

            if (isShutDown.get()) {
                barrier.countDown();
            }
        }

        /**
         * *************** WorkerThread - healper functions ******************
         */
        private void killThread() {
            shouldRun = false;
        }

    }
}
