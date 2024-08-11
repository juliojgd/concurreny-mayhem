

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.tan;
import static java.lang.Math.atan;
import static java.lang.Math.cbrt;


import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.stream.IntStream;


public class VTvsPlatformThreadsTest {

    private static Logger LOG = LoggerFactory.getLogger(VTvsPlatformThreadsTest.class);

    private static int LIGHT_CPU_OP_ITERATIONS = 100_000;
    private static int HEAVY_CPU_OP_ITERATIONS = LIGHT_CPU_OP_ITERATIONS * 1000;
    private static int MEDIUM_CPU_OP_ITERATIONS = LIGHT_CPU_OP_ITERATIONS * 12;
    private static int HEAVY_CPU_OP_ITERATIONS_SLICED_IN_FOUR = HEAVY_CPU_OP_ITERATIONS / 4;

    // VT configuration
    private static int MAX_VIRTUALTHREADS_FJP_SIZE = 2;

    // Platform Threads configuration
    private static int MAX_PLATFORMTHREADS_FIXEDTP_SIZE = 2;

    @Test
    void cpuBoundTasksWithPlatformThreads() throws InterruptedException {
        final var totalPlatFormThreads = MAX_PLATFORMTHREADS_FIXEDTP_SIZE;
        final var totalTasks = 6;
        final var cpuBoundTasks = 4;
        final var lightCpuTasks = totalTasks - cpuBoundTasks;

        LOG.info("FixedThreadPool size={}", totalPlatFormThreads);
        ExecutorService myExecutor = Executors.newFixedThreadPool(totalPlatFormThreads);
        var myFutures = new ArrayList<Future<?>>(totalTasks);
        CountDownLatch latch = new CountDownLatch(totalTasks);
        IntStream.range(0, cpuBoundTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myCpuBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        TimeUnit.MILLISECONDS.sleep(1000);
        for (int i = 0; i < lightCpuTasks; i++) {
            int finalI = i;
            myFutures.add(myExecutor.submit(() -> {
                myLightCpuBoundRunnable(finalI).run();
                latch.countDown();
            }));
        }
        latch.await();
    }

    @Test
    void cpuBoundTasksWithVirtualThreads() throws InterruptedException {
        final var totalTasks = 6;
        final var cpuBoundTasks = 4;
        final var lightCpuTasks = totalTasks - cpuBoundTasks;

        LOG.info("Value of -Djdk.virtualThreadScheduler.maxPoolSize={}", System.getProperty("jdk.virtualThreadScheduler.maxPoolSize"));
        assert String.valueOf(MAX_VIRTUALTHREADS_FJP_SIZE).equals(System.getProperty("jdk.virtualThreadScheduler.maxPoolSize"));
        ExecutorService myExecutor = Executors.newVirtualThreadPerTaskExecutor();
        var myFutures = new ArrayList<Future<?>>(totalTasks);
        CountDownLatch latch = new CountDownLatch(totalTasks);
        IntStream.range(0, cpuBoundTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myCpuBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        TimeUnit.MILLISECONDS.sleep(1000);
        IntStream.range(0, lightCpuTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myLightCpuBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        latch.await();
    }

    @Test
    void ioBoundTasksWithPlatformThreads() throws InterruptedException {
        final var totalPlatFormThreads = MAX_PLATFORMTHREADS_FIXEDTP_SIZE;
        final var totalTasks = 6;
        final var ioBoundTasks = 4;
        final var lightCpuTasks = totalTasks - ioBoundTasks;

        LOG.info("FixedThreadPool size={}", totalPlatFormThreads);
        ExecutorService myExecutor = Executors.newFixedThreadPool(totalPlatFormThreads);
        var myFutures = new ArrayList<Future<?>>(totalTasks);
        CountDownLatch latch = new CountDownLatch(totalTasks);
        IntStream.range(0, ioBoundTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myIoBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        TimeUnit.MILLISECONDS.sleep(1000);
        IntStream.range(0, lightCpuTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myLightCpuBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        latch.await();
    }

    @Test
    void ioBoundTasksWithVirtualThreads() throws InterruptedException {
        final var totalTasks = 6;
        final var ioBoundTasks = 4;
        final var lightCpuTasks = totalTasks - ioBoundTasks;

        LOG.info("Value of -Djdk.virtualThreadScheduler.maxPoolSize={}", System.getProperty("jdk.virtualThreadScheduler.maxPoolSize"));
        assert String.valueOf(MAX_VIRTUALTHREADS_FJP_SIZE).equals(System.getProperty("jdk.virtualThreadScheduler.maxPoolSize"));
        ExecutorService myExecutor = Executors.newVirtualThreadPerTaskExecutor();
        final var myFutures = new ArrayList<Future<?>>(totalTasks);
        final var latch = new CountDownLatch(totalTasks);
        IntStream.range(0, ioBoundTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myIoBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        TimeUnit.MILLISECONDS.sleep(1000);
        IntStream.range(0, lightCpuTasks).forEach(taskId ->
                myFutures.add(myExecutor.submit(() -> {
                    myLightCpuBoundRunnable(taskId).run();
                    latch.countDown();
                })));

        latch.await();
    }


    void myCpuBoundRunnableTest() {
        myCpuBoundRunnable(1).run();

    }

    @Test
    void myMixedBoundRunnableTest() {
        myMixedBoundRunnable().run();
    }


    void myIoBoundRunnableTest() {
        myIoBoundRunnable(1).run();
    }

    private Runnable myCpuBoundRunnable(int taskId) {
        return () -> {

            LOG.info("I am {} , running myCpuBoundRunnable task C-{} and I have STARTED", Thread.currentThread(), taskId);

            costlyCPUOperation(HEAVY_CPU_OP_ITERATIONS);

            LOG.info("I am {} , running myCpuBoundRunnable task C-{} and I have FINISHED", Thread.currentThread(), taskId);
        };
    }


    private Runnable myLightCpuBoundRunnable(int taskId) {
        return () -> {
            LOG.info("I am {}, running myLightCpuBoundRunnable task LC-{} and I have STARTED", Thread.currentThread(), taskId);
            costlyCPUOperation(LIGHT_CPU_OP_ITERATIONS);
            LOG.info("I am {}, running myLightCpuBoundRunnable task LC-{} and I have FINISHED", Thread.currentThread(), taskId);
        };
    }

    private Runnable myIoBoundRunnable(int taskId) {
        return () -> {
            LOG.info("I am {}, running myIoBoundRunnable task IO-{} and I have STARTED", Thread.currentThread(), taskId);
            for (int j = 0; j < 8; j++) {
                try {
                    costlyCPUOperation(MEDIUM_CPU_OP_ITERATIONS);

                    LOG.info("I am {}, running myIoBoundRunnable task IO-{} and I am going to sleep for 2 seconds", Thread.currentThread(), taskId);
                    TimeUnit.MILLISECONDS.sleep(2000); // Simulates a slow IO operation
                    LOG.info("I am {}, running myIoBoundRunnable task IO-{} and I am going to run CPU operations again", Thread.currentThread(), taskId);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            LOG.info("I am {}, running myIoBoundRunnable task IO-{} and I have FINISHED", Thread.currentThread(), taskId);

        };
    }

    private Runnable myMixedBoundRunnable() { //Same number of CPU intensive operations but with short IO simulations in the middle of the CPU calculations
        return () -> {
            try {
                costlyCPUOperation(HEAVY_CPU_OP_ITERATIONS_SLICED_IN_FOUR);
                LOG.info("I am {}, running myMixedBoundRunnable task and I am going to sleep", Thread.currentThread());
                TimeUnit.MILLISECONDS.sleep(100); // Simulates a very fast IO operation
                LOG.info("I am {}, running myMixedBoundRunnable task and I am going to run CPU operations again", Thread.currentThread());
                costlyCPUOperation(HEAVY_CPU_OP_ITERATIONS_SLICED_IN_FOUR);
                LOG.info("I am {}, running myMixedBoundRunnable task and I am going to sleep", Thread.currentThread());
                TimeUnit.MILLISECONDS.sleep(100); // Simulates a very fast IO operation
                LOG.info("I am {}, running myMixedBoundRunnable task and I am going to run CPU operations again", Thread.currentThread());
                costlyCPUOperation(HEAVY_CPU_OP_ITERATIONS_SLICED_IN_FOUR);
                LOG.info("I am {}, running myMixedBoundRunnable task and I am going to sleep", Thread.currentThread().getName());
                TimeUnit.MILLISECONDS.sleep(20); // Simulates a very fast IO operation
                LOG.info("I am {}, running myMixedBoundRunnable task and I am going to run CPU operations again", Thread.currentThread());
                costlyCPUOperation(HEAVY_CPU_OP_ITERATIONS_SLICED_IN_FOUR);

                LOG.info("I am {}, running myMixedBoundRunnable task and I have finished", Thread.currentThread());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        };
    }

    private void costlyCPUOperation(final int iterations) {
        for (int i = 0; i < iterations; i++) {
            double d = tan(atan(tan(atan(tan(atan(tan(atan(tan(atan(tan(atan(123456789.123456789))))))))))));
            cbrt(d);
        }
    }

    private ThreadFactory namingVTThreadFactory(final String name) {
        final var myVTFactory = Thread.ofVirtual().factory();
        try {
            final var nameField = myVTFactory.getClass().getSuperclass().getDeclaredField("name");
            nameField.setAccessible(true);
            nameField.set(myVTFactory, name);
            nameField.setAccessible(false);
            return myVTFactory;
        } catch (IllegalAccessException | NoSuchFieldException e) {
            return null;
        }

    }


}

