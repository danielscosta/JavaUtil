package model;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daniel.costa
 *
 */
public class CallbackConsumer  implements Runnable {

    private final Runnable task;

    private static volatile AtomicInteger globalCounter = new AtomicInteger(0);

    public CallbackConsumer(Runnable task) {
        this.task = task;
        while(globalCounter.get() > 5000) {
            synchronized (globalCounter) {
                try {
                    globalCounter.wait(500);
                } catch (InterruptedException e) {}
            }
        }
        globalCounter.incrementAndGet();
    }

    @Override
    public void run() {
        task.run();
        globalCounter.decrementAndGet();
        synchronized (globalCounter) { globalCounter.notifyAll(); }
    }

}
