package com.netflix.eureka2.transport.base;

import com.netflix.eureka2.transport.MessageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

import static rx.Scheduler.Worker;

/**
 * A decorator for MessageConnection that self closes after a specified period of time
 * @author David Liu
 */
public class SelfClosingConnection implements MessageConnection {

    private static final Logger logger = LoggerFactory.getLogger(SelfClosingConnection.class);

    private static final SelfClosingException CLOSING_EXCEPTION = new SelfClosingException("Connection self-closing");

    private final Action0 selfTerminateTask = new Action0() {
        @Override
        public void call() {
            logger.info("Shutting down the connection after {} seconds", lifecycleDurationMs);
            SelfClosingConnection.this.shutdown(CLOSING_EXCEPTION);
        }
    };

    private final MessageConnection delegate;
    private final Worker terminationWorker;
    private final long lifecycleDurationMs;

    public SelfClosingConnection(MessageConnection delegate, long lifecycleDurationMs) {
        this(delegate, lifecycleDurationMs, Schedulers.computation());
    }

    public SelfClosingConnection(MessageConnection delegate, long lifecycleDurationMs, Scheduler terminationScheduler) {
        this.delegate = delegate;
        this.lifecycleDurationMs = lifecycleDurationMs;

        terminationWorker = terminationScheduler.createWorker();
        if (lifecycleDurationMs > 0) {
            terminationWorker.schedule(selfTerminateTask, lifecycleDurationMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Observable<Void> submit(Object message) {
        return delegate.submit(message);
    }

    @Override
    public Observable<Void> submitWithAck(Object message) {
        return delegate.submitWithAck(message);
    }

    @Override
    public Observable<Void> submitWithAck(Object message, long timeout) {
        return delegate.submitWithAck(message, timeout);
    }

    @Override
    public Observable<Void> acknowledge() {
        return delegate.acknowledge();
    }

    @Override
    public Observable<Object> incoming() {
        return delegate.incoming();
    }

    @Override
    public Observable<Void> onError(Throwable error) {
        return delegate.onError(error);
    }

    @Override
    public Observable<Void> onCompleted() {
        return delegate.onCompleted();
    }

    @Override
    public void shutdown() {
        terminationWorker.unsubscribe();
        delegate.shutdown();
    }

    @Override
    public void shutdown(Throwable e) {
        terminationWorker.unsubscribe();
        delegate.shutdown(e);
    }

    @Override
    public Observable<Void> lifecycleObservable() {
        return delegate.lifecycleObservable();
    }


    public static class SelfClosingException extends Exception {
        public SelfClosingException(String msg) {
            super(msg);
        }
    }
}