package com.netflix.suro.sink.kafka;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.suro.message.MessageContainer;

public class MessageContainerWrapper {

	private final AtomicReference<Throwable> cause;
	private final AtomicInteger retryCount;
	private final SoftReference<MessageContainer> messageContainer;
	private final String routingKey;

	public MessageContainerWrapper(MessageContainer container, Throwable th) {
		this(container, th, 0);
	}

	public MessageContainerWrapper(MessageContainer container, Throwable cause, int retrycount) {
		this.messageContainer = new SoftReference<MessageContainer>(container);
		this.retryCount = new AtomicInteger(retrycount);
		this.cause = new AtomicReference<Throwable>(cause);
		this.routingKey = container.getRoutingKey();
	}

	public int getRetryCount() {
		return retryCount.get();
	}

	public MessageContainer getMessageContainer() {
		return messageContainer.get();
	}

	public int incrementAndGetRetryCount(Throwable cause) {
		this.cause.set(cause);
		return retryCount.incrementAndGet();
	}

	public Throwable getThrowable() {
		return this.cause.get();
	}

	public String getRoutingKey() {
		return routingKey;
	}

}
