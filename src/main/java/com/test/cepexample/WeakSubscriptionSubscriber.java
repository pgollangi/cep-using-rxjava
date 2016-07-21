package com.test.cepexample;

import java.lang.ref.WeakReference;

import rx.Subscriber;

public class WeakSubscriptionSubscriber<T> extends Subscriber<T> {

	private final WeakReference<Subscriber<? super T>> mWeakReference;

	public WeakSubscriptionSubscriber(final Subscriber<? super T> child) {
		mWeakReference = new WeakReference<Subscriber<? super T>>(child);
		child.add(this);
	}

	public void onCompleted() {
		final Subscriber<? super T> subscriber = mWeakReference.get();
		if (!isUnsubscribed() && subscriber != null) {
			subscriber.onCompleted();
		}
	}

	public void onError(Throwable throwable) {
		final Subscriber<? super T> subscriber = mWeakReference.get();
		if (!isUnsubscribed() && subscriber != null) {
			subscriber.onError(throwable);
		}
	}

	public void onNext(T t) {
		final Subscriber<? super T> subscriber = mWeakReference.get();
		if (!isUnsubscribed() && subscriber != null) {
			subscriber.onNext(t);
		}
	}
}