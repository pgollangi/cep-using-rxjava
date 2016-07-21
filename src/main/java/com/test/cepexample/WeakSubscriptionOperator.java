package com.test.cepexample;

import rx.Observable;
import rx.Subscriber;

public class WeakSubscriptionOperator<T> implements Observable.Operator<T, T> {
	@Override
	public Subscriber<? super T> call(final Subscriber<? super T> child) {
		return new WeakSubscriptionSubscriber<T>(child);
	}
}