package com.test.cepexample;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;

public class ChangesTracker {

	private Map<Long, Observable<Change>> usersChache = new HashMap<>();
	private Map<Long, List<Subscriber<Change>>> userSubscribers = new HashMap<>();
	private Map<Long, Map<Long, Subscription>> userSubscriptions = new HashMap<>();

	private Map<Long, Observable<Change>> rolesCache = new HashMap<>();
	private Map<Long, Map<Class<?>, Subscription>> roleSubscriptions = new HashMap<>();
	private Map<Long, List<Subscriber<Change>>> roleSubscribers = new HashMap<>();

	private Map<Class<?>, Observable<Change>> entitiesCache = new HashMap<>();
	private Map<Class<?>, List<Subscriber<Change>>> entitySubscribers = new HashMap<>();

	private void initStream() {
		usersChache = new HashMap<>();
		userSubscribers = new HashMap<>();
		userSubscriptions = new HashMap<>();
		rolesCache = new HashMap<>();
		roleSubscriptions = new HashMap<>();
		roleSubscribers = new HashMap<>();
		entitiesCache = new HashMap<>();
		entitySubscribers = new HashMap<>();
		addListenerToRole();
		addListenerToRoleMembership();

		// Delete users cache too, as users need to reconnect after any
		// package installation
		usersChache.clear();
	}

	/**
	 * If already created then returns that otherwise creates a pipe.
	 * 
	 * @param userId
	 * @return
	 */
	public Observable<Change> getStreamForUser(long userId) {
		Observable<Change> observable = usersChache.get(userId);
		if (observable == null) {
			observable = createUserStream(userId);
			usersChache.put(userId, observable);
		}
		return observable;
	}

	/**
	 * Get the pipes of the given user roles and combine them into one pipe and
	 * returns that.
	 * 
	 * @param userId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Observable<Change> createUserStream(long userId) {
		List<Subscriber<Change>> subscribers = getUserSubscribers(userId);
		Observable<Change> create = newObserable(subscriber -> {
			subscribers.add((Subscriber<Change>) subscriber);
		});
		getUserRoles(userId).subscribe(userRoles -> {
			for (Role role : userRoles) {
				subscribeToRole(role, userId);
			}
		});
		return create;
	}

	/**
	 * Creates a subscription to given entity. If any change happened then tells
	 * that to the subscribers of the given role.
	 * 
	 * @param entity
	 * @param user
	 */
	private void subscribeToRole(Role role, long user) {
		Observable<Change> roleStream = getRoleStream(role);
		Subscription subscribe = roleStream.subscribe(c -> {
			List<Subscriber<Change>> subscribers = getUserSubscribers(user);
			subscribers.stream().filter(s -> !s.isUnsubscribed()).forEach(s -> s.onNext(c));
		});
		getUserSubscriptions(user).put(role.getId(), subscribe);
	}

	private Map<Long, Subscription> getUserSubscriptions(long user) {
		Map<Long, Subscription> map = userSubscriptions.get(user);
		if (map == null) {
			map = new HashMap<>();
			userSubscriptions.put(user, map);
		}
		return map;
	}

	private List<Subscriber<Change>> getUserSubscribers(long user) {
		List<Subscriber<Change>> subscribers = userSubscribers.get(user);
		if (subscribers == null) {
			subscribers = new ArrayList<>();
			userSubscribers.put(user, subscribers);
		}
		return subscribers;
	}

	/**
	 * If already created then returns that otherwise creates a pipe.
	 * 
	 * @param role
	 * @return
	 */
	private Observable<Change> getRoleStream(Role role) {
		long id = role.getId();
		Observable<Change> observable = rolesCache.get(id);
		if (observable == null) {
			observable = createRoleStream(role);
			rolesCache.put(id, observable);
		}
		return observable;
	}

	private void addListenerToRoleMembership() {
		Observable<Change> roleStream = getEntityStream(RoleMembership.class);
		roleStream.subscribe(c -> {
			updateUserStream(c);
		});
	}

	/**
	 * Get the subscriptions to roles of user. If new role is added to user then
	 * subscribe to that role also. If removed then unsubscribe to that role.
	 * 
	 * @param c
	 */
	private void updateUserStream(Change c) {
		RoleMembership rm = (RoleMembership) c.getObject();
		long userId = rm.getUser().getId();
		Map<Long, Subscription> subscriptions = userSubscriptions.get(userId);
		if (subscriptions == null) {
			return;
		}
		Role role = rm.getRole();
		switch (c.getType()) {
		case CREATED:
			subscribeToRole(role, userId);
			break;

		case DELETED:
			Subscription subscription = subscriptions.get(role.getId());
			subscription.unsubscribe();
			break;
		default:
			break;
		}
	}

	private void addListenerToRole() {
		Observable<Change> roleStream = getEntityStream(Role.class);
		roleStream.subscribe(c -> {
			if (c.getType() == ChangeType.UPDATED) {
				updateRoleStream((Role) c.getObject());
			}
		});
	}

	/**
	 * Get the subscriptions to entities of role. If new entity is added to role
	 * then subscribe to that entity also. If removed then unsubscribe to that
	 * entity.
	 * 
	 * @param role
	 */
	private void updateRoleStream(Role role) {
		Map<Class<?>, Subscription> subscriptions = roleSubscriptions.get(role);
		// Role was not used till now so no need to update
		if (subscriptions == null) {
			return;
		}
		Set<Class<?>> previousEntities = subscriptions.keySet();
		List<Class<?>> allowedEntities = getAllowedEntities(role);
		for (Class<?> entity : previousEntities) {
			if (!allowedEntities.contains(entity)) {
				Subscription subscription = subscriptions.get(entity);
				subscription.unsubscribe();
			}
		}
		for (Class<?> entity : allowedEntities) {
			if (!previousEntities.contains(entity)) {
				subscribeToEntity(entity, role);
			}
		}
	}

	/**
	 * Get the pipes of the allowed entities of the given role and combines them
	 * into one pipe and returns that.
	 * 
	 * @param role
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Observable<Change> createRoleStream(Role role) {
		List<Subscriber<Change>> subscribers = getRoleSubscribers(role.getId());
		Observable<Change> create = newObserable(subscriber -> {
			subscribers.add((Subscriber<Change>) subscriber);
		});
		List<Class<?>> allowedEntities = getAllowedEntities(role);
		for (Class<?> entity : allowedEntities) {
			subscribeToEntity(entity, role);
		}
		return create;
	}

	/**
	 * Creates a subscription to given entity. If any change happened then tells
	 * that to the subscribers of the given role.
	 * 
	 * @param entity
	 * @param role
	 */
	private void subscribeToEntity(Class<?> entity, Role role) {
		long roleId = role.getId();
		Observable<Change> entityStream = getEntityStream(entity);
		Subscription subscribe = entityStream.subscribe(c -> {
			List<Subscriber<Change>> subscribers = getRoleSubscribers(roleId);
			subscribers.stream().filter(s -> !s.isUnsubscribed()).forEach(s -> s.onNext(c));
		});
		getRoleSubscriptions(roleId).put(entity, subscribe);
	}

	private Map<Class<?>, Subscription> getRoleSubscriptions(long role) {
		Map<Class<?>, Subscription> map = roleSubscriptions.get(role);
		if (map == null) {
			map = new HashMap<>();
			roleSubscriptions.put(role, map);
		}
		return map;
	}

	private List<Subscriber<Change>> getRoleSubscribers(long role) {
		List<Subscriber<Change>> subscribers = roleSubscribers.get(role);
		if (subscribers == null) {
			subscribers = new ArrayList<>();
			roleSubscribers.put(role, subscribers);
		}
		return subscribers;
	}

	/**
	 * If already created then returns that otherwise creates a pipe.
	 * 
	 * @param entity
	 * @return
	 */
	private Observable<Change> getEntityStream(Class<?> entity) {
		Observable<Change> observable = entitiesCache.get(entity);
		if (observable == null) {
			observable = createEntityStream(entity);
			entitiesCache.put(entity, observable);
		}
		return observable;
	}

	/**
	 * Creates a pipe and returns that. If anyone subscribed to that pipe then
	 * maintains them in map. If any transaction is committed then sends those
	 * changes to subscribers of that entity.
	 * 
	 * @param entity
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Observable<Change> createEntityStream(Class<?> entity) {
		Observable<Change> o = newObserable(subscriber -> {
			if (subscriber.isUnsubscribed()) {
				return;
			}
			addSubscriber(entity, (Subscriber<Change>) subscriber);
		});
		return o;
	}

	private <T> Observable<T> newObserable(OnSubscribe<T> f) {
		Observable<T> o = Observable.create(f);
		o.lift(new WeakSubscriptionOperator<T>());
		return o;
	}

	private void addSubscriber(Class<?> entity, Subscriber<Change> subscriber) {
		List<Subscriber<Change>> list = entitySubscribers.get(entity);
		if (list == null) {
			list = new ArrayList<>();
			entitySubscribers.put(entity, list);
		}
		list.add(subscriber);
	}

	private List<Class<?>> getAllowedEntities(Role role) {
		Collection<Permission> allPermission = ((DefaultAuthorizationProvider) authProvider)
				.resolvePermissionsInRole(role);
		List<Class<?>> entities = entitiesCache.keySet().stream().map(e -> EntityPermission.read(e))
				.filter(ep -> allPermission.stream().anyMatch(p -> p.implies(ep))).map(ep -> ep.getEntity())
				.collect(Collectors.toList());
		return entities;
	}

	private Observable<List<Role>> getUserRoles(long userId) {
		return queryRepo.getRoleMembershipsByUser(userId).map(roleMemberships -> {
			return roleMemberships.stream().map(rm -> rm.getRole()).collect(Collectors.toList());
		});
	}

	public void fireChanges(List<Change> changes) {
		// Do not send server side objects
		// filtering to get non server side objects
		List<DatabaseObject> addChanges = changes.getAdded().stream()
				.filter(e -> !entityRegistry.getEntity(e.getClass()).isServerBundle()).collect(Collectors.toList());

		List<DatabaseObject> updatedChanges = changes.getUpdated().stream()
				.filter(e -> !entityRegistry.getEntity(e.getClass()).isServerBundle()).collect(Collectors.toList());

		List<DatabaseObject> deletedChanges = changes.getDeleted().stream()
				.filter(e -> !entityRegistry.getEntity(e.getClass()).isServerBundle()).collect(Collectors.toList());

		for (DatabaseObject obj : addChanges) {
			// Skipping child objects
			if (obj.getParent() != null) {
				continue;
			}
			Change change = new Change(obj, ChangeType.CREATED, getCurrentUser());
			fire(obj.getClass(), change);
		}
		for (DatabaseObject obj : updatedChanges) {
			Change change = new Change(obj, ChangeType.UPDATED, getCurrentUser());
			fire(obj.getClass(), change);
		}
		for (DatabaseObject obj : deletedChanges) {
			Change change = new Change(obj, ChangeType.DELETED, getCurrentUser());
			fire(obj.getClass(), change);
		}
	}

	private long getCurrentUser() {
		Object principal = SecurityUtils.getSubject().getPrincipal();
		if (principal instanceof Long) {
			return (long) principal;
		}
		// TODO might be master user
		return 0l;
	}

	private void fire(Class<?> clazz, Change change) {
		clazz = LazyInitializer.getActualClass(clazz);
		List<Subscriber<Change>> list = entitySubscribers.get(clazz);
		if (list == null) {
			return;
		}
		// Just to avoid ConcurrentModificationException
		for (Subscriber<Change> subscriber : new ArrayList<>(list)) {
			if (subscriber.isUnsubscribed()) {
				continue;
			}
			subscriber.onNext(change);
		}
	}
}