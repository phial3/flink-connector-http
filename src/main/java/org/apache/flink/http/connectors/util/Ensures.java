package org.apache.flink.http.connectors.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.function.Supplier;

public enum Ensures {
    ;

    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, Supplier<Object> errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage.get().toString());
        }
    }

    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    public static void checkState(boolean expression, Supplier<Object> errorMessage) {
        if (!expression) {
            throw new IllegalStateException(errorMessage.get().toString());
        }
    }

    public static <T extends Object> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    public static <T extends Object> T checkNotNull(T reference, Supplier<Object> errorMessage) {
        if (reference == null) {
            throw new NullPointerException(errorMessage.get().toString());
        }
        return reference;
    }

    /**
     * Assert that the supplied array is neither {@code null} nor <em>empty</em>.
     *
     * <p><strong>WARNING</strong>: this method does NOT check if the supplied
     * array contains any {@code null} elements.
     *
     * @param array the array to check
     * @return the supplied array as a convenience
     * @throws IllegalStateException if the supplied array is
     *                               {@code null} or <em>empty</em>
     * @see #condition(boolean)
     */
    public static <T> T[] notEmpty(T[] array) {
        condition(array != null && array.length > 0);
        return array;
    }

    /**
     * Assert that the supplied array is neither {@code null} nor <em>empty</em>.
     *
     * <p><strong>WARNING</strong>: this method does NOT check if the supplied
     * array contains any {@code null} elements.
     *
     * @param array        the array to check
     * @param errorMessage precondition violation message supplier
     * @return the supplied array as a convenience
     * @throws IllegalStateException if the supplied array is
     *                               {@code null} or <em>empty</em>
     * @see #condition(boolean, Supplier)
     */
    public static <T> T[] notEmpty(T[] array, Supplier<Object> errorMessage) {
        condition(array != null && array.length > 0, errorMessage);
        return array;
    }

    /**
     * Assert that the supplied {@link Collection} is neither {@code null} nor empty.
     *
     * <p><strong>WARNING</strong>: this method does NOT check if the supplied
     * collection contains any {@code null} elements.
     *
     * @param collection the collection to check
     * @return the supplied collection as a convenience
     * @throws IllegalStateException if the supplied collection is {@code null} or empty
     * @see #condition(boolean)
     */
    public static <T extends Collection<?>> T notEmpty(T collection) {
        condition(collection != null && !collection.isEmpty());
        return collection;
    }

    /**
     * Assert that the supplied {@link Collection} is neither {@code null} nor empty.
     *
     * <p><strong>WARNING</strong>: this method does NOT check if the supplied
     * collection contains any {@code null} elements.
     *
     * @param collection   the collection to check
     * @param errorMessage precondition violation message supplier
     * @return the supplied collection as a convenience
     * @throws IllegalStateException if the supplied collection is {@code null} or empty
     * @see #condition(boolean, Supplier)
     */
    public static <T extends Collection<?>> T notEmpty(T collection, Supplier<Object> errorMessage) {
        condition(collection != null && !collection.isEmpty(), errorMessage);
        return collection;
    }

    /**
     * Assert that the supplied {@link String} is not blank.
     *
     * <p>A {@code String} is <em>blank</em> if it is {@code null} or consists
     * only of whitespace characters.
     *
     * @param str the string to check
     * @return the supplied string as a convenience
     * @throws IllegalStateException if the supplied string is blank
     * @see #notBlank(String, Supplier)
     */
    public static String notBlank(String str) {
        condition(StringUtils.isNotBlank(str));
        return str;
    }

    /**
     * Assert that the supplied {@link String} is not blank.
     *
     * <p>A {@code String} is <em>blank</em> if it is {@code null} or consists
     * only of whitespace characters.
     *
     * @param str          the string to check
     * @param errorMessage precondition violation message supplier
     * @return the supplied string as a convenience
     * @throws IllegalStateException if the supplied string is blank
     * @see StringUtils#isNotBlank(CharSequence)
     * @see #condition(boolean, Supplier)
     */
    public static String notBlank(String str, Supplier<Object> errorMessage) {
        condition(StringUtils.isNotBlank(str), errorMessage);
        return str;
    }

    /**
     * Assert that the supplied {@code predicate} is {@code true}.
     *
     * @param predicate the predicate to check
     * @throws IllegalStateException if the predicate is {@code false}
     * @see #condition(boolean, Supplier)
     */
    public static void condition(boolean predicate) {
        if (!predicate) {
            throw new IllegalStateException();
        }
    }

    /**
     * Assert that the supplied {@code predicate} is {@code true}.
     *
     * @param predicate    the predicate to check
     * @param errorMessage precondition violation message supplier
     * @throws IllegalStateException if the predicate is {@code false}
     */
    public static void condition(boolean predicate, Supplier<Object> errorMessage) {
        if (!predicate) {
            throw new IllegalStateException(errorMessage.get().toString());
        }
    }
}
