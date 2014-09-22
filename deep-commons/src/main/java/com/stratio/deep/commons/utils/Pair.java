/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.commons.utils;

import com.google.common.base.Objects;

import java.io.Serializable;


/**
 * Common utility class wrapping a pair of objects.
 * Instances of Pair are immutable.
 */
public class Pair<L, R> implements Serializable {
    /**
     * Left element of the pair.
     */
    public final L left;

    /**
     * Right element of the pair.
     */
    public final R right;

    /**
     * private constructor.
     *
     * @param left  left element
     * @param right right element
     */
    private Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Creates a new immutable pair of objects.
     *
     * @param left  the left element.
     * @param right the right element.
     * @param <L>   the type of the left element.
     * @param <R>   the type of the right element.
     * @return a new pair.
     */
    public static <L, R> Pair<L, R> create(L left, R right) {
        return new Pair<>(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int hashCode() {
        int hashCode = 31 + (left == null ? 0 : left.hashCode());
        return 31 * hashCode + (right == null ? 0 : right.hashCode());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof Pair)) {
            return false;
        }
        Pair that = (Pair) o;
        // handles nulls properly
        return Objects.equal(left, that.left) && Objects.equal(right, that.right);
    }
}
