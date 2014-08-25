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

package com.stratio.deep.rdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Wrapper class holding information of a computed token range.
 */
public class DeepTokenRange implements Comparable<DeepTokenRange>, Serializable {
    private Comparable startToken;
    private Comparable endToken;
    private List<String> replicas;

    /**
     * Construct a new token range with no replica information.
     *
     * @param startToken first token of this range.
     * @param endToken   last token of this range.
     */
    public DeepTokenRange(Comparable startToken, Comparable endToken) {
        this.startToken = startToken;
        this.endToken = endToken;
    }

    /**
     * Construct a new token range with replica information.
     *
     * @param startToken first token of this range.
     * @param endToken   last token of this range.
     * @param replicas   the list of replica machines holding this range of tokens.
     */
    public DeepTokenRange(Comparable startToken, Comparable endToken, List<String> replicas) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.replicas = replicas;
    }

    /**
     * Construct a new token range with replica information
     * @param replicas
     */
    public DeepTokenRange(String [] replicas) {
        this.replicas = Arrays.asList(replicas);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DeepTokenRange{" +
                "startToken=" + startToken +
                ", endToken=" + endToken +
                ", replicas=" + replicas +
                "}\n";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DeepTokenRange that = (DeepTokenRange) o;

        if (!endToken.equals(that.endToken)) {
            return false;
        }
        if (!startToken.equals(that.startToken)) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = startToken.hashCode();
        result = 31 * result + endToken.hashCode();
        return result;
    }

    public Comparable getStartToken() {
        return startToken;
    }

    public void setStartToken(Comparable startToken) {
        this.startToken = startToken;
    }

    public Comparable getEndToken() {
        return endToken;
    }

    public void setEndToken(Comparable endToken) {
        this.endToken = endToken;
    }

    public List<String> getReplicas() {
        return replicas;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(DeepTokenRange o) {
        return startToken.compareTo(o.startToken);
    }
}
