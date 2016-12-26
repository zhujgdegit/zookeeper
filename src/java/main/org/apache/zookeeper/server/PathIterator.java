/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.util.Iterator;

/**
 * Iterates over a ZooKeeper path. Each iteration goes up one parent path. Thus, the
 * effect of the iterator is to iterate over the initial path and then all of its parents.
 */
public class PathIterator implements Iterator<String> {
    private String path;
    private int level = -1;

    /**
     * @param path initial path
     */
    public PathIterator(String path) {
        // NOTE: asserts that the path has already been validated
        this.path = path;
    }

    /**
     * Return an Iterable view so that this Iterator can be used in for each
     * statements. IMPORTANT: the returned Iterable is single use only
     * @return Iterable
     */
    public Iterable<String> asIterable() {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return PathIterator.this;
            }
        };
    }

    @Override
    public boolean hasNext() {
        return !path.isEmpty();
    }

    /**
     * Returns true if this iterator is currently at a parent path as opposed
     * to the initial path given to the constructor
     *
     * @return true/false
     */
    public boolean atParentPath()
    {
        return level > 0;
    }

    @Override
    public String next() {
        String localPath = path;
        if (hasNext()) {
            ++level;
            if (path.equals("/")) {
                path = "";
            } else {
                path = path.substring(0, path.lastIndexOf('/'));
                if ( path.length() == 0 ) {
                    path = "/";
                }
            }
        }
        return localPath;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
