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

public class PathIterator {
    private String path;
    private int level = -1;

    public PathIterator(String path) {
        // NOTE: asserts that the path has already been validated
        this.path = path;
    }

    public boolean hasNext() {
        return !path.isEmpty();
    }

    public boolean atParentPath()
    {
        return level > 0;
    }

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
}
