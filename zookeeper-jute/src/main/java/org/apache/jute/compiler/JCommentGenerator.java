/*
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

package org.apache.jute.compiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.jute.compiler.generated.Token;

public class JCommentGenerator {

    /**
     * {fieldCommentsBeginLineNumber : {fieldCommentsBeginColumn : fieldCommentsToken}}.
     */
    private final TreeMap<Integer, TreeMap<Integer, Token>> fieldCommentsTokenMap = new TreeMap<>();

    /**
     * {recordCommentsBeginLineNumber : {recordCommentsBeginColumn : recordCommentsToken}}.
     */
    private final TreeMap<Integer, TreeMap<Integer, Token>> recordCommentsTokenMap = new TreeMap<>();

    private final TreeSet<RangeInfo> jFieldTreeSet = new TreeSet<>();

    private final TreeSet<RangeInfo> jRecordTreeSet = new TreeSet<>();

    public void addJRecord(JRecord jRecord) {
        jRecordTreeSet.add(jRecord);
    }

    public void addJField(JField jField) {
        jFieldTreeSet.add(jField);
    }

    public void putFieldSpecialToken(Token fieldToken) {
        putSpecialToken(fieldToken, fieldCommentsTokenMap);
    }

    public void putRecordSpecialToken(Token recordToken) {
        putSpecialToken(recordToken, recordCommentsTokenMap);
    }

    private static void putSpecialToken(Token classToken, TreeMap<Integer, TreeMap<Integer, Token>> commentsTokenMap) {
        if (classToken == null || classToken.specialToken == null || commentsTokenMap == null) {
            return;
        }

        Token tmp = classToken;
        while ((tmp = tmp.specialToken) != null && tmp.image != null) {
            Token finalTmp = tmp;
            commentsTokenMap.compute(tmp.beginLine, (key, value) -> {
                if (value == null) {
                    value = new TreeMap<>();
                }
                value.putIfAbsent(finalTmp.beginColumn, finalTmp);
                return value;
            });
        }
    }

    public List<String> getRecordCommentsList(JRecord jRecord) {
        return getCommentsList(jRecord, jRecordTreeSet, recordCommentsTokenMap);
    }

    public List<String> getFieldCommentsList(JField jField) {
        return getCommentsList(jField, jFieldTreeSet, fieldCommentsTokenMap);
    }

    private static List<String> getCommentsList(RangeInfo rangeInfo,
                                        TreeSet<RangeInfo> rangeInfoSet,
                                        NavigableMap<Integer, TreeMap<Integer, Token>> commentsTokenMap) {
        if (rangeInfo == null || rangeInfoSet == null || commentsTokenMap == null || commentsTokenMap.isEmpty()) {
            return Collections.emptyList();
        }

        RangeInfo prevRangeInfo = rangeInfoSet.lower(rangeInfo);
        RangeInfo nextRangeInfo = rangeInfoSet.higher(rangeInfo);

        int beginLine = getBeginLine(rangeInfo, prevRangeInfo);
        int beginColumn = getBeginColumn(rangeInfo, nextRangeInfo);
        int endLine = rangeInfo.getEndLine();
        int endColumn = getEndColumn(rangeInfo, nextRangeInfo);

        NavigableMap<Integer, TreeMap<Integer, Token>> lineRangeMap = commentsTokenMap.subMap(beginLine, true, endLine, true);
        if (lineRangeMap == null || lineRangeMap.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<String> commentList = new ArrayList<>();
        Iterator<Map.Entry<Integer, TreeMap<Integer, Token>>> lineIterator = lineRangeMap.entrySet().iterator();
        while (lineIterator.hasNext()) {
            Map.Entry<Integer, TreeMap<Integer, Token>> lineEntry = lineIterator.next();
            if (lineEntry.getValue() == null || lineEntry.getValue().isEmpty()) {
                lineIterator.remove();
                continue;
            }

            NavigableMap<Integer, Token> columnMap;
            if (lineEntry.getKey() == beginLine) {
                columnMap = lineEntry.getValue()
                        .subMap(beginColumn, true, beginLine == endLine ? endColumn : Integer.MAX_VALUE, true);
            } else if (lineEntry.getKey() == endLine) {
                columnMap = lineEntry.getValue().subMap(0, true, endColumn, true);
            } else {
                columnMap = lineEntry.getValue();
            }

            if (columnMap == null || columnMap.isEmpty()) {
                continue;
            }

            Iterator<Map.Entry<Integer, Token>> columnIterator = columnMap.entrySet().iterator();
            while (columnIterator.hasNext()) {
                Map.Entry<Integer, Token> entry = columnIterator.next();
                columnIterator.remove();

                Optional.of(entry)
                        .map(Map.Entry::getValue)
                        .map(token -> token.image)
                        .filter(image -> !image.isEmpty())
                        .ifPresent(commentList::add);
            }

            if (lineEntry.getValue().isEmpty()) {
                lineIterator.remove();
            }
        }

        return commentList;
    }

    private static int getBeginLine(RangeInfo thisRangeInfo, RangeInfo prevRangeInfo) {
        if (thisRangeInfo == null || prevRangeInfo == null) {
            return 0;
        }

        if (prevRangeInfo.getEndLine() == thisRangeInfo.getBeginLine()) {
            return prevRangeInfo.getEndLine();
        }

        return prevRangeInfo.getEndLine() + 1;
    }

    private static int getBeginColumn(RangeInfo thisRangeInfo, RangeInfo prevRangeInfo) {
        if (thisRangeInfo == null || prevRangeInfo == null) {
            return 0;
        }

        if (prevRangeInfo.getEndLine() == thisRangeInfo.getBeginLine()) {
            return thisRangeInfo.getBeginColumn();
        }

        return 0;
    }

    private static int getEndColumn(RangeInfo thisRangeInfo, RangeInfo nextRangeInfo) {
        if (thisRangeInfo == null || nextRangeInfo == null) {
            return Integer.MAX_VALUE;
        }

        if (thisRangeInfo.getEndLine() == nextRangeInfo.getBeginLine()) {
            return nextRangeInfo.getBeginColumn() - 1;
        }

        return Integer.MAX_VALUE;
    }

}
