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

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import org.junit.jupiter.api.Test;

public class JRecordTest {

    @Test
    public void testGetComments() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        JRecord jRecord = new JRecord("test.test.name", null, null);
        StringJoiner joiner = new StringJoiner("&&");
        List<String> comments = Arrays.asList(
                "// hello this is comment1",
                "/** hello this is comment2 */",
                "/** hello this is comment3 **/",
                "/* hello this is comment4 */",
                "/* hello this is comment5 **/",
                "*hello this is comment6",
                "**hello this is comment7");
        String resultStr = (String) invokeMethod(jRecord, "getComments",
                new Class<?>[]{StringJoiner.class, List.class},
                new Object[]{joiner, comments});
        List<String> result = Arrays.asList(resultStr.split("&&"));

        assertEquals(7, result.size());
        assertEquals("hello this is comment1", result.get(0));
        assertEquals("hello this is comment2", result.get(1));
        assertEquals("hello this is comment3", result.get(2));
        assertEquals("hello this is comment4", result.get(3));
        assertEquals("hello this is comment5", result.get(4));
        assertEquals("hello this is comment6", result.get(5));
        assertEquals("hello this is comment7", result.get(6));
    }

    @Test
    public void testGetCommentsHasNewline() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        JRecord jRecord = new JRecord("test.test.name", null, null);
        StringJoiner joiner = new StringJoiner("&&");
        List<String> comments = Arrays.asList(
                "// hello this is comment1 \n // hello this is comment2",
                "/** hello this is comment3 */ \n /** hello this is comment4 */",
                "/* hello this is comment5 */ \n /* hello this is comment6 */",
                "/***** hello this is comment7 */ \n /***** hello this is comment8 */",
                "*hello this is comment9\n*hello this is comment10",
                "**** ** hello this is comment11\n**** ** hello this is comment12");
        String resultStr = (String) invokeMethod(jRecord, "getComments",
                new Class<?>[]{StringJoiner.class, List.class},
                new Object[]{joiner, comments});
        List<String> result = Arrays.asList(resultStr.split("&&"));

        assertEquals(12, result.size());
        assertEquals("hello this is comment1", result.get(0));
        assertEquals("hello this is comment2", result.get(1));
        assertEquals("hello this is comment3", result.get(2));
        assertEquals("hello this is comment4", result.get(3));
        assertEquals("hello this is comment5", result.get(4));
        assertEquals("hello this is comment6", result.get(5));
        assertEquals("hello this is comment7", result.get(6));
        assertEquals("hello this is comment8", result.get(7));
        assertEquals("hello this is comment9", result.get(8));
        assertEquals("hello this is comment10", result.get(9));
        assertEquals("hello this is comment11", result.get(10));
        assertEquals("hello this is comment12", result.get(11));
    }

    @Test
    public void testGetCommentsBadFormat() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        JRecord jRecord = new JRecord("test.test.name", null, null);
        StringJoiner joiner = new StringJoiner("&&");
        List<String> comments = Arrays.asList(
                "// hello this is comment1",
                "/** /**hello this /**is comment2 */",
                "/** hello this is */comment3 **/",
                "** *** *****hello this is comment4",
                "/**/**/**hello this is comment5*/*/**/**/*/",
                "   hello */*/this is */comment6   ",
                "// hello //////this //is// //comment7"
        );
        String resultStr = (String) invokeMethod(jRecord, "getComments",
                new Class<?>[]{StringJoiner.class, List.class},
                new Object[]{joiner, comments});
        List<String> result = Arrays.asList(resultStr.split("&&"));

        assertEquals(7, result.size());
        assertEquals("hello this is comment1", result.get(0));
        assertEquals("hello this is comment2", result.get(1));
        assertEquals("hello this is comment3", result.get(2));
        assertEquals("hello this is comment4", result.get(3));
        assertEquals("hello this is comment5", result.get(4));
        assertEquals("hello this is comment6", result.get(5));
        assertEquals("hello this is comment7", result.get(6));
    }

    private Object invokeMethod(final Object target,
                                final String methodName,
                                final Class<?>[] classes,
                                final Object[] args
                           ) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> targetClazz = target.getClass();
        Method method = targetClazz.getDeclaredMethod(methodName, classes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
