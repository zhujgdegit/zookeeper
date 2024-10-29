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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.jute.compiler.generated.Token;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class JCommentGeneratorTest {

    @Test
    public void testAddJField() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();
        JField field1 = createJField("field1", 10, 15, 0, 20);
        JField field2 = createJField("field2", 20, 25, 0, 20);
        JField field3 = createJField("field3", 30, 25, 0, 20);
        jCommentGenerator.addJField(field1);
        jCommentGenerator.addJField(field2);

        TreeSet<RangeInfo> jFieldTreeSet = getField(jCommentGenerator, "jFieldTreeSet", TreeSet.class);
        assertEquals(2, jFieldTreeSet.size());
        assertTrue(jFieldTreeSet.contains(field1));
        assertTrue(jFieldTreeSet.contains(field2));
        assertFalse(jFieldTreeSet.contains(field3));
    }

    @Test
    public void testAddJFieldSort() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        JField field1 = createJField("field1", 10, 15, 0, 20);
        JField field2 = createJField("field2", 20, 25, 0, 20);
        JField field3 = createJField("field3", 30, 25, 0, 20);
        JField field4 = createJField("field4", 40, 45, 0, 20);

        jCommentGenerator.addJField(field1);
        jCommentGenerator.addJField(field2);
        jCommentGenerator.addJField(field3);
        jCommentGenerator.addJField(field4);

        TreeSet<RangeInfo> jFieldTreeSet = getField(jCommentGenerator, "jFieldTreeSet", TreeSet.class);

        assertNull(jFieldTreeSet.lower(field1));
        assertEquals(field2, jFieldTreeSet.higher(field1));
        assertEquals(field1, jFieldTreeSet.lower(field2));
        assertEquals(field3, jFieldTreeSet.higher(field2));
        assertEquals(field2, jFieldTreeSet.lower(field3));
        assertEquals(field4, jFieldTreeSet.higher(field3));
        assertEquals(field3, jFieldTreeSet.lower(field4));
        assertNull(jFieldTreeSet.higher(field4));
    }

    @Test
    public void testAddJFieldBeginLineEqlSort() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        JField field1 = createJField("field1", 10, 10, 0, 10);
        JField field2 = createJField("field2", 10, 10, 15, 20);
        JField field3 = createJField("field3", 10, 10, 25, 30);
        JField field4 = createJField("field4", 10, 10, 35, 40);


        jCommentGenerator.addJField(field1);
        jCommentGenerator.addJField(field2);
        jCommentGenerator.addJField(field3);
        jCommentGenerator.addJField(field4);

        TreeSet<RangeInfo> jFieldTreeSet = getField(jCommentGenerator, "jFieldTreeSet", TreeSet.class);

        assertNull(jFieldTreeSet.lower(field1));
        assertEquals(field2, jFieldTreeSet.higher(field1));
        assertEquals(field1, jFieldTreeSet.lower(field2));
        assertEquals(field3, jFieldTreeSet.higher(field2));
        assertEquals(field2, jFieldTreeSet.lower(field3));
        assertEquals(field4, jFieldTreeSet.higher(field3));
        assertEquals(field3, jFieldTreeSet.lower(field4));
        assertNull(jFieldTreeSet.higher(field4));
    }

    @Test
    public void testPutFieldSpecialToken() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        Token token1 = createToken("token1Image", 40, 45, 0, 10);
        Token token2 = createToken("token2Image", 30, 35, 0, 10);
        Token token3 = createToken("token3Image", 20, 25, 0, 10);
        Token token4 = createToken("token4Image", 10, 15, 0, 10);
        Token token5 = createToken("token5Image", 0, 5, 0, 10);


        setField(token1, "specialToken", token2);
        setField(token2, "specialToken", token3);
        setField(token3, "specialToken", token4);
        setField(token4, "specialToken", token5);

        jCommentGenerator.putFieldSpecialToken(token1);
        TreeMap<Integer, TreeMap<Integer, Token>> fieldCommentsTokenMap =
                getField(jCommentGenerator, "fieldCommentsTokenMap", TreeMap.class);

        assertEquals(4, fieldCommentsTokenMap.size());
        for (TreeMap<Integer, Token> value : fieldCommentsTokenMap.values()) {
            assertEquals(1, value.size());
        }
    }

    @Test
    public void testPutFieldSpecialTokenBeginLineEql() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        Token token1 = createToken("token1Image", 10, 10, 0, 10);
        Token token2 = createToken("token2Image", 10, 10, 15, 20);
        Token token3 = createToken("token3Image", 10, 10, 25, 30);
        Token token4 = createToken("token4Image", 10, 10, 35, 40);
        Token token5 = createToken("token5Image", 10, 10, 45, 50);

        setField(token1, "specialToken", token2);
        setField(token2, "specialToken", token3);
        setField(token3, "specialToken", token4);
        setField(token4, "specialToken", token5);

        jCommentGenerator.putFieldSpecialToken(token1);
        TreeMap<Integer, TreeMap<Integer, Token>> fieldCommentsTokenMap =
                getField(jCommentGenerator, "fieldCommentsTokenMap", TreeMap.class);

        assertEquals(1, fieldCommentsTokenMap.size());
        for (TreeMap<Integer, Token> value : fieldCommentsTokenMap.values()) {
            assertEquals(4, value.size());
        }
    }

    @Test
    public void testGetFieldCommentsList() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        Token token1 = createToken("token1Image", 12, 12, 0, 10);
        Token token2 = createToken("token2Image", 13, 13, 0, 10);
        Token token3 = createToken("token3Image", 14, 14, 0, 10);
        Token token4 = createToken("token4Image", 33, 33, 0, 10);
        Token token5 = createToken("token5Image", 34, 34, 0, 10);

        setField(token1, "specialToken", token2);
        setField(token2, "specialToken", token3);
        setField(token3, "specialToken", token4);
        setField(token4, "specialToken", token5);

        jCommentGenerator.putFieldSpecialToken(token1);

        JField field1 = createJField("field1", 15, 20, 0, 20);
        JField field2 = createJField("field2", 35, 40, 0, 20);
        List<String> expectedField1CommentList = Arrays.asList("token2Image", "token3Image");
        List<String> expectedField2CommentList = Arrays.asList("token4Image", "token5Image");

        jCommentGenerator.addJField(field1);
        jCommentGenerator.addJField(field2);
        List<String> field1CommentList = jCommentGenerator.getFieldCommentsList(field1);
        List<String> field2CommentList = jCommentGenerator.getFieldCommentsList(field2);

        assertEquals(2, field1CommentList.size());
        assertEquals(2, field2CommentList.size());
        assertEquals(expectedField1CommentList, field1CommentList);
        assertEquals(expectedField2CommentList, field2CommentList);
    }

    @Test
    public void testGetFieldCommentsListEqlLine() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        Token emptyToken = createToken("emptyToken", 0, 0, 0, 0);

        Token token1 = createToken("token1Image", 10, 10, 2, 3);
        Token token2 = createToken("token2Image", 10, 10, 5, 7);
        Token token3 = createToken("token3Image", 10, 10, 17, 20);
        Token token4 = createToken("token4Image", 10, 10, 35, 37);
        Token token5 = createToken("token5Image", 10, 10, 42, 45);

        setField(emptyToken, "specialToken", token1);
        setField(token1, "specialToken", token2);
        setField(token2, "specialToken", token3);
        setField(token3, "specialToken", token4);
        setField(token4, "specialToken", token5);

        jCommentGenerator.putFieldSpecialToken(emptyToken);

        // column0 - column29 belongs field1
        JField field1 = createJField("field1", 10, 10, 10, 15);

        // column30 - columnEnd belongs field2
        JField field2 = createJField("field2", 10, 10, 30, 40);
        List<String> expectedField1CommentList = Arrays.asList("token1Image", "token2Image", "token3Image");
        List<String> expectedField2CommentList = Arrays.asList("token4Image", "token5Image");

        jCommentGenerator.addJField(field1);
        jCommentGenerator.addJField(field2);
        List<String> field1CommentList = jCommentGenerator.getFieldCommentsList(field1);
        List<String> field2CommentList = jCommentGenerator.getFieldCommentsList(field2);

        assertEquals(3, field1CommentList.size());
        assertEquals(2, field2CommentList.size());
        assertEquals(expectedField1CommentList, field1CommentList);
        assertEquals(expectedField2CommentList, field2CommentList);
    }

    @Test
    public void testGetFieldCommentsListNotEqlLine() throws NoSuchFieldException, IllegalAccessException {
        JCommentGenerator jCommentGenerator = new JCommentGenerator();

        Token emptyToken = createToken("emptyToken", 0, 0, 0, 0);

        Token token1 = createToken("token1Image", 8, 8, 0, 10);
        Token token2 = createToken("token2Image", 9, 9, 5, 12);
        Token token3 = createToken("token3Image", 12, 12, 31, 34);
        Token token4 = createToken("token4Image", 12, 12, 35, 38);
        Token token5 = createToken("token5Image", 12, 12, 55, 70);
        Token token6 = createToken("token6Image", 16, 17, 0, 10);

        setField(emptyToken, "specialToken", token1);
        setField(token1, "specialToken", token2);
        setField(token2, "specialToken", token3);
        setField(token3, "specialToken", token4);
        setField(token4, "specialToken", token5);
        setField(token5, "specialToken", token6);

        jCommentGenerator.putFieldSpecialToken(emptyToken);

        // line0,column0 - line12,column34 belongs field1
        JField field1 = createJField("field1", 10, 12, 0, 30);

        // line12,column35 - line15,columnEnd belongs field2
        JField field2 = createJField("field2", 12, 15, 35, 40);
        List<String> expectedField1CommentList = Arrays.asList("token1Image", "token2Image", "token3Image");
        List<String> expectedField2CommentList = Arrays.asList("token4Image", "token5Image");

        jCommentGenerator.addJField(field1);
        jCommentGenerator.addJField(field2);
        List<String> field1CommentList = jCommentGenerator.getFieldCommentsList(field1);
        List<String> field2CommentList = jCommentGenerator.getFieldCommentsList(field2);

        assertEquals(3, field1CommentList.size());
        assertEquals(2, field2CommentList.size());
        assertEquals(expectedField1CommentList, field1CommentList);
        assertEquals(expectedField2CommentList, field2CommentList);
    }

    private Token createToken(String image,
                              int beginLine,
                              int endLine,
                              int beginColumn,
                              int endColumn) throws NoSuchFieldException, IllegalAccessException {
        Token token = new Token(0, image);
        setField(token, "beginLine", beginLine);
        setField(token, "endLine", endLine);
        setField(token, "beginColumn", beginColumn);
        setField(token, "endColumn", endColumn);
        return token;
    }

    private JField createJField(String name, int beginLine, int endLine, int beginColumn, int endColumn) {
        JField field = new JField(new JByte(), name);
        field.setBeginLine(beginLine);
        field.setEndLine(endLine);
        field.setBeginColumn(beginColumn);
        field.setEndColumn(endColumn);
        return field;
    }

    private void setField(final Object target, final String fieldName, final Object newValue) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = target.getClass();
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, newValue);
    }

    private <T> T getField(final Object target,
                           final String fieldName,
                           final Class<T> fieldClassType) throws NoSuchFieldException, IllegalAccessException {
        Class<?> targetClazz = target.getClass();
        Field field = targetClazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return fieldClassType.cast(field.get(target));
    }
}
