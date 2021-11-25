// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.util;


import org.junit.Assert;
import org.junit.Test;

public class SqlParserUtilsTest {

    @Test
    public void testRemoveCommentInSql() {
        final String originSql = "select count(*) from tbl where price > 10.0";
        final String originSql2 = "select count(*) from tbl where TEST_COLUMN != 'not--a comment'";

        {
            String sqlWithComment = "-- comment \n" + originSql;
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- comment \n -- comment\n" + originSql;
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- \n -- comment \n" + originSql;
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = originSql + "-- \n -- comment \n";
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- \n -- comment \n" + originSql + "-- \n -- comment \n";
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment */ " + originSql + "-- \n -- comment \n";
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1/comment2 */ " + originSql;
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * comment2 */ " + originSql;
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * comment2 */ /* comment3 / comment4 */ -- comment 5\n" + originSql;
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * \ncomment2 */ -- comment 5\n" + originSql + "/* comment3 / comment4 */";
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "/* comment1 * \ncomment2 */ -- comment 3\n" + originSql + "-- comment 5\n";
            Assert.assertEquals(originSql, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment2 = "/* comment1 * \ncomment2 */ -- comment 5\n" + originSql2 + "/* comment3 / comment4 */";
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "/* comment1 * comment2 */ /* comment3 / comment4 */ -- comment 5\n" + originSql2;
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "/* comment1 * comment2 */ " + originSql2;
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "/* comment1/comment2 */ " + originSql2;
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "-- \n -- comment \n" + originSql2;
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "-- comment \n" + originSql2;
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String sqlWithComment2 = "-- comment \n -- comment\n" + originSql2;
            Assert.assertEquals(originSql2, SqlParserUtils.removeCommentInSql(sqlWithComment2));
        }

        {
            String content = "        --  One-line comment and /**range\n" +
                    "/*\n" +
                    "Multi-line comment\r\n" +
                    "--  Multi-line comment*/\n" +
                    "select price as " +
                    "/*\n" +
                    "Multi-line comment\r\n" +
                    "--  Multi-line comment*/\n" +
                    "revenue from /*One-line comment-- One-line comment*/ v_lineitem;";
            String expectedContent = "select price as revenue from  v_lineitem;";
            String trimmedContent = SqlParserUtils.removeCommentInSql(content).replaceAll("\n", "").trim();
            Assert.assertEquals(trimmedContent, expectedContent);
        }

        {
            String sqlWithComment = "select count(*) from tbl WHERE column_name = '--this is not comment'\n " +
                    "LIMIT 100 offset 0";
            Assert.assertEquals(sqlWithComment, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "select count(*) from tbl WHERE column_name = '--this is not comment'";
            Assert.assertEquals(sqlWithComment, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "select count(*) from tbl WHERE column_name = '/*--this is not comment*/'";
            Assert.assertEquals(sqlWithComment, SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "-- comment \n" + originSql + "/* comment */;" + "-- comment \n" + originSql + "/* comment */";
            Assert.assertEquals("select count(*) from tbl where price > 10.0;\n" +
                    "select count(*) from tbl where price > 10.0", SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithComment = "select count(*) from tbl /* this is test*/  where price > 10.0 -- comment \n" +
                    ";" + "insert into tbl(id) values(?); -- comment \n";
            Assert.assertEquals("select count(*) from tbl   where price > 10.0 \n" +
                    ";insert into tbl(id) values(?);", SqlParserUtils.removeCommentInSql(sqlWithComment));
        }

        {
            String sqlWithoutComment = "select * from tbl where price=\"/* this is not comment */\"";
            Assert.assertEquals(sqlWithoutComment, SqlParserUtils.removeCommentInSql(sqlWithoutComment));
        }

        {
            String sqlWithoutComment = "select count(*) from tbl -- 'comment'\n";
            Assert.assertEquals("select count(*) from tbl", SqlParserUtils.removeCommentInSql(sqlWithoutComment));
        }
        {
            Assert.assertEquals("select count(*)",
                    SqlParserUtils.removeCommentInSql("select count(*) -- , --\t --/ --"));
        }
    }

}