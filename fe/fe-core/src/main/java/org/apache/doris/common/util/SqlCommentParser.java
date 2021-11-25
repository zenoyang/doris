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

public class SqlCommentParser {

    private final String sql;
    private int pos;

    public SqlCommentParser(String sql) {
        this.sql = sql;
        this.pos = 0;
    }

    public String removeCommentSql() {
        StringBuilder newSql = new StringBuilder();
        int startIndex = 0;
        while (true) {
            Comment comment = this.nextComment();
            // the sql is parse over
            if (comment == null) {
                // process the sql without comment case
                if (startIndex != sql.length()) {
                    newSql.append(sql, startIndex, sql.length());
                }
                break;
            } else {
                newSql.append(sql, startIndex, comment.startIndex);
                startIndex = comment.endIndex;
            }
        }
        return newSql.toString().trim();
    }

    /**
     * Get next comment of sql
     * it only support two comment modes, one is -- ,the other is /*
     * @return null, only if sql parser over;
     */
    public Comment nextComment() {
        while (pos < sql.length()) {
            char c = sql.charAt(pos);
            Integer startIndex = null;
            switch (c) {
                // ignore the type of quote
                case '\'':
                    ++pos;
                    while (pos < sql.length()) {
                        c = sql.charAt(pos);
                        ++pos;
                        if (c == '\'') {
                            break;
                        }
                    }
                    break;
                case '`':
                    ++pos;
                    while (pos < sql.length()) {
                        c = sql.charAt(pos);
                        ++pos;
                        if (c == '`') {
                            break;
                        }
                    }
                    break;
                case '\"':
                    ++pos;
                    while (pos < sql.length()) {
                        c = sql.charAt(pos);
                        ++pos;
                        if (c == '\"') {
                            break;
                        }
                    }
                    break;
                // parse the type of comment
                case '/':
                    // possible start of '/*'
                    if (pos + 1 < sql.length()) {
                        char c1 = sql.charAt(pos + 1);
                        if (c1 == '*') {
                            startIndex = pos;
                            int end = sql.indexOf("*/", pos + 2);
                            if (end < 0) {
                                end = sql.length();
                            } else {
                                end += "*/".length();
                            }
                            pos = end;
                            Integer endIndex = pos;
                            return new Comment(startIndex, endIndex);
                        }
                    }
                case '-':
                    // possible start of '--' comment
                    if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '-') {
                        startIndex = pos;
                        pos = indexOfLineEnd(sql, pos + 2);
                        Integer endIndex = pos;
                        return new Comment(startIndex, endIndex);
                    }
                default:
                    if (isOpenQuote(c)) {
                        break;
                    } else {
                        // parse the type of ID
                        ++pos;
                        loop:
                        while (pos < sql.length()) {
                            c = sql.charAt(pos);
                            switch (c) {
                                case '\'':
                                case '`':
                                case '\"':
                                case '/':
                                    break loop;
                                case '-':
                                    // possible start of '--' comment
                                    if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '-') {
                                        break loop;
                                    }
                                default:
                                    ++pos;
                            }
                        }
                    }
            }
        }
        return null;
    }

    private boolean isOpenQuote(char character) {
        if (character == '\"') {
            return true;
        } else if (character == '`') {
            return true;
        } else if (character == '\'') {
            return true;
        }
        return false;
    }

    private int indexOfLineEnd(String sql, int i) {
        int length = sql.length();
        while (i < length) {
            char c = sql.charAt(i);
            switch (c) {
                case '\r':
                case '\n':
                    return i;
                default:
                    ++i;
            }
        }
        return i;
    }

    public static class Comment {
        private Integer startIndex;
        private Integer endIndex;

        Comment(Integer startIndex, Integer endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }
    }
}
