/*
 * This file is part of the Data-Driven Domain Discovery Tool (D4).
 * 
 * Copyright (c) 2018-2020 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opendata.test.db;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.util.StringHelper;
import org.opendata.db.eq.CompressedTermIndexIterator;
import org.opendata.db.eq.EQ;

/**
 * Unit tests for the compressed term index iterator.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CompressedTermIndexIteratorTest {
    
    public CompressedTermIndexIteratorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testInputBufferIterator() throws java.io.IOException {
    
        String[] input = new String[]{
            "0\t1\t1:1,2:2",
            "2\t2,3\t2:2,3:3,4:4"
        };
        
        StringReader reader = new StringReader(StringHelper.joinStrings(input, "\n"));
        BufferedReader in = new BufferedReader(reader);
        
        CompressedTermIndexIterator iterator;
        iterator = new CompressedTermIndexIterator(in, true);
        
        ArrayList<EQ> eqs = new ArrayList<>();
        while (iterator.hasNext()) {
            eqs.add(iterator.next());
        }
        
        assertEquals(2, eqs.size());
        assertEquals(0, eqs.get(0).id());
        assertEquals(2, eqs.get(1).id());
    }
}
