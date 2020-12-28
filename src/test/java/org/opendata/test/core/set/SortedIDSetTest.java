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
package org.opendata.test.core.set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.set.SortedObjectSet;
import org.opendata.core.util.IdentifiableCount;

/**
 *
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SortedIDSetTest {
    
    public SortedIDSetTest() {
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
    public void testSortedIDSetArray() {
    
        SortedObjectSet<IdentifiableInteger> elements;
        
        elements = new SortedObjectSet<>(new IdentifiableInteger[]{});
        assertArrayEquals(new Integer[0], elements.toKeyArray());
        
        elements = new SortedObjectSet<>(new IdentifiableInteger[]{
            new IdentifiableCount(2, 3),
            new IdentifiableCount(4, 2),
            new IdentifiableCount(5, 1),
            new IdentifiableCount(6, 0),
        });
        assertArrayEquals(new Integer[]{2, 4, 5, 6}, elements.toKeyArray());
    }
}
