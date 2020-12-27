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
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.set.StringSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SetIntersectTest {
    
    public SetIntersectTest() {
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
    public void testImmutableIDSet() {
    
        IDSet set1 = new ImmutableIDSet(new Integer[]{1,2,3,5,7,10});
        IDSet set2 = new ImmutableIDSet(new Integer[]{0,3,6,7,12,15});
        
        IDSet intersect = set1.intersect(set2);
        for (int id : new Integer[]{3,7}) {
            assertTrue(intersect.contains(id));
        }
    }

    @Test
    public void testHashIDIDSet() {
    
        IDSet set1 = new HashIDSet(new int[]{1,2,3,5,7,10});
        IDSet set2 = new HashIDSet(new int[]{0,3,6,7,12,15});
        
        IDSet intersect = set1.intersect(set2);
        for (int id : new Integer[]{3,7}) {
            assertTrue(intersect.contains(id));
        }
    }

    @Test
    public void testStringSet() {
    
        StringSet set1 = new StringSet();
        for (String val : new String[]{"10:00 AM", "4:00 PM", "8:30 AM", "9:30 AM"}) {
            set1.add(val);
        }
        StringSet set2 = new StringSet();
        for (String val : new String[]{"11:00 AM", "4:00 PM", "8:00 AM", "9:30 AM"}) {
            set2.add(val);
        }
        
        StringSet intersect = set1.intersect(set2);
        for (String val : new String[]{"4:00 PM", "9:30 AM"}) {
            assertTrue(intersect.contains(val));
        }
    }
}
