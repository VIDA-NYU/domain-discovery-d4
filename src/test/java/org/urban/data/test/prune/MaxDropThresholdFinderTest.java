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
package org.urban.data.test.prune;

import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.prune.MaxDropThresholdFinder;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MaxDropThresholdFinderTest {
    
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
    public void testDropFinder() {
	
        ArrayList<IdentifiableDouble> elements = new ArrayList<>();
	elements.add(new IdentifiableDouble(10, 0.75));
	elements.add(new IdentifiableDouble(1, 0.7));
	elements.add(new IdentifiableDouble(2, 0.6));
	elements.add(new IdentifiableDouble(3, 0.57));
	elements.add(new IdentifiableDouble(7, 0.55));
	elements.add(new IdentifiableDouble(8, 0.49));
	elements.add(new IdentifiableDouble(4, 0.48));
	elements.add(new IdentifiableDouble(5, 0.45));
	elements.add(new IdentifiableDouble(6, 0.4));
	elements.add(new IdentifiableDouble(11, 0.35));
	elements.add(new IdentifiableDouble(12, 0.28));

        MaxDropFinder<IdentifiableDouble> dropFinder;
        dropFinder = new MaxDropFinder<>(0.5, true, true);

	assertEquals(2, new MaxDropFinder<>(0.5, true, true).getPruneIndex(elements));
	assertEquals(elements.size(), new MaxDropFinder<>(0.5, true, false).getPruneIndex(elements));
	assertEquals(5, new MaxDropThresholdFinder<>(0.5, true, true).getPruneIndex(elements));
	assertEquals(elements.size(), new MaxDropFinder<>(0.5, true, false).getPruneIndex(elements));
	
	elements = new ArrayList<>();
	elements.add(new IdentifiableDouble(10, 0.75));
	elements.add(new IdentifiableDouble(1, 0.7));
	elements.add(new IdentifiableDouble(1, 0.77));
	elements.add(new IdentifiableDouble(1, 0.73));
	elements.add(new IdentifiableDouble(2, 0.6));
	elements.add(new IdentifiableDouble(3, 0.57));
	elements.add(new IdentifiableDouble(7, 0.55));
	elements.add(new IdentifiableDouble(8, 0.5));
	elements.add(new IdentifiableDouble(4, 0.48));
	elements.add(new IdentifiableDouble(5, 0.45));
	elements.add(new IdentifiableDouble(6, 0.3));
	elements.add(new IdentifiableDouble(11, 0.35));
	elements.add(new IdentifiableDouble(12, 0.28));

    	assertEquals(10, new MaxDropFinder<>(0.5, true, true).getPruneIndex(elements));
	assertEquals(10, new MaxDropThresholdFinder<>(0.5, true, true).getPruneIndex(elements));
}
}
