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
package org.opendata.core.util;

import java.text.NumberFormat;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MemUsagePrinter {
    
    public void print(String headline) {
	
	Runtime runtime = Runtime.getRuntime();
	NumberFormat format = NumberFormat.getInstance();
	long maxMemory = runtime.maxMemory();
	long allocatedMemory = runtime.totalMemory();
	long freeMemory = runtime.freeMemory();
        
        System.out.println(headline);
	System.out.println("free memory: " + format.format(freeMemory / 1024));
	System.out.println("allocated memory: " + format.format(allocatedMemory / 1024));
	System.out.println("max memory: " + format.format(maxMemory / 1024));
	System.out.println("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
    }
    
    public void print() {
    
        this.print("MEMORY USAGE");
    }
}
