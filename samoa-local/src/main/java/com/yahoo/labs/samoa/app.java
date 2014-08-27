/**
 * Created with IntelliJ IDEA.
 * User: amir
 * Date: 2014-08-26
 * Time: 16:20
 */
package com.yahoo.labs.samoa;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2014 Yahoo! Inc.
 * %%
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
 * #L%
 */

import com.yahoo.labs.samoa.sentinel.task.SentimentAnalysis;
import com.yahoo.labs.samoa.topology.impl.SimpleComponentFactory;

public class app
{
    public static void main( String[] args ) {
        SentimentAnalysis se = new SentimentAnalysis();
        se.setFactory(new SimpleComponentFactory());

        se.parallelismOption.setValue(4);
        se.instanceLimitOption.setValue(100);

        se.init();
    }
}
