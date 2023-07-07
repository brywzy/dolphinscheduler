/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.google.common.base.Strings;

public class SqlSplitter {

    private SqlSplitter() {
    }

    private static final String LINE_SEPARATOR = "\n";

    /**
     * split sql by segment separator
     * <p>The segment separator is used
     * when the data source does not support multi-segment SQL execution,
     * and the client needs to split the SQL and execute it multiple times.</p>
     * @param sql
     * @param segmentSeparator
     * @return
     */
    public static List<String> split(String sql, String segmentSeparator) {
        if (Strings.isNullOrEmpty(segmentSeparator)) {
            return Collections.singletonList(sql);
        }

        String[] lines = sql.split(segmentSeparator);
        List<String> segments = new ArrayList<>();
        for (String line : lines) {
            if (line.trim().isEmpty() || line.startsWith("--")) {
                continue;
            }
            segments.add(line);
        }
        return segments;
    }

    public static void main(String[] args) {
        String sql = "insert overwrite table bi_analysis.car partition(dt='2023-07-06')\n" +
                "select id, created_date, last_modified_date, is_free, is_delete, capacity_tonnage, sum_capacity_tonnage, license_image,\n" +
                "license_expire, plate_number, car_color, shipping_cert, shipping_cert_image, shipping_cert_expire, car_model_id, owner_id, driver_id,\n" +
                "applicant_id, axle_number, auth_status, current_orderitem_sn, payee_agent, is_stop, is_freeze, shipping_cert_frontSide_image,\n" +
                "priority_reason_id, priority, car_file_id, owner_company, weight_sum_capacity_tonnage, weight_priority, unload_type, data_from,\n" +
                "contractor_status,flag_status,plate_color,road_trans_operat_permit_no,license_image_back,vin,use_character,license_org,\n" +
                "register_date,issue_date,owner_name,car_ascription_certificate_image,fuel_flag\n" +
                "from order_center.car where dt='2023-07-06';\n" +
                "\n" +
                "insert overwrite table bi_analysis.car partition(dt='2023-07-06')\n" +
                "select id, created_date, last_modified_date, is_free, is_delete, capacity_tonnage, sum_capacity_tonnage, license_image,\n" +
                "license_expire, plate_number, car_color, shipping_cert, shipping_cert_image, shipping_cert_expire, car_model_id, owner_id, driver_id,\n" +
                "applicant_id, axle_number, auth_status, current_orderitem_sn, payee_agent, is_stop, is_freeze, shipping_cert_frontSide_image,\n" +
                "priority_reason_id, priority, car_file_id, owner_company, weight_sum_capacity_tonnage, weight_priority, unload_type, data_from,\n" +
                "contractor_status,flag_status,plate_color,road_trans_operat_permit_no,license_image_back,vin,use_character,license_org,\n" +
                "register_date,issue_date,owner_name,car_ascription_certificate_image,fuel_flag\n" +
                "from order_center.car where dt='2023-07-06'";

        String segmentSeparator = ";";

        List<String> split = split(sql, segmentSeparator);

        System.out.println(split);


    }
}
