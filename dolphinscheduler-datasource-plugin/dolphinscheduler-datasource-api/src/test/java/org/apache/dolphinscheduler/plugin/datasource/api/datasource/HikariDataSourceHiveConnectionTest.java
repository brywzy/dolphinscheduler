package org.apache.dolphinscheduler.plugin.datasource.api.datasource;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class HikariDataSourceHiveConnectionTest {

    public static void main(String[] args) throws SQLException, InterruptedException {

        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        dataSource.setJdbcUrl("jdbc:hive2://cfhy-test-bigdata-13:10000");
        dataSource.setUsername( "dataanalysis");
        dataSource.setPassword(PasswordUtils.decodePassword("dataanalysis"));

        dataSource.setMinimumIdle(5);
        dataSource.setMaximumPoolSize(50);
        dataSource.setConnectionTestQuery("select 1");

        Connection connection = dataSource.getConnection();
        System.out.println("---"+connection);
        Statement statement = connection.createStatement();
        statement.execute("set mapreduce.job.queuename=etl");
        statement.execute("insert overwrite table bi_analysis.car partition(dt='2023-07-11') " +
                "select id, created_date, last_modified_date, is_free, is_delete, capacity_tonnage, sum_capacity_tonnage, license_image, " +
                "license_expire, plate_number, car_color, shipping_cert, shipping_cert_image, shipping_cert_expire, car_model_id, owner_id, driver_id, " +
                "applicant_id, axle_number, auth_status, current_orderitem_sn, payee_agent, is_stop, is_freeze, shipping_cert_frontSide_image, " +
                "priority_reason_id, priority, car_file_id, owner_company, weight_sum_capacity_tonnage, weight_priority, unload_type, data_from, " +
                "contractor_status,flag_status,plate_color,road_trans_operat_permit_no,license_image_back,vin,use_character,license_org, " +
                "register_date,issue_date,owner_name,car_ascription_certificate_image,fuel_flag " +
                "from order_center.car where dt='2023-07-11'  ");
        connection.close();

        Connection connection1 = dataSource.getConnection();
        System.out.println("---"+connection1);
        Statement statement1 = connection1.createStatement();
        statement1.execute("set mapreduce.job.queuename=etl");
        statement1.execute("insert overwrite table bi_analysis.order_item partition(dt='2023-07-11') " +
                " " +
                "select " +
                "    id, order_business_id, order_common_id, order_broker_id, created_date, " +
                "    last_modified_date, version, amount, tax_amount, invoice_amount, " +
                "    frozen_amount, cancel_reason, freeze_status, freeze_msg, freight, " +
                "    gps_end_time, gps_start_time, gao_de_gps_path, gao_de_gps_position, " +
                "    zhong_jiao_xing_lu_gps_path, is_delete, original_price, original_ton, " +
                "    original_ton_image_url, rejecte_reason, sn, " +
                "    from_unixtime(IF(SUBSTR(sn,1,2)='YD', unix_timestamp(SUBSTR(sn,3,14),'yyyyMMddHHmmss'),unix_timestamp(SUBSTR(sn,1,8),'yyyyMMdd')),'yyyy-MM-dd HH:mm:ss') as sn_time, " +
                "    order_sn, status, status_bd, " +
                "    ton, car_id, chat_group_id, driver_id, order_item_report_id, old_id, " +
                "    expected_payment_date, is_valid_wucheorderitem, validate_wucheorderitem_message, " +
                "    broker_original_ton, final_amount, final_freight, final_original_price, " +
                "    service_charge_type, service_charge, total_service_charge, car_owner_id, " +
                "    payment_ton, business_id, accept_date, broker_accept_date, payment_date, " +
                "    over_time, translate(translate(translate(driver_name, ' ', ''), '(','（'), ')','）') as driver_name, " +
                "    driver_phone, car_number, created_user_id, created_user_name, " +
                "    transfer_num, payee_id, assess_flag, is_problem, loading_weighing_time, unloading_weighing_time, " +
                "    time_update, platform_service_fee, platform_service_fee_radio, dispatch_fare, business_settle_day, " +
                "    broker_settle_day, platform_dispatch_fare, is_platform_order, cargo_type_note,broker_flag,business_created_date, " +
                "    is_export_bill,is_pay_bank,publish_user_id,tax_publish_flag,tax_threshold,risk_check_result,fuel_flag,agent_flag,flag,agent_id,risk_check_success_reason, " +
                "    is_credit " +
                "from order_center.order_item where dt='2023-07-11'");
        connection1.close();

        Thread.sleep(10000000);

    }
}
