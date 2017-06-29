package com.hongya.transformer.mr.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.hongya.common.GlobalConstants;
import com.hongya.transformer.model.dim.StatsUserDimension;
import com.hongya.transformer.model.dim.base.BaseDimension;
import com.hongya.transformer.model.value.BaseStatsValueWritable;
import com.hongya.transformer.model.value.reduce.MapWritableValue;
import com.hongya.transformer.mr.IOutputCollector;
import com.hongya.transformer.service.IDimensionConverter;

public class StatsDeviceBrowserNewInstallUserCollector implements IOutputCollector {

    @Override
    /**
     * 给sql语句中的？赋值的方法
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();
    }

}
