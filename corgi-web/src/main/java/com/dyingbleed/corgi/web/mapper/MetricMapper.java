package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.BatchTaskMetric;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * Created by 李震 on 2018/5/24.
 */
@Mapper
public interface MetricMapper {

    @Insert("insert into metric " +
            "(batch_task_name, execute_time) " +
            "values " +
            "(#{m.batch_task_name}, #{m.execute_time})")
    public void insertBatchTaskMetric(@Param("m") BatchTaskMetric metric);
    
    @Select("SELECT " +
            "  m.* " +
            "FROM metric m " +
            "JOIN ( " +
            "  SELECT " +
            "    m.batch_task_name, " +
            "    MAX(m.execute_time) as execute_time " +
            "  FROM metric m " +
            "  WHERE m.batch_task_name = #{name} " +
            "  GROUP BY m.batch_task_name " +
            ") f  " +
            "ON m.batch_task_name = f.batch_task_name " +
            "AND m.execute_time = f.execute_time")
    public BatchTaskMetric queryLastBatchTaskMetricByName(@Param("name") String name);

}
