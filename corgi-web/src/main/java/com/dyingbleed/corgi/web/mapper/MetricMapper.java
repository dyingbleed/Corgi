package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.BatchTaskMetric;
import org.apache.ibatis.annotations.*;

import java.util.Date;

/**
 * Created by 李震 on 2018/5/24.
 */
@Mapper
public interface MetricMapper {

    @Insert("insert into metric " +
            "(" +
            "  batch_task_name, " +
            "  execute_time" +
            ") values (" +
            "  #{m.batchTaskName}, " +
            "  #{m.executeTime}" +
            ")")
    void insertBatchTaskMetric(@Param("m") BatchTaskMetric metric);

    @Results(value = {
            @Result(property = "id", column = "id"),
            @Result(property = "batchTaskName", column = "batch_task_name"),
            @Result(property = "executeTime", column = "execute_time")
    })
    @Select("select " +
            "  m.id, " +
            "  m.batch_task_name, " +
            "  m.execute_time  " +
            "from metric m " +
            "join ( " +
            "  select " +
            "    m.batch_task_name, " +
            "    max(m.execute_time) as execute_time " +
            "  from metric m " +
            "  where m.batch_task_name = #{name} " +
            "  and m.execute_time < curdate() " +
            "  group by m.batch_task_name " +
            ") f  " +
            "on m.batch_task_name = f.batch_task_name " +
            "and m.execute_time = f.execute_time")
    BatchTaskMetric queryLastMetricByName(@Param("name") String name);

}
