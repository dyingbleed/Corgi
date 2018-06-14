package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.ExecuteLog;
import com.dyingbleed.corgi.web.bean.Measure;
import com.dyingbleed.corgi.web.bean.MeasureStat;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/24.
 */
@Mapper
public interface MeasureMapper {

    @Insert("insert into measure " +
            "(" +
            "  name, " +
            "  submission_time, " +
            "  completion_time, " +
            "  elapsed_seconds, " +
            "  input_rows, " +
            "  input_data, " +
            "  output_rows, " +
            "  output_data " +
            ") values (" +
            "  #{m.name}, " +
            "  #{m.submissionTime}, " +
            "  #{m.completionTime}, " +
            "  #{m.elapsedSeconds}, " +
            "  #{m.inputRows}, " +
            "  #{m.inputData}, " +
            "  #{m.outputRows}, " +
            "  #{m.outputData} " +
            ")")
    void insertMeasure(@Param("m") Measure measure);

    @Results(value = {
            @Result(property = "taskCount", column = "task_count"),
            @Result(property = "elapsedSecondSum", column = "elapsed_second_sum")
    })
    @Select("select " +
            "  count(1) as task_count, " +
            "  sum(elapsed_seconds) as elapsed_second_sum " +
            "from measure " +
            "where submission_time >= curdate()")
    MeasureStat queryTodayMeasureStat();

    @Results(value = {
            @Result(property = "id", column = "id"),
            @Result(property = "name", column = "name"),
            @Result(property = "submissionTime", column = "submission_time"),
            @Result(property = "completionTime", column = "completion_time"),
            @Result(property = "elapsedSeconds", column = "elapsed_seconds"),
            @Result(property = "inputRows", column = "input_rows"),
            @Result(property = "inputData", column = "input_data"),
            @Result(property = "outputRows", column = "output_rows"),
            @Result(property = "outputData", column = "output_data"),
    })
    @Select("select " +
            "  id, " +
            "  name, " +
            "  submission_time, " +
            "  completion_time, " +
            "  elapsed_seconds, " +
            "  input_rows, " +
            "  input_data, " +
            "  output_rows, " +
            "  output_data " +
            "from measure " +
            "where submission_time >= curdate()")
    List<Measure> queryTodayMeasureDetail();

    @Insert("insert into execute_log " +
            "(" +
            "  batch_task_name, " +
            "  execute_time" +
            ") values (" +
            "  #{l.batchTaskName}, " +
            "  #{l.executeTime}" +
            ")")
    void insertExecuteLog(@Param("l") ExecuteLog l);

    @Results(value = {
            @Result(property = "id", column = "id"),
            @Result(property = "batchTaskName", column = "batch_task_name"),
            @Result(property = "executeTime", column = "execute_time")
    })
    @Select("select " +
            "  l.id, " +
            "  l.batch_task_name, " +
            "  l.execute_time  " +
            "from execute_log l " +
            "join ( " +
            "  select " +
            "    l.batch_task_name, " +
            "    max(l.execute_time) as execute_time " +
            "  from execute_log l " +
            "  where l.batch_task_name = #{name} " +
            "  and l.execute_time < curdate() " +
            "  group by l.batch_task_name " +
            ") f  " +
            "on l.batch_task_name = f.batch_task_name " +
            "and l.execute_time = f.execute_time")
    ExecuteLog queryLastExecuteLogByName(@Param("name") String name);

}
