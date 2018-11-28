package com.dyingbleed.corgi.web.mapper;

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

}
