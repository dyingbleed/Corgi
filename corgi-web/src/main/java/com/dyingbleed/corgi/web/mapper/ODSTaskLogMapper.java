package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.ODSTaskLog;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2019-05-06.
 */
@Mapper
public interface ODSTaskLogMapper {

    @Results(value = {
            @Result(property = "taskId", column = "task_id")
    })
    @Select("SELECT " +
            "  l.id," +
            "  l.task_id," +
            "  l.state," +
            "  l.content," +
            "  l.ts " +
            "FROM ods_task_log l " +
            "WHERE l.task_id=${taskId} " +
            "ORDER BY l.ts DESC " +
            "LIMIT 10")
    List<ODSTaskLog> queryODSTaskLogByTaskId(@Param("taskId") Long taskId);

    @Insert("INSERT INTO ods_task_log (" +
            "  task_id," +
            "  state," +
            "  content" +
            ") VALUES (" +
            "  #{log.taskId}," +
            "  #{log.state}," +
            "  #{log.content}" +
            ")")
    void insertODSTaskLog(@Param("log") ODSTaskLog log);

}
