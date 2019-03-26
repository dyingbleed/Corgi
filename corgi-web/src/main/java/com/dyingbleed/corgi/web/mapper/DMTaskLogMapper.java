package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.DMTaskLog;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2019/3/21.
 */
@Mapper
public interface DMTaskLogMapper {

    @Results(value = {
            @Result(property = "taskId", column = "task_id")
    })
    @Select("SELECT " +
            "  l.id," +
            "  l.task_id," +
            "  l.state," +
            "  l.content," +
            "  l.ts " +
            "FROM dm_task_log l " +
            "WHERE l.task_id=${taskId} " +
            "ORDER BY l.ts DESC " +
            "LIMIT 10")
    List<DMTaskLog> queryDMTaskLogByTaskId(@Param("taskId") Long taskId);

    @Insert("INSERT INTO dm_task_log (" +
            "  task_id," +
            "  state," +
            "  content" +
            ") VALUES (" +
            "  #{log.taskId}," +
            "  #{log.state}," +
            "  #{log.content}" +
            ")")
    void insertDMTaskLog(@Param("log") DMTaskLog log);

}
