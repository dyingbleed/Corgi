package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.core.bean.ODSTask;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/15.
 */
@Mapper
public interface ODSTaskMapper {

    @Insert("INSERT INTO ods_task (" +
            "  name, " +
            "  datasource_id, " +
            "  source_db, " +
            "  source_table, " +
            "  mode, " +
            "  time_column, " +
            "  sink_db, " +
            "  sink_table" +
            ") VALUES (" +
            "  #{task.name}, " +
            "  #{task.datasourceId}, " +
            "  #{task.sourceDb}, " +
            "  #{task.sourceTable}, " +
            "  #{task.mode}, " +
            "  #{task.timeColumn}, " +
            "  #{task.sinkDb}, " +
            "  #{task.sinkTable}" +
            ")")
    void insertODSTask(@Param("task") ODSTask task);

    @Delete("DELETE FROM ods_task WHERE id=#{id}")
    void deleteODSTaskById(@Param("id") Long id);

    @Delete("DELETE FROM ods_task WHERE datasource_id=#{datasource_id}")
    void deleteODSTaskByDatasourceId(@Param("datasource_id") Long dataSourceId);

    @Update("UPDATE ods_task " +
            "SET " +
            "  name=#{task.name}, " +
            "  datasource_id=#{task.datasourceId}, " +
            "  source_db=#{task.sourceDb}, " +
            "  source_table=#{task.sourceTable}, " +
            "  mode=#{task.mode}, " +
            "  time_column=#{task.timeColumn}, " +
            "  sink_db=#{task.sinkDb}, " +
            "  sink_table=#{task.sinkTable}" +
            "WHERE id=#{task.id}")
    void updateODSTask(@Param("task") ODSTask task);

    @Results(value = {
            @Result(property = "datasourceId", column = "datasource_id"),
            @Result(property = "sourceDb", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "sinkDb", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table")
    })
    @Select("SELECT " +
            "  id, " +
            "  name, " +
            "  datasource_id, " +
            "  source_db, " +
            "  source_table, " +
            "  mode, " +
            "  sink_db, " +
            "  sink_table " +
            "FROM ods_task")
    List<ODSTask> queryAllODSTask();

    @Results(value = {
            @Result(property = "datasourceId", column = "datasource_id"),
            @Result(property = "sourceDb", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "timeColumn", column = "time_column"),
            @Result(property = "sinkDb", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table")
    })
    @Select("SELECT " +
            "  id, " +
            "  name, " +
            "  datasource_id, " +
            "  source_db, " +
            "  source_table, " +
            "  mode, " +
            "  time_column, " +
            "  sink_db, " +
            "  sink_table " +
            "FROM ods_task " +
            "WHERE id=#{id}")
    ODSTask queryODSTaskById(@Param("id") Long id);

    @Results(value = {
            @Result(property = "datasourceId", column = "datasource_id"),
            @Result(property = "datasourceUrl", column = "datasource_url"),
            @Result(property = "datasourceUsername", column = "datasource_username"),
            @Result(property = "datasourcePassword", column = "datasource_password"),
            @Result(property = "sourceDb", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "timeColumn", column = "time_column"),
            @Result(property = "sinkDb", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table")
    })
    @Select("SELECT " +
            "  b.id, " +
            "  b.name, " +
            "  b.datasource_id, " +
            "  d.url as datasource_url, " +
            "  d.username as datasource_username, " +
            "  d.password as datasource_password, " +
            "  b.source_db, " +
            "  b.source_table, " +
            "  b.mode, " +
            "  b.time_column, " +
            "  b.sink_db, " +
            "  b.sink_table " +
            "FROM ods_task b " +
            "LEFT JOIN datasource d " +
            "ON b.datasource_id = d.id " +
            "WHERE b.name=#{name}")
    ODSTask queryODSTaskByName(@Param("name") String name);
}
