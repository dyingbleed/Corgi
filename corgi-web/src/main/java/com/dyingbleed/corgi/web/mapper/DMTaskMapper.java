package com.dyingbleed.corgi.web.mapper;

/**
 * Created by 李震 on 2019/3/11.
 */

import com.dyingbleed.corgi.core.bean.DMTask;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface DMTaskMapper {

    @Insert("INSERT INTO dm_task (" +
            "  name," +
            "  source_db," +
            "  source_table," +
            "  mode," +
            "  datasource_id," +
            "  sink_db," +
            "  sink_table," +
            "  where_exp," +
            "  day_offset," +
            "  pks " +
            ") VALUES (" +
            "  #{task.name}," +
            "  #{task.sourceDB}," +
            "  #{task.sourceTable}," +
            "  #{task.mode}," +
            "  #{task.datasourceId}," +
            "  #{task.sinkDB}," +
            "  #{task.sinkTable}," +
            "  #{task.whereExp}," +
            "  #{task.dayOffset}," +
            "  #{task.pks} " +
            ")" +
            "")
    public void insertDMTask(@Param("task") DMTask task);

    @Update("UPDATE dm_task SET" +
            "  name=#{task.name}," +
            "  source_db=#{task.sourceDB}," +
            "  source_table=#{task.sourceTable}," +
            "  mode=#{task.mode}," +
            "  datasource_id=#{task.datasourceId}," +
            "  sink_db=#{task.sinkDB}," +
            "  sink_table=#{task.sinkTable}," +
            "  where_exp=#{task.whereExp}," +
            "  day_offset=#{task.dayOffset}," +
            "  pks=#{task.pks} " +
            "WHERE id=#{task.id}")
    public void updateDMTask(@Param("task") DMTask task);

    @Delete("DELETE FROM dm_task WHERE id=#{id}")
    public void deleteDMTaskById(@Param("id") Long id);

    @Delete("DELETE FROM dm_task WHERE datasource_id=#{datasource_id}")
    public void deleteDMTaskByDatasourceId(@Param("datasource_id") Long datasourceId);

    @Results(value = {
            @Result(property = "sourceDB", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "datasourceId", column = "datasource_id"),
            @Result(property = "sinkDB", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table"),
            @Result(property = "whereExp", column = "where_exp"),
            @Result(property = "dayOffset", column = "day_offset")
    })
    @Select("SELECT" +
            "  id," +
            "  name," +
            "  source_db," +
            "  source_table," +
            "  mode," +
            "  datasource_id," +
            "  sink_db," +
            "  sink_table," +
            "  where_exp," +
            "  day_offset," +
            "  pks " +
            "FROM dm_task")
    public List<DMTask> queryAllDMTask();

    @Results(value = {
            @Result(property = "sourceDB", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "datasourceId", column = "datasource_id"),
            @Result(property = "sinkDB", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table"),
            @Result(property = "whereExp", column = "where_exp"),
            @Result(property = "dayOffset", column = "day_offset")
    })
    @Select("SELECT" +
            "  id," +
            "  name," +
            "  source_db," +
            "  source_table," +
            "  mode," +
            "  datasource_id," +
            "  sink_db," +
            "  sink_table," +
            "  where_exp," +
            "  day_offset," +
            "  pks " +
            "FROM dm_task " +
            "WHERE id=#{id}")
    public DMTask queryDMTaskById(@Param("id") Long id);

    @Results(value = {
            @Result(property = "sourceDB", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "datasourceId", column = "datasource_id"),
            @Result(property = "datasourceUrl", column = "datasource_url"),
            @Result(property = "datasourceUsername", column = "datasource_username"),
            @Result(property = "datasourcePassword", column = "datasource_password"),
            @Result(property = "sinkDB", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table"),
            @Result(property = "whereExp", column = "where_exp"),
            @Result(property = "dayOffset", column = "day_offset")
    })
    @Select("SELECT" +
            "  t.id," +
            "  t.name," +
            "  t.source_db," +
            "  t.source_table," +
            "  t.where_exp," +
            "  t.day_offset," +
            "  t.datasource_id," +
            "  ds.url AS datasource_url," +
            "  ds.username AS datasource_username," +
            "  ds.password AS datasource_password," +
            "  t.sink_db," +
            "  t.sink_table," +
            "  t.mode, " +
            "  t.pks " +
            "FROM dm_task t " +
            "LEFT JOIN datasource ds " +
            "ON t.datasource_id = ds.id " +
            "WHERE t.name=#{name}")
    public DMTask queryDMTaskByName(@Param("name") String name);

}
