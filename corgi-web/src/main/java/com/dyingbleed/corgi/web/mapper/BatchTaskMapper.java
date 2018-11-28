package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.BatchTask;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/15.
 */
@Mapper
public interface BatchTaskMapper {

    /**
     * 新增批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    @Insert("insert into batch (" +
            "  name, " +
            "  datasource_id, " +
            "  source_db, " +
            "  source_table, " +
            "  mode, " +
            "  time_column, " +
            "  sink_db, " +
            "  sink_table" +
            ") values (" +
            "  #{task.name}, " +
            "  #{task.dataSourceId}, " +
            "  #{task.sourceDb}, " +
            "  #{task.sourceTable}, " +
            "  #{task.mode}, " +
            "  #{task.timeColumn}, " +
            "  #{task.sinkDb}, " +
            "  #{task.sinkTable}" +
            ")")
    void insertBatchTask(@Param("task") BatchTask task);

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    @Delete("delete from batch where id=#{id}")
    void deleteBatchTaskById(@Param("id") Long id);

    /**
     * 根据数据源 ID 删除批量任务
     *
     * @param dataSourceId 数据源 ID
     *
     * */
    @Delete("delete from batch where datasource_id=#{datasource_id}")
    void deleteBatchTaskByDataSourceId(@Param("datasource_id") Long dataSourceId);

    /**
     * 修改批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    @Update("update batch " +
            "set " +
            "  name=#{task.name}, " +
            "  datasource_id=#{task.dataSourceId}, " +
            "  source_db=#{task.sourceDb}, " +
            "  source_table=#{task.sourceTable}, " +
            "  mode=#{task.mode}, " +
            "  time_column=#{task.timeColumn}, " +
            "  sink_db=#{task.sinkDb}, " +
            "  sink_table=#{task.sinkTable}" +
            "where id=#{task.id}")
    void updateBatchTask(@Param("task") BatchTask task);

    /**
     * 查询所有批量任务
     *
     * @return 批量任务列表
     *
     * */
    @Results(value = {
            @Result(property = "id", column = "id"),
            @Result(property = "name", column = "name"),
            @Result(property = "sourceDb", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "mode", column = "mode"),
            @Result(property = "sinkDb", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table")
    })
    @Select("select " +
            "  id, " +
            "  name, " +
            "  source_db, " +
            "  source_table, " +
            "  mode, " +
            "  sink_db, " +
            "  sink_table " +
            "from batch")
    List<BatchTask> queryAllBatchTask();

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    @Results(value = {
            @Result(property = "id", column = "id"),
            @Result(property = "name", column = "name"),
            @Result(property = "dataSourceId", column = "datasource_id"),
            @Result(property = "sourceDb", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "mode", column = "mode"),
            @Result(property = "timeColumn", column = "time_column"),
            @Result(property = "sinkDb", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table")
    })
    @Select("select " +
            "  id, " +
            "  name, " +
            "  datasource_id, " +
            "  source_db, " +
            "  source_table, " +
            "  mode, " +
            "  time_column, " +
            "  sink_db, " +
            "  sink_table " +
            "from batch " +
            "where id=#{id}")
    BatchTask queryBatchTaskById(@Param("id") Long id);

    /**
     * 根据名称查询批量任务
     *
     * @param name 批量任务名称
     *
     * @return 批量任务
     *
     * */
    @Results(value = {
            @Result(property = "id", column = "id"),
            @Result(property = "name", column = "name"),
            @Result(property = "dataSourceId", column = "datasource_id"),
            @Result(property = "dataSourceUrl", column = "datasource_url"),
            @Result(property = "dataSourceUsername", column = "datasource_username"),
            @Result(property = "dataSourcePassword", column = "datasource_password"),
            @Result(property = "sourceDb", column = "source_db"),
            @Result(property = "sourceTable", column = "source_table"),
            @Result(property = "mode", column = "mode"),
            @Result(property = "timeColumn", column = "time_column"),
            @Result(property = "sinkDb", column = "sink_db"),
            @Result(property = "sinkTable", column = "sink_table")
    })
    @Select("select " +
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
            "from batch b " +
            "left join datasource d " +
            "on b.datasource_id = d.id " +
            "where b.name=#{name}")
    BatchTask queryBatchTaskByName(@Param("name") String name);
}
