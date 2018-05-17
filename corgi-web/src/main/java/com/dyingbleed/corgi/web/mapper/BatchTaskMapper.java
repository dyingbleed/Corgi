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
    @Insert("insert into batch " +
            "(name, datasource_id, source_db, source_table, mode, sink_db, sink_table) " +
            "values" +
            "(#{task.name}, #{task.datasource_id}, #{task.source_db}, #{task.source_table}, #{task.mode}, #{task.sink_db}, #{task.sink_table})")
    public void insertBatchTask(@Param("task") BatchTask task);

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    @Delete("delete from batch where id=#{id}")
    public void deleteBatchTaskById(@Param("id") Long id);

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
            "name=#{task.name}, " +
            "datasource_id=#{task.datasource_id}, " +
            "source_db=#{task.source_db}, " +
            "source_table=#{task.source_table}, " +
            "mode=#{task.mode}, " +
            "sink_db=#{task.sink_db}, " +
            "sink_table=#{task.sink_table}")
    public void updateBatchTask(@Param("task") BatchTask task);

    /**
     * 查询所有批量任务
     *
     * @return 批量任务列表
     *
     * */
    @Select("select * from batch")
    public List<BatchTask> queryAllBatchTask();

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    @Select("select * from batch where id=#{id}")
    public BatchTask queryBatchTaskById(@Param("id") Long id);

}
