package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.Alert;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2019/2/19.
 */
@Mapper
public interface AlertMapper {

    @Insert("INSERT INTO alert (" +
            "  level, type, batch_task_id, msg" +
            ") VALUES (" +
            "  #{alert.level}," +
            "  #{alert.type}," +
            "  #{alert.batchTaskId}," +
            "  #{alert.msg}" +
            ")")
    public void insertAlert(@Param("alert") Alert alert);

    @Update("UPDATE alert " +
            "SET level=#{alert.level}, msg=#{alert.msg} " +
            "WHERE type=#{alert.type} AND batch_task_id=#{alert.batchTaskId}")
    public void updateAlert(@Param("alert") Alert alert);

    @Delete("DELETE FROM alert " +
            "WHERE type=#{type} AND batch_task_id=#{batchTaskId}")
    public void deleteAlert(@Param("type") String type, @Param("batchTaskId") Long batchTaskId);

    @Results(value = {
            @Result(property = "batchTaskId", column = "batch_task_id"),
            @Result(property = "batchTaskName", column = "batch_task_name")
    })
    @Select("SELECT" +
            "  a.id, a.level, a.type, a.batch_task_id, b.name AS batch_task_name, a.msg " +
            "FROM alert a " +
            "LEFT JOIN batch b " +
            "ON a.batch_task_id = b.id " +
            "where a.type=#{type} AND a.batch_task_id=#{batchTaskId}")
    public Alert queryAlert(@Param("type") String type, @Param("batchTaskId") Long batchTaskId);

    @Results(value = {
            @Result(property = "batchTaskId", column = "batch_task_id"),
            @Result(property = "batchTaskName", column = "batch_task_name")
    })
    @Select("SELECT" +
            "  a.id, a.level, a.type, a.batch_task_id, b.name AS batch_task_name, a.msg " +
            "FROM alert a " +
            "LEFT JOIN batch b " +
            "ON a.batch_task_id = b.id")
    public List<Alert> queryAllAlerts();

}
