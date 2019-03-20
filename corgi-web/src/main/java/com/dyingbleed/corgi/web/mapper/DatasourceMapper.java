package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.Datasource;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/9.
 */
@Mapper
public interface DatasourceMapper {

    /**
     * 新增数据源
     *
     * @param ds 数据源
     *
     * */
    @Insert("INSERT INTO datasource (" +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password" +
            ") VALUES (" +
            "  #{ds.name}, " +
            "  #{ds.url}, " +
            "  #{ds.username}, " +
            "  #{ds.password}" +
            ")")
    void insertDatasource(@Param("ds") Datasource ds);

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    @Delete("DELETE FROM datasource WHERE id=#{id}")
    void deleteDatasourceById(@Param("id") Long id);

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * */
    @Update("UPDATE datasource " +
            "SET " +
            "  name=#{ds.name}, " +
            "  url=#{ds.url}, " +
            "  username=#{ds.username}, " +
            "  password=#{ds.password}" +
            "WHERE id=#{ds.id}")
    void updateDatasource(@Param("ds") Datasource ds);

    /**
     * 查询所有数据源
     *
     * @return 数据源
     * */
    @Select("SELECT " +
            "  id, " +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password " +
            "FROM datasource")
    List<Datasource> queryAllDatasource();

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     * */
    @Select("SELECT " +
            "  id, " +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password " +
            "FROM datasource " +
            "WHERE id=#{id}")
    Datasource queryDatasourceById(@Param("id") Long id);

    /**
     * 根据名称查询数据源
     *
     * @param name 数据源名称
     *
     * @return 数据源
     * */
    @Select("SELECT " +
            "  id, " +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password " +
            "FROM datasource " +
            "WHERE name=#{name}")
    Datasource queryDatasourceByName(@Param("name") String name);

}
