package com.dyingbleed.corgi.web.mapper;

import com.dyingbleed.corgi.web.bean.DataSource;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/9.
 */
@Mapper
public interface DataSourceMapper {

    /**
     * 新增数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     * */
    @Insert("insert into corgi.datasource (" +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password" +
            ") values (" +
            "  #{ds.name}, " +
            "  #{ds.url}, " +
            "  #{ds.username}, " +
            "  #{ds.password}" +
            ")")
    void insertDataSource(@Param("ds") DataSource ds);

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    @Delete("delete from corgi.datasource where id=#{id}")
    void deleteDataSourceById(@Param("id") Long id);

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     * */
    @Update("update corgi.datasource " +
            "set " +
            "  name=#{ds.name}, " +
            "  url=#{ds.url}, " +
            "  username=#{username}, " +
            "  password=#{password}" +
            "where id=#{ds.id}")
    void updateDataSource(@Param("ds") DataSource ds);

    /**
     * 查询所有数据源
     *
     * @return 数据源
     * */
    @Select("select " +
            "  id, " +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password " +
            "from corgi.datasource")
    List<DataSource> queryAllDataSource();

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     * */
    @Select("select " +
            "  id, " +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password " +
            "from corgi.datasource " +
            "where id=#{id}")
    DataSource queryDataSourceById(@Param("id") Long id);

    /**
     * 根据名称查询数据源
     *
     * @param name 数据源名称
     *
     * @return 数据源
     * */
    @Select("select " +
            "  id, " +
            "  name, " +
            "  url, " +
            "  username, " +
            "  password " +
            "from corgi.datasource " +
            "where name=#{name}")
    DataSource queryDataSourceByName(@Param("name") String name);

}
