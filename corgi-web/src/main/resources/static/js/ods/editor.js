$(function () {
    // 数据对象
    var odsTask = {};

    var datasourceArray = []; // 数据源列表
    var sourceDBArray = []; // Source 数据库
    var sourceTableArray = []; // Source 表
    var timeColumn = {}; // 时间列
    var sinkDBArray = []; // Sink 数据库

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            odsTask: odsTask,
            datasourceArray: datasourceArray,
            sourceDBArray: sourceDBArray,
            sourceTableArray: sourceTableArray,
            timeColumn: timeColumn,
            sinkDBArray: sinkDBArray
        },
        watch: {
            "odsTask.datasourceId": function (oldValue) {
                queryAllSourceDB(oldValue);
            },
            "odsTask.sourceDb": function(oldValue) {
                queryAllSourceTable(app.odsTask.datasourceId, oldValue);
            },
            "odsTask.sourceTable": function (oldValue) {
                queryAllTimeColumn(app.odsTask.datasourceId, app.odsTask.sourceDb, oldValue);
            },
            "odsTask.mode": function (oldValue) {
                if (oldValue === 'COMPLETE') app.odsTask.timeColumn = null;
            }
        },
        methods: {
            insertOrUpdateODSTask: function (task) {
                insertOrUpdateTask(task);
            }
        }
    });

    /**
     * 插入或修改批量任务
     *
     * @param task 批量任务
     *
     * */
    function insertOrUpdateTask(task) {
        $.post('/api/ods', task).done(function () {
            alert("保存成功！");
            window.location.href = '/ods/';
        }).fail(function () {
            alert("保存失败！");
        });
    }

    /**
     * 查询所有数据源
     * */
    function queryAllDataSource() {
        $.get('/api/datasource').done(function (data) {
            app.datasourceArray = data;
        }).fail(function (e) {
            alert("查询数据源失败！");
        });
    }

    /**
     * 查询所有 Source 数据库
     *
     * @param id 数据源 ID
     *
     * */
    function queryAllSourceDB(id) {
        var url = '/api/datasource/db/' + id;
        $.get(url).done(function (data) {
            app.sourceDBArray = data;
        }).fail(function (e) {
            alert("查询 Source 数据库失败！");
        });
    }

    /**
     * 查询所有 Sink 数据库
     * */
    function queryAllSinkDB() {
        $.get('/api/hive/db').done(function (data) {
            app.sinkDBArray = data;
        }).fail(function (e) {
            alert("查询 Sink 数据库失败！");
        });
    }

    /**
     * 查询所有 Source 表
     *
     * @param id 数据源 ID
     * @param db 数据库名
     *
     * */
    function queryAllSourceTable(id, db) {
        var url = '/api/datasource/table/' + id + '/' + db;
        $.get(url).done(function (data) {
            app.sourceTableArray = data;
        }).fail(function (e) {
            alert("查询 Source 表失败！");
        });
    }

    /**
     * 查询所有时间列
     *
     * @param id
     * @param db
     * @param table
     *
     * */
    function queryAllTimeColumn(id, db, table) {
        var url = '/api/datasource/timecolumn/' + id + '/' + db + '/' + table;
        $.get(url).done(function (data) {
            app.timeColumn = data;
        }).fail(function (e) {
            alert("查询时间字段失败！");
        });
    }

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    function queryODSTaskById(id) {
        var url = '/api/ods/' + id;
        $.get(url).done(function (data) {
            app.odsTask = data;
        }).fail(function () {
            alert("加载数据失败！");
        });
    }

    queryAllDataSource();
    queryAllSinkDB();
    if (!_.isNull(id)) {
        queryODSTaskById(id);
    }
});