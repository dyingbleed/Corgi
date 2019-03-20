$(function () {
    // 数据对象
    var dmTask = {};

    var datasourceArray = []; // 数据源列表
    var sourceDBArray = []; // Source 数据库
    var sourceTableArray = []; // Source 表
    var sinkDBArray = []; // Sink 数据库
    var sinkTableArray = []; // Sink 表
    var dayOffsetHelp = null;

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            dmTask: dmTask,
            datasourceArray: datasourceArray,
            sourceDBArray: sourceDBArray,
            sourceTableArray: sourceTableArray,
            sinkDBArray: sinkDBArray,
            sinkTableArray: sinkTableArray,
            dayOffsetHelp: dayOffsetHelp
        },
        watch: {
            "dmTask.sourceDB": function (oldValue) {
                queryAllSourceTable(oldValue);
            },
            "dmTask.datasourceId": function (oldValue) {
                queryAllSinkDB(oldValue);
            },
            "dmTask.sinkDB": function(oldValue) {
                queryAllSinkTable(app.dmTask.datasourceId, oldValue);
            },
            "dmTask.dayOffset": function (oldValue) {
                oldValue = _.toInteger(oldValue);
                if (_.isInteger(oldValue)) {
                    app.dayOffsetHelp = moment().add(oldValue, 'd').format('LL');
                } else {
                    app.dayOffsetHelp = "Unknown";
                }
            }
        },
        methods: {
            insertOrUpdateDMTask: function (dmTask) {
                insertOrUpdateDMTask(dmTask)
            }
        }
    });

    /**
     * 插入或更新批量任务
     *
     * @param task 批量任务
     *
     * */
    function insertOrUpdateDMTask(task) {
        $.post('/api/v1/dm', task).done(function () {
            alert("保存成功！");
            window.location.href = '/dm/';
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
     * */
    function queryAllSourceDB() {
        $.get('/api/hive/db').done(function (data) {
            app.sourceDBArray = data;
        }).fail(function (e) {
            alert("查询 Source 数据库失败！");
        });
    }

    /**
     * 查询所有 Source 表
     *
     * @param db 数据库名
     *
     * */
    function queryAllSourceTable(db) {
        var url = '/api/hive/table/' + db;
        $.get(url).done(function (data) {
            app.sourceTableArray = data;
        }).fail(function (e) {
            alert("查询 Source 表失败！");
        });
    }

    /**
     * 查询所有 Sink 数据库
     *
     * @param id 数据源 ID
     *
     * */
    function queryAllSinkDB(id) {
        var url = '/api/datasource/db/' + id;
        $.get(url).done(function (data) {
            app.sinkDBArray = data;
        }).fail(function (e) {
            alert("查询 Sink 数据库失败！");
        });
    }

    /**
     * 查询所有 Sink 表
     *
     * @param id 数据源 ID
     * @param db 数据库名
     *
     * */
    function queryAllSinkTable(id, db) {
        var url = '/api/datasource/table/' + id + '/' + db;
        $.get(url).done(function (data) {
            app.sinkTableArray = data;
        }).fail(function (e) {
            alert("查询 Sink 表失败！");
        });
    }

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    function queryDMTaskById(id) {
        var url = '/api/dm/' + id;
        $.get(url).done(function (data) {
            app.dmTask = data;
        }).fail(function () {
            alert("加载数据失败！");
        });
    }


    queryAllSourceDB();
    queryAllDataSource();
    if (!_.isNull(id)) {
        queryDMTaskById(id);
    }
});