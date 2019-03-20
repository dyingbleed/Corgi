$(function () {
    // 数据对象
    var batchTaskArray = []
    var columnArray = []

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            batchTaskId: 0,
            batchTaskArray: batchTaskArray,
            columnArray: columnArray
        },
        watch: {
            batchTaskId: function (id) {
                for (i in app.batchTaskArray) {
                    var batchTask = app.batchTaskArray[i];
                    if (batchTask.id == id) {
                        queryColumn(batchTask.sinkDb, batchTask.sinkTable);
                        break;
                    }
                }
            }
        }
    });

    /**
     * 查询所有批量任务
     * */
    function queryAllBatchTask() {
        $.get('/api/ods').done(function (data) {
            app.batchTaskArray = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    /**
     * 根据批量任务 ID 查询 Hive 表元数据
     *
     * */
    function queryColumn(db, table) {
        var url = "/api/hive//column/" + db + "/" + table;
        $.get(url).done(function (data) {
            app.columnArray = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    queryAllBatchTask();
});