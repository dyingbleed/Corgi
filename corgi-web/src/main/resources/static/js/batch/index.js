$(function () {
    // 数据对象
    var batchTaskArray = [];

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            batchTaskArray: batchTaskArray
        },
        methods: {
            deleteBatchTaskById: function (id) {
                if (confirm("是否确认删除？")) {
                    deleteBatchTaskById(id);
                }
            },
            updateBatchTaskSyncById: function(id, sync) {
                if (sync) {
                    if (confirm("同步是耗费资源的操作，请确认是否启用？")) {
                        updateBatchTaskSyncById(id, sync);
                    }
                } else {
                    updateBatchTaskSyncById(id, sync);
                }
            },
            gotoAddBatchTask: function () {
                window.location.href = '/batch/editor/'
            }
        }
    });

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    function deleteBatchTaskById(id) {
        var url = '/api/batch/' + id;
        $.ajax(url, {
            method: 'DELETE'
        }).done(function () {
            alert("删除成功！");
            queryAllBatchTask();
        }).fail(function () {
            alert("删除失败！");
        });
    }

    /**
     * 更新批量任务同步
     *
     * @param id 批量任务 ID
     * @param sync 是否同步
     *
     * */
    function updateBatchTaskSyncById(id, sync) {
        var url = '/api/batch/sync/' + id;
        $.post(url, {
            sync: sync
        }).done(function () {
            if (sync) {
                alert("启动同步成功！");
            } else {
                alert("关闭同步成功！");
            }
            queryAllBatchTask();
        }).fail(function () {
            if (sync) {
                alert("启动同步失败！");
            } else {
                alert("关闭同步失败！");
            }
        });
    }

    /**
     * 查询所有批量任务
     * */
    function queryAllBatchTask() {
        $.get('/api/batch').done(function (data) {
            app.batchTaskArray = data;
        }).fail(function () {
            alert("加载数据失败！");
        });
    }

    queryAllBatchTask();
});