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