$(function () {

    // 数据对象
    var dmTaskArray = [];

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            dmTaskArray: dmTaskArray
        },
        methods: {
            runDMTaskById: function (id) {
                if (confirm("是否确认运行？")) {
                    runDMTaskById(id);
                }
            },
            deleteDMTaskById: function (id) {
                if (confirm("是否确认删除？")) {
                    deleteDMTaskById(id);
                }
            }
        }
    });

    /**
     * 根据 ID 运行 DM 任务
     *
     * @param id 批量任务 ID
     *
     * */
    function runDMTaskById(id) {
        var url = '/api/dm/run/' + id;
        $.post(url).done(function (url) {
            alert("任务运行成功！");
        }).fail(function () {
            alert("任务运行失败！");
        });
    }

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    function deleteDMTaskById(id) {
        var url = '/api/dm/' + id;
        $.ajax(url, {
            method: 'DELETE'
        }).done(function () {
            alert("删除成功！");
            queryAllDMTask();
        }).fail(function () {
            alert("删除失败！");
        });
    }

    /**
     * 查询所有批量任务
     * */
    function queryAllDMTask() {
        $.get('/api/dm').done(function (data) {
            app.dmTaskArray = data;
        }).fail(function () {
            alert("加载 DM 任务列表失败！");
        });
    }

    queryAllDMTask();
});