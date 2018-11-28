$(function () {

    // 数据对象
    var batchTaskArray = [];
    // 数据源列表
    var dataSourceArray = [];

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            ds: '',
            keyword: '',
            dataSourceArray: dataSourceArray,
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
            },
            search: function (ds, keyword) {
                search(ds, keyword);
            },
            reset: function () {
                app.ds = '';
                app.keyword = '';
                app.batchTaskArray = batchTaskArray;
            }
        }
    });

    /**
     * 搜索
     *
     * @param ds 数据源
     * @param keyword 查询关键字
     *
     * */
    function search(ds, keyword) {
        app.batchTaskArray = _.filter(app.batchTaskArray, function(i) {
            var p = true;

            if (ds !== '') {
                p = p && i.sourceDb === ds;
            }

            if (keyword != null && _.trim(keyword) !== '') {
                p = p && _.includes(i.sourceTable, keyword)
            }

            return p;
        });
    }

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
     * 查询所有数据源
     * */
    function queryAllDataSource() {
        $.get('/api/datasource').done(function (data) {
            app.dataSourceArray = data;
        }).fail(function () {
            alert("加载数据源失败！");
        });
    }

    /**
     * 查询所有批量任务
     * */
    function queryAllBatchTask() {
        $.get('/api/batch').done(function (data) {
            batchTaskArray = data;
            app.batchTaskArray = batchTaskArray;
        }).fail(function () {
            alert("加载任务列表失败！");
        });
    }

    queryAllDataSource();
    queryAllBatchTask();

});