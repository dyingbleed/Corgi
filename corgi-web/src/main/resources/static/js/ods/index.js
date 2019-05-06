$(function () {

    // 数据对象
    var odsTaskArray = [];
    // 数据源列表
    var dataSourceArray = [];

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            ds: '',
            keyword: '',
            dataSourceArray: dataSourceArray,
            odsTaskArray: odsTaskArray
        },
        methods: {
            runODSTaskById: function (id) {
                runODSTaskById(id);
            },
            deleteODSTaskById: function (id) {
                if (confirm("是否确认删除？")) {
                    deleteODSTaskById(id);
                }
            },
            search: function (ds, keyword) {
                search(ds, keyword);
            },
            reset: function () {
                app.ds = '';
                app.keyword = '';
                app.odsTaskArray = odsTaskArray;
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
        app.odsTaskArray = _.filter(odsTaskArray, function(i) {
            var p = true;

            if (ds !== '') {
                p = p && _.lowerCase(i.sourceDb) === _.lowerCase(ds);
            }

            if (keyword != null && _.trim(keyword) !== '') {
                p = p && _.includes(_.lowerCase(i.sourceTable), _.lowerCase(keyword))
            }

            return p;
        });
    }

    /**
     * 根据 ID 运行 DM 任务
     *
     * @param id 批量任务 ID
     *
     * */
    function runODSTaskById(id) {
        var url = '/api/ods/run/' + id;
        $.post(url).done(function () {
            alert("任务已启动！");
        }).fail(function () {
            alert("任务启动失败！");
        });
    }

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    function deleteODSTaskById(id) {
        var url = '/api/ods/' + id;
        $.ajax(url, {
            method: 'DELETE'
        }).done(function () {
            alert("删除成功！");
            queryAllODSTask();
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
    function queryAllODSTask() {
        $.get('/api/ods').done(function (data) {
            odsTaskArray = data;
            app.odsTaskArray = odsTaskArray;
        }).fail(function () {
            alert("加载 ODS 任务列表失败！");
        });
    }

    queryAllDataSource();
    queryAllODSTask();
});