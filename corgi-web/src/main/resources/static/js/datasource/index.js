$(function () {
    // 数据对象
    var dataSourceArray = []

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            dataSourceArray: dataSourceArray
        },
        methods: {
            deleteDataSourceById: function (id) {
                if (confirm("是否确认删除？\n删除数据源将同时删除数据源关联任务！")) {
                    deleteDataSourceById(id)
                }
            },
            gotoAddDataSource: function () {
                window.location.href = '/datasource/editor/';
            }
        }
    });

    /**
     * 查询所有数据源
     * */
    function queryAllDataSource() {
        $.get('/api/datasource').done(function (data) {
            app.dataSourceArray = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    /**
     * 根据数据源 ID 删除数据源
     *
     * */
    function deleteDataSourceById(id) {
        var url = '/api/datasource/' + id
        $.ajax(url, {
            method: 'DELETE'
        }).done(function () {
            alert("删除成功！");
            queryAllDataSource();
        }).fail(function () {
            alert("删除失败！");
        });
    }

    queryAllDataSource();
});

