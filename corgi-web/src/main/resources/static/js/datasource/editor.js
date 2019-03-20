$(function () {
    // 数据对象
    var  dataSource = {}

    // Vue 实例
    var app = new Vue({
        el: "#app",
        data: {
             dataSource:  dataSource
        },
        methods: {
            insertOrUpdateDataSource: function () {
                insertOrUpdateDatasource(app.dataSource);
            },
            testDataSourceConnection: function () {
                $.get('/api/datasource/test', app. dataSource).done(function (data) {
                    alert(data);
                });
            }
        }
    });

    function insertOrUpdateDatasource(ds) {
        $.post('/api/datasource', ds).done(function () {
            alert("保存成功！");
            window.location.href = '/datasource/';
        }).fail(function () {
            alert("保存失败！");
        });
    }

    function queryDataSourceById(id) {
        var url = '/api/datasource/' + id;
        $.get(url).done(function (data) {
            app. dataSource = data;
        }).fail(function () {
            alert("加载数据失败！");
        });
    }

    if (!_.isNull(id)) {
        queryDataSourceById(id);
    }
});