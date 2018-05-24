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
                if (_.isNull(id)) {
                    insertDataSource(app.dataSource);
                } else {
                    updateDataSource(app.dataSource);
                }
            },
            testDataSourceConnection: function () {
                $.get('/api/datasource/test', app. dataSource)
                    .done(function (data) {
                        alert(data);
                    });
            }
        }
    });

    function insertDataSource(ds) {
        $.ajax('/api/datasource', {
            method: 'PUT',
            data: app. dataSource
        }).done(function () {
            alert('保存成功！');
            window.location.href = '/datasource/';
        });
    }

    function updateDataSource(ds) {
        $.ajax('/api/datasource', {
            method: 'POST',
            data: app. dataSource
        }).done(function () {
            alert('保存成功！');
            window.location.href = '/datasource/';
        });
    }

    function queryDataSourceById(id) {
        var url = '/api/datasource/' + id;
        $.get(url).done(function (data) {
            app. dataSource = data;
        });
    }

    if (!_.isNull(id)) queryDataSourceById(id);
});