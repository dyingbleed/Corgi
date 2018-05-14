$(function () {
    // 数据对象
    var datasource = {}

    // Vue 实例
    var app = new Vue({
        el: "#app",
        data: {
            datasource: datasource
        },
        methods: {
            insertDataSource: function () {
                $.ajax('/api/datasource', {
                    method: 'PUT',
                    data: app.datasource
                }).done(function () {
                    alert('保存成功！');
                    window.location.href = '/datasource';
                })
            },
            testDataSourceConnection: function () {
                $.get('/api/datasource/test', app.datasource)
                    .done(function (data) {
                        alert(data);
                    });
            }
        }
    });

    function queryDataSourceById(id) {
        var url = '/api/datasource/' + id;
        $.get(url)
            .done(function (data) {
                app.datasource = data;
            });
    }

    if (!_.isUndefined(id)) queryDataSourceById(id);
});