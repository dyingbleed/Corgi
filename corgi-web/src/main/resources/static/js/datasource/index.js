$(function () {
    // 数据对象
    var datasource = []

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            datasource: datasource
        },
        methods: {
            deleteDataSource: function (id) {
                if (confirm("是否确认删除？")) {
                    deleteDataSource(id)
                }
            },
            gotoAddDataSource: function () {
                window.location.href = '/datasource/editor';
            }
        }
    });

    function queryAllDataSource() {
        $.get('/api/datasource')
            .done(function (datasource) {
                app.datasource = datasource
            });
    }

    function deleteDataSource(id) {
        var url = '/api/datasource/' + id
        $.ajax(url, {
            method: 'DELETE'
        }).done(function () {
            alert("删除成功！");
            queryAllDataSource();
        })
    }

    queryAllDataSource();
});

