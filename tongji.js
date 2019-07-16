// jquery完成数据上报
// 网页加载完成,调用function回调函数
$(document).ready(function(){
    /**
     * 上报用户信息 访问数据到nginx打点服务器
     */
    // 使用ajax发起get请求,地址,参数
    $.get("http://localhost:8888/dig",
        {
            "time"  :   gettime(),
            // nginx的access_log会默认记录用户ip,所以ip可以不上报
            // "ip"    :   getip(),
            "url"   :   geturl(),
            "refer" :   getrefer(),
            "ua"    :   getuser_agent()
        });
})

function gettime(){
    var nowDate = new Date();
    return nowDate.toLocaleString();
}

function geturl(){
    return window.location.href;
}

// function getip(){
//     return returnCitySN["cip"]+','+returnCitySN["cname"];
// }

function getrefer(){
    return document.referrer;
}

// function getcookie(){
//     return document.cookie;
// }

function getuser_agent(){
    return navigator.userAgent;
}