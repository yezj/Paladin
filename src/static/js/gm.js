$(document).ready(function() {
    $("#init").click(function() {
        var nameval = $("#name").val();
        var heroval = $("#hero").val();
        if(nameval.length == 0 || heroval.length == 0)
        {
            if(nameval.length == 0 && heroval.length == 0)
            {
                alert("用户ID和武将ID不能为空");
            }
            else if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("武将ID不能为空");
            }

        }
        else {
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), hero: $("#hero").val(), type: "init"},
                dataType: "json",
                success: function (data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                    // Play with returned data in JSON format
                },
                error: function (XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }
    });
    $("#recruit").click(function() {
        var nameval = $("#name").val();
        var heroval = $("#hero").val();
        if(nameval.length == 0 || heroval.length == 0)
        {
            if(nameval.length == 0 && heroval.length == 0)
            {
                alert("用户ID和武将ID不能为空");
            }
            else if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("武将ID不能为空");
            }

        }
        else {
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), hero: $("#hero").val(), type: "recruit"},
                dataType: "json",
                success: function (data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                    // Play with returned data in JSON format
                },
                error: function (XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                    //alert("初始化成功");
                }
            });
        }
    });
    $("#dispear").click(function() {
        var nameval = $("#name").val();
        var heroval = $("#hero").val();
        if(nameval.length == 0 || heroval.length == 0)
        {
            if(nameval.length == 0 && heroval.length == 0)
            {
                alert("用户ID和武将ID不能为空");
            }
            else if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("武将ID不能为空");
            }

        }
        else
        {
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), hero: $("#hero").val(), type: "dispear"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                    //alert("初始化成功");
                }
            });
        }
    });
    $("#mprod").click(function() {
        var nameval = $("#name").val();
        var prodval = $("#prod").val();
        var numval = $("#num").val();
        if(nameval.length == 0 || prodval.length == 0 || numval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else if(prodval.length == 0)
            {
                alert("物品ID不能为空");
            }
            else
            {
                alert("物品数量不能为空");
            }

        }
        else
        {
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), prod: $("#prod").val(), num: $("#num").val(), type: "modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                    //alert("初始化成功");
                }
            });
        }
    });
    $("#mbatt").click(function() {
        var nameval = $("#name").val();
        var battval = $("#batt").val();
        if(nameval.length == 0 || battval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("关卡ID不能为空");
            }

        }
        else
        {
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), batt: $("#batt").val(), type: "modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                    //alert("初始化成功");
                }
            });
        }
    });
    $("#minst").click(function() {
        var nameval = $("#name").val();
        var instval = $("#inst").val();
        if(nameval.length == 0 || instval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("精英关卡ID不能为空");
            }

        }
        else
        {
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), inst: $("#inst").val(), type: "modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                    //alert("初始化成功");
                }
            });
        }
    });
    $("#mgold").click(function() {
        var nameval = $("#name").val();
        var goldval = $("#gold").val();
        if(nameval.length == 0 || goldval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("金币数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), gold: $("#gold").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }
    });
    $("#mcoin").click(function() {
        var nameval = $("#name").val();
        var coinvar = $("#acoin").val();
        if(nameval.length == 0 || coinvar.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("巅峰币数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), acoin: $("#acoin").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }
    });
    $("#mrock").click(function() {
        var nameval = $("#name").val();
        var rockval = $("#rock").val();
        if(nameval.length == 0 || rockval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("宝石数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), rock: $("#rock").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }

    });
    $("#mvrock").click(function() {
        var nameval = $("#name").val();
        var rockval = $("#vrock").val();
        if(nameval.length == 0 || rockval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("宝石数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), vrock: $("#vrock").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }

    });
    $("#mexped_coin").click(function() {
        var nameval = $("#name").val();
        var exped_coin = $("#exped_coin").val();
        if(nameval.length == 0 || exped_coin.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("五关币数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), exped_coin: $("#exped_coin").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }

    });
    $("#mversus_coin").click(function() {
        var nameval = $("#name").val();
        var versus_coin = $("#versus_coin").val();
        if(nameval.length == 0 || versus_coin.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("群雄币数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), versus_coin: $("#versus_coin").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }

    });
    $("#mfeat").click(function() {
        var nameval = $("#name").val();
        var featval = $("#feat").val();
        if(nameval.length == 0 || featval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("宝石数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), feat: $("#feat").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }

    });
    $("#mxp").click(function() {
        var nameval = $("#name").val();
        var xpval = $("#xp").val();
        if(nameval.length == 0 || xpval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("宝石数量不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), xp: $("#xp").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }

    });
    $("#mhp").click(function() {
        var nameval = $("#name").val();
        var hpval = $("#hp").val();
        if(nameval.length == 0 || hpval.length == 0)
        {
            if(nameval.length == 0)
            {
                alert("用户ID不能为空");
            }
            else
            {
                alert("体力值不能为空");
            }

        }
        else{
            $.ajax({
                type: "GET",
                url: "/gm/",
                data: {name: $("#name").val(), hp: $("#hp").val(), type:"modify"},
                dataType: "json",
                success: function(data) {
                    $("#errorinfo").html('<p class="error" style="color:#0F0">' + data["error"] + '</p>');
                },
                error: function(XMLHttpRequest, textStatus, errorThrown) {
                    $("#errorinfo").html('<p class="error" style="color:#F00">' + JSON.parse(XMLHttpRequest.responseText)["error"] + '</p>');
                }
            });
        }
    });

    });