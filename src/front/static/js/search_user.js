function get_one_user(obj, is_staff)
{
    var url = "/userinfo/get_json_users/"
    if(is_staff=="1")
        url += "?is_staff=1"
    $("#"+obj).autocomplete({
        minLength: 0,
        source: function(request, response ) {
            $.ajax({
                type:"POST",
                url:url,
                dataType:"json",
                data:"q="+$("#"+obj).val(),
                success:function(data){
                    if(data.length<=0)
                    {
                        $("#id_error").css("display","block");
                        $("#id_error").html("<span class='error'>搜索用户名不存在，请输入用户名关键字</span>");							
                    }
                    else
                        $("#id_error").css("display","none");
                    response(data);
                }
            });				
        },			
        focus: function( event, ui ) {
            $("#"+obj).val( ui.item.username );
            return false;
        },
        select: function( event, ui ) {
            $("#"+obj).val( ui.item.username );
            $( "#id_"+obj ).val( ui.item.id );

            return false;
        }
    })
    .data( "autocomplete" )._renderItem = function( ul, item ) {
        return $( "<li></li>" )
            .data( "item.autocomplete", item )
            .append( "<a>" + item.username + "</a>")
            .appendTo( ul );
    }
}

function split( val ) {
    return val.split( /,\s*/ );
}
function extractLast( term ) {
    return split( term ).pop();
}
	
function get_more_user(obj, is_staff)
{
    var url = "/userinfo/get_json_users/"
    if(is_staff=="1")
        url += "?is_staff=1"    
    $("#"+obj)
    // don't navigate away from the field on tab when selecting an item
    .bind( "keydown", function( event ) {
        if ( event.keyCode === $.ui.keyCode.TAB &&
                $( this ).data( "autocomplete" ).menu.active ) {
            event.preventDefault();
        }
    })
    .autocomplete({
        source: function( request, response ) {
            $.ajax({
                type:"POST",
                url:url,
                dataType:"json",
                data:"q="+split( $("#"+obj).val() ).pop(),
                success:function(data){
                    if(data.length<=0)
                    {
                        $("#id_error").css("display","block");
                        $("#id_error").html("<span class='error'>搜索用户名不存在，请输入用户名关键字</span>");							
                    }
                    else
                        $("#id_error").css("display","none");
                    response(data);
                }
            });						
        },
        search: function() {
            // custom minLength
            var term = extractLast( this.value );
            if ( term.length < 0 ) {
                return false;
            }
        },
        focus: function() {
            // prevent value inserted on focus
            return false;
        },
        select: function( event, ui ) {
            var terms = split( this.value );
            var ids = split( $( "#id_"+obj ).val() );
            // remove the current input
            terms.pop();
            ids.pop();
            // add the selected item
            terms.push( ui.item.username );
            ids.push( ui.item.id );
            // add placeholder to get the comma-and-space at the end
            terms.push( "" );
            ids.push( "" );
            this.value = terms.join( ", " );
            $( "#id_"+obj ).val( ids.join( "," ) );
            return false;
        }
    })
    .data( "autocomplete" )._renderItem = function( ul, item ) {
        return $( "<li></li>" )
            .data( "item.autocomplete", item )
            .append( "<a>" + item.username + "</a>")
            .appendTo( ul );
    }    
}
