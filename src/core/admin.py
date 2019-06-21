from django.contrib import admin
import simplejson as json
from django.utils.translation import ugettext_lazy as _
from models import *


class ChannelAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug')
    search_fields = ('title',)
    fields = ('title', 'slug')


class UserAdmin(admin.ModelAdmin):
    # list_display = ('nickname', 'avat', 'xp', 'gold', 'rock')
    search_fields = ('nickname',)


class GateAdmin(admin.ModelAdmin):
    list_display = ('gate_id', 'vers', 'rs', 'itemTypes', 'props', 'taskStep', 'tasks', 'scores', 'gird',
                    'newGridTypes', 'newGrid', 'portal', 'item', 'itemBg', 'wallH', 'wallV', 'taskBgItem',
                    'wayDownOut', 'attach', 'diff', 'taskType', 'trackBelt', 'iceWall', 'flipBlocker', 'movingFloor')
    search_fields = ('gate_id',)


class PropAdmin(admin.ModelAdmin):
    list_display = ('pid', 'name', 'num', 'type', 'expired')
    search_fields = ('pid', 'name')
    fields = ('pid', 'name', 'num', 'type', 'expired')


admin.site.register(Channel, ChannelAdmin)
admin.site.register(User, UserAdmin)
# admin.site.register(Mail, MailAdmin)
admin.site.register(Gate, GateAdmin)
admin.site.register(Prop, PropAdmin)
