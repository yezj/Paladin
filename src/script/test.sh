#!/bin/sh
while true;do
stty -icanon min 0 time 100
echo ".........................."
echo "1. 关卡汇总.xlsx"
echo "2. 武将汇总.xlsx"
echo "3. 经验经济汇总.xlsx"
echo "4. 武将汇总.xlsx 经验经济汇总.xlsx"
echo "5. 退出"
echo ".........................."
echo "请输入选择项"
read Arg
if [ "$Arg" = 3 ];then
    python get_match.py	> data.txt
elif [ "$Arg" = 2 ];then
    python get_hero.py	> data.txt
elif [ "$Arg" = 1 ];then
    python get_economy.py > data.txt
elif [ "$Arg" = 4 ];then
    python get_herostar.py > data.txt
elif [ "$Arg" = 5 ];then
    exit 1
else
    echo "请输入正确的编号"
fi
done;

echo
echo "others function..."

