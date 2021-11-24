echo "开始时间： $(date "+%Y-%m-%d %H:%M")"
if [ $2 == 1 ];then
    echo "正在运行脚本： $3"
    echo "脚本内容："
    cat $3
    if [ $1 == 77 ];then
        mysql -h 172.28.36.77 -uwanganming -pNDR_AhfzXT3MSxfh <$3
    elif [ $1 == 7 ];then
	mysql -h 47.96.87.7 -uroot -p000000 --default-character-set=utf8 <$3
    fi
else 
    echo "正在运行语句： $3"
    if [ $1 == 77 ];then
        mysql -h 172.28.36.77 -uwanganming -pNDR_AhfzXT3MSxfh -e "$3"
    elif [ $1 == 7 ];then
        mysql -h 47.96.87.7 -uroot -p000000 --default-character-set=utf8 -e "$3"
    fi
fi
echo "结束时间： $(date "+%Y-%m-%d %H:%M")"

