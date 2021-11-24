d=$(date "+%Y%m%d%H%M")   ### 当前时间 精确到小时
da=$(date "+%Y%m%d")    ### 当天日期
dda=$(date "+%Y-%m-%d %H:%M:%S")  ##当天精确时间
ods_path='/data/ods'    ### ftp 目录
bak_path='/data/bak'    ### 备份目录
oper_path='/data/oper'  ### 加载过程中目录
succ_path='/data/succ'  ### 加载完成目录
failed_path='/data/failed'  ### 加载失败目录
#json_name='city_newest_issue_supply.json'  ### json文件名字
json_name='dwb_poi_info_text2mysql.json' ### json文件名字
bak_dir="$bak_path/$da" #根据日期创建数据目录
succ_dir="$succ_path/$da" #根据日期创建数据目录
failed_dir="$failed_path/$da" #根据日期创建数据目录
  
echo "satrting date to $d"
cd $ods_path
## 解压
for filename in `ls $ods_path`
do
  echo "We are finding '$filename' under the folder '$ods_path'"
  if [ ${filename##*.} == 'rar' ]
  then
     /usr/bin/unrar e $ods_path/$filename $ods_path/
    if [ ! -d $bak_dir ]; then
      mkdir -p -m 755 $bak_dir
      echo "mkdir -p -m 755 ${bak_dir} done"
    fi
  elif [ ${filename##*.} == 'zip' ]
  then
     /usr/bin/unzip $ods_path/$filename -d $ods_path/
     if [ ! -d $bak_dir ]; then
         mkdir -p -m 755 $bak_dir
         echo "mkdir -p -m 755 ${bak_dir} done"
     fi
  fi
done

## 改名和移动
for filenames in `ls $ods_path`
do
  newfilename=${filenames%.*}_$d.${filenames##*.}
  if  [ ${filenames##*.} == 'rar' ]
  then
    mv $filenames $bak_dir/$newfilename
  elif [ ${filenames##*.} == 'zip' ]
  then
    mv $filenames $bak_dir/$newfilename
  else 
    mv $filenames $oper_path/
  fi
done

## 加载数据文件
for fn in `ls $oper_path`
do
  echo "File name currently being loaded : "$fn
  newfn=${fn%.*}_$d
  if  [ ${fn##*.} == 'txt' ]
  then
    mv $oper_path/$fn $oper_path/$newfn
    python2 /opt/datax/bin/datax.py -p "-Dfilename=${newfn} -Dpath=${oper_path} -Dtime='${dda}'" /bdp/conf/datax/${json_name} 
    if [ $? -ne 0 ]; then
      echo "data load to mysql filed"
      if [ ! -d $failed_dir ]; then
        mkdir -p -m 755 $failed_dir
        echo "mkdir -p -m 755 ${failed_dir} done"
      fi
      mv $oper_path/$newfn $failed_dir
    else
      echo "data load to mysql success"
      if [ ! -d $succ_dir ]; then
        mkdir -p -m 755 $succ_dir
        echo "mkdir -p -m 755 ${succ_dir} done"
      fi
      mv $oper_path/$newfn $succ_dir
    fi
  fi
done
cat -n /bdp/log/tdrlmV3.log |grep "写失败总数                    :" | awk '{print "第"$1"行>>>>>>>>>>>>>>>>>>>>>>>>>>>"$2$3$4$5$6}' | tail -n 1 >> /bdp/log/tdr_error.log
echo ">>>>>>>Done"
echo "End time is "$(date "+%Y-%m-%d %H:%M") 
exit 0
