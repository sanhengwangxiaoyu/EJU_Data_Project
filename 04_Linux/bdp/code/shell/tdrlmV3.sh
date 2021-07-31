#!/bin/sh

d=$(date "+%Y%m%d%H")   ### 当前时间 精确到小时
ods_path='/data/ods'    ### ftp 目录
bak_path='/data/bak'    ### 备份目录
oper_path='/data/oper'  ### 加载过程中目录
succ_path='/data/succ'  ### 加载完成目录
failed_path='/data/failed'  ### 加载失败目录
json_name='city_newest_issue_supply.json'  ### json文件名字
step=20 #间隔的秒数，不能大于60
bak_dir="$bak_path/$d" #根据日期创建数据目录
succ_dir="$succ_path/$d" #根据日期创建数据目录
failed_dir="$failed_path/$d" #根据日期创建数据目录


for((i=0;i<=60;i=(i+step))); do
  
  echo "satrting date to $d"
  cd $ods_path
  ## 解压
  for filename in `ls $ods_path`
  do
    echo "We are finding '$filename' under the folder '$ods_path'"
    if [ ${filename##*.} == 'rar' ]
    then
      unrar e $filename
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
      mv $filenames $bak_path/$d/$newfilename
    else 
      mv $filenames $oper_path/$filenames
    fi
  done

  ## 加载数据文件
  for fn in `ls $oper_path`
  do
    echo "File name currently being loaded : "$fn
    newfn=${fn%.*}_$d
    if  [ ${fn##*.} == 'txt' ]
    then
      #python2 /opt/datax/bin/datax.py /opt/datax/job/job.json > /root/datax$d.log 2>&1
      python2 /opt/datax/bin/datax.py -p "-Dfilename=${fn} -Dpath=${oper_path}" /bdp/conf/datax/${json_name} 
      if [ $? -ne 0 ]; then
        echo "data load to mysql filed"
        if [ ! -d $failed_dir ]; then
          mkdir -p -m 755 $failed_dir
          echo "mkdir -p -m 755 ${failed_dir} done"
        fi
        mv $oper_path/$fn $failed_path/$d/$newfn
      else
        echo "data load to mysql success"
        if [ ! -d $succ_dir ]; then
          mkdir -p -m 755 $succ_dir
          echo "mkdir -p -m 755 ${succ_dir} done"
        fi
        mv $oper_path/$fn $succ_path/$d/$newfn
      fi
      #cat /root/datax$d.log
    fi
  done

  #rm -rf  /root/datax$d.log

  echo ">>>>>>>Done"
  echo "End time is "$(date "+%Y%m%d%H") 
  sleep $step
done
exit 0
