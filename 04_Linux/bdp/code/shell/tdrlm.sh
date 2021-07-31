#!/bin/sh

d=$(date "+%Y%m%d%H")   ### 当前时间 精确到小时
ods_path='/data/ods'    ### ftp 目录
bak_path='/data/bak'    ### 备份目录
oper_path='/data/oper'  ### 加载过程中目录

echo "satrting date to $d"

mkdir -p $bak_path/$d
mkdir -p $oper_path/$d


cd $ods_path
## 解压
for filename in `ls $ods_path`
do
echo "We are finding '$filename' under the folder '$ods_path'"
  if [ ${filename##*.} == 'rar' ]
  then
    unrar e $filename
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
    mv $filenames $oper_path/$d/$newfilename
  fi
done

echo ">>>>>>>完毕"
