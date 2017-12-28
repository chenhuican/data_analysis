# data_analysis
Python数据分析脚本，工作邮件自动化报表
# ----- Git 常用命令速查表 ----

注：
origin 名字可以修改，比如修改成data_analysis ，master 就是本地分支名称。-u 参数是否加上要看你这个仓库是否是主仓库，
如果加上了-u，那么之后你直接敲git push 或者 git pull 后就会用这个-u的仓库。
如果是副仓库建议不要加

使用 Git 管理文件时，每次结束工作前请依次执行git add、git commit和git push命令将文件推送到远程仓库。


---------------------------------------------------------------
## 情况一：
$ git remote -v
origin  https://github.com/chenhuican/pandas_sql.git (fetch)
origin  https://github.com/chenhuican/pandas_sql.git (push)

对于此类提交命令： git push origin master

## 情况二：
$ git remote -v
data_analysis   https://github.com/chenhuican/data_analysis.git (fetch)
data_analysis   https://github.com/chenhuican/data_analysis.git (push)

提交命令：$ git push data_analysis master
---------------------------------------------------------------

## 创建版本库

$ git clone <url>                  #克隆远程版本库
$ git init                         #初始化本地版本库

## 修改和提交

$ git status                       #查看状态
$ git diff                         #查看变更内容
$ git add .                        #跟踪所有改动过的文件
$ git add <file>                   #跟踪指定的文件
$ git mv <old><new>                #文件改名
$ git rm<file>                     #删除文件
$ git rm --cached<file>            #停止跟踪文件但不删除
$ git commit -m "commit messages"  #提交所有更新过的文件
$ git commit --amend               #修改最后一次改动


## 查看提交历史

$ git log                    #查看提交历史
$ git log -p <file>          #查看指定文件的提交历史
$ git blame <file>           #以列表方式查看指定文件的提交历史


## 撤销

$ git reset --hard HEAD      #撤销工作目录中所有未提交文件的修改内容
$ git checkout HEAD <file>   #撤销指定的未提交文件的修改内容
$ git revert <commit>        #撤销指定的提交
$ git log --before="1 days"  #退回到之前1天的版本

## 分支与标签

$ git branch                   #显示所有本地分支
$ git checkout <branch/tag>    #切换到指定分支和标签
$ git branch <new-branch>      #创建新分支
$ git branch -d <branch>       #删除本地分支
$ git tag                      #列出所有本地标签
$ git tag <tagname>            #基于最新提交创建标签
$ git tag -d <tagname>         #删除标签


## 合并与衍合

$ git merge <branch>        #合并指定分支到当前分支
$ git rebase <branch>       #衍合指定分支到当前分支


## 远程操作

$ git remote -v                   #查看远程版本库信息
$ git remote show <remote>        #查看指定远程版本库信息
$ git remote add <remote> <url>   #添加远程版本库
$ git fetch <remote>              #从远程库获取代码
$ git pull <remote> <branch>      #下载代码及快速合并
$ git push <remote> <branch>      #上传代码及快速合并
$ git push <remote> :<branch/tag-name>  #删除远程分支或标签
$ git push --tags                       #上传所有标签

