# coding:utf-8
from datetime import datetime
import random
import json
import itchat
from pprint import pprint
from apscheduler.schedulers.background import BlockingScheduler
import  json
import  urllib.request
import requests
from urllib.parse import urlencode

cityList_bsgs = [
    {'code':'101280101','name':"广州"}
]

chatroom_list ='GST饿坏吃饭群'

eating_shops = ['多喝汤','瓦罐汤','潮汕鱼旦粉','大鸡扒','家乐缘','面谱','牛丼','兰州拉面']

#返回dict类型: twitter = {'image': imgPath, 'message': content}
def get_weather_realtime(cityID):
    url = "http://www.weather.com.cn/data/sk/" + str(cityID) + ".html"
    try:
        stdout = urllib.request.urlopen(url)
        weatherInfomation = stdout.read().decode('utf-8')
        jsonDatas = json.loads(weatherInfomation)
        #print(jsonDatas)
        city        = jsonDatas["weatherinfo"]["city"]
        temp        = jsonDatas["weatherinfo"]["temp"]
        fx          = jsonDatas["weatherinfo"]["WD"]        #风向
        fl          = jsonDatas["weatherinfo"]["WS"]        #风力
        sd          = jsonDatas["weatherinfo"]["SD"]        #相对湿度
        tm          = jsonDatas["weatherinfo"]["time"]
        content = city +" " + temp + "℃  " + fx + fl + " " + "相对湿度:" + sd + " " 
        # + "发布时间:" + tm
        twitter = {'image': "", 'message': content}
 
    except (SyntaxError) as err:
        print("SyntaxError: " + err.args)
    except:
        print("OtherError: ")
    else:
        return twitter
    finally:
        pass

def get_context():
    for city in cityList_bsgs:
        title_small = "[吃饭看天气预报]:"
        twitter = get_weather_realtime(city['code'])
        print(title_small + twitter['message'])
        twitter_realTime = title_small + twitter['message']
    return "\n"+twitter_realTime+"\n"
 
def send_chatrooms_msg(chatroomName, context):
    shops = random.sample(eating_shops, 1)
    #python2.7 用：choices_content = u"**各位饿货今天咋们去吃：[ %s ]**"%(shops[0].decode("utf8"))
    choices_content = u"[中午吃饭决定了]:**各位饿货们！今天中午咱们去吃：【 %s 】 **"%(shops[0])
    itchat.get_chatrooms(update=True)
    chatrooms = itchat.search_chatrooms(name=chatroomName)
    print("chatrooms is %s \n" %chatroomName)
    if len(chatrooms)==0:
        print(u'没有找到群聊：' + chatroomName)
    else:
        for room in chatrooms:
            if room['NickName'] == chatroomName:
                userName = room['UserName']
                print("userName is %s" %userName)
                break
        itchat.send_msg(context+choices_content, userName)
        print("发送时间：" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
        print("发送到：" + chatroomName + "\n")
        print("发送内容：" + context+choices_content + "\n")
        print('*'*50+"\n")
        scheduler.print_jobs()

def loginCallback():
    print("Login successful !")
 
def exitCallback():
    print("logout successful !")
 
# itchat.auto_login(hotReload=True, 
#                   enableCmdQR=True, 
#                   loginCallback=loginCallback, 
#                   exitCallback=exitCallback)

if __name__ == "__main__":
    print('*'*50+"\n")
    print("---------- 使用方法  ----------\n")
    print("Step01: 收藏群 [GST饿坏吃饭群]\n")
    print("Step02: 扫描出现的二维码\n")
    print("Step03: 完成上两步后即可自动发消息\n")
    print('*'*50+"\n")
	try:
		f = open("config.json")
		configs = json.load(f)
		exe_hour = configs['exe_hour']
		exe_time = configs['exe_time']
		is_realtime = configs['is_realtime']
		interval_time = configs['interval_time']
	except IOError:
		print("Config File: config.json is not exists.")
    itchat.auto_login(hotReload=True)
    scheduler = BlockingScheduler()
	if not is_realtime:
		#指定时点执行
		scheduler.add_job(send_chatrooms_msg, 
						  'cron', 
						   day_of_week='mon-fri',
						   hour =exe_hour,
						   minute =exe_time,
						   kwargs={"chatroomName": chatroom_list, 
								   "context": get_context()})
	else:
		#间隔执行  每3秒执行一次
		scheduler.add_job(send_chatrooms_msg, 'cron', second = '*/{}'.format(interval_time), kwargs={"chatroomName": chatroom_list, "context": get_context()})
    print("任务" + ":\n"+"待发送到：" + chatroom_list + "\n"+"待发送内容：" + get_context() + "\n")
    print('*'*50+"\n")

    scheduler.start()
