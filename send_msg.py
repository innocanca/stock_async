import requests
import json

# ！！！配置区：请修改为你自己的信息 ！！！
# 获取方式：企业微信群 -> 群设置 -> 群机器人 -> 添加机器人 -> 复制Webhook地址
WEBHOOK_URL = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=015361ec-495b-4a6e-b54f-36089b60a876'

def send_robot_message(content, msgtype="text"):
    """
    通过企业微信群机器人发送消息
    
    Args:
        content: 消息内容
        msgtype: 消息类型，支持 "text" 或 "markdown"
    """
    if msgtype == "markdown":
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
    else:
        payload = {
            "msgtype": "text", 
            "text": {
                "content": content
            }
        }
    
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    resp = requests.post(WEBHOOK_URL, data=json.dumps(payload, ensure_ascii=False).encode('utf-8'), headers=headers)
    result = resp.json()
    if result['errcode'] == 0:
        print(f"机器人{msgtype}消息发送成功！")
    else:
        print(f"机器人{msgtype}消息发送失败: {result}")

def send_markdown_message(content):
    """发送markdown格式消息的便捷方法"""
    send_robot_message(content, msgtype="markdown")