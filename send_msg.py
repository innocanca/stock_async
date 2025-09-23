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
    
    Returns:
        bool: 发送是否成功
    """
    try:
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
            return True
        else:
            print(f"机器人{msgtype}消息发送失败: {result}")
            return False
            
    except Exception as e:
        print(f"发送{msgtype}消息时出现异常: {e}")
        return False

def split_markdown_content(content, max_length=3500):
    """
    智能分割markdown内容，确保不超过最大长度
    使用字节长度而非字符长度来判断，预留500字节安全空间
    """
    # 检查UTF-8字节长度，而非字符长度
    if len(content.encode('utf-8')) <= max_length:
        return [content]
    
    chunks = []
    lines = content.split('\n')
    current_chunk = ""
    
    for line in lines:
        # 检查添加这一行后是否会超过字节长度限制
        test_content = current_chunk + line + '\n'
        
        if len(test_content.encode('utf-8')) > max_length and current_chunk:
            # 当前块已接近限制，保存并开始新块
            chunks.append(current_chunk.rstrip())
            current_chunk = line + '\n'
        else:
            current_chunk += line + '\n'
    
    # 添加最后一块
    if current_chunk.strip():
        chunks.append(current_chunk.rstrip())
    
    return chunks

def send_markdown_message(content):
    """
    发送markdown格式消息的便捷方法
    自动处理超长消息分页发送
    """
    try:
        byte_length = len(content.encode('utf-8'))
        print(f"消息长度: {len(content)} 字符, {byte_length} 字节")
        # 检查内容字节长度，如果超过限制则分页发送
        if byte_length > 3500:
            print(f"消息过长({len(content)}字符, {byte_length}字节)，将分页发送")
            print(f"content: {content}")
            chunks = split_markdown_content(content)
            total_chunks = len(chunks)
            
            print(f"将分成{total_chunks}页发送")
            
            for i, chunk in enumerate(chunks, 1):
                # 为每个分页添加标识
                if total_chunks > 1:
                    page_info = f"\n\n---\n📖 **第{i}/{total_chunks}页**"
                    chunk_with_page = chunk + page_info
                else:
                    chunk_with_page = chunk
                
                chunk_byte_length = len(chunk_with_page.encode('utf-8'))
                print(f"发送第{i}页，长度: {len(chunk_with_page)} 字符, {chunk_byte_length} 字节")
                success = send_robot_message(chunk_with_page, msgtype="markdown")
                if not success:
                    print(f"第{i}页发送失败，停止发送剩余页面")
                    return False
                
                # 分页间隔，避免发送过快
                if i < total_chunks:
                    import time
                    time.sleep(1)
            
            return True
        else:
            print(f"直接发送消息，长度: {len(content)} 字符, {byte_length} 字节")
            return send_robot_message(content, msgtype="markdown")
            
    except Exception as e:
        print(f"发送markdown消息时出错: {e}")
        return False