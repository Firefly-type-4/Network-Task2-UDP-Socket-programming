import socket
import random
import time
import struct
from datetime import datetime

# 配置参数
SERVER_PORT = 10000
PACKET_LOSS_RATE = 0.2  # 20%丢包率
STUDENT_ID_XOR = 0x5A3C
LOG_FILE = "run_log.txt"

# 全局变量
is_connected = False
client_addr = None
expected_seq = 0
received_bytes = 0

def log(message):
    """写入运行日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [SERVER] {message}\n")

def validate_student_id(received_id):
    """验证StudentID字段"""
    original_id = received_id ^ STUDENT_ID_XOR
    return 0 <= original_id <= 9999

def main():
    global is_connected, client_addr, expected_seq, received_bytes
    
    # 创建UDP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind(("0.0.0.0", SERVER_PORT))
    print(f"服务器已启动，监听端口 {SERVER_PORT}")
    log("服务器启动，开始监听")
    
    try:
        while True:
            data, addr = server_socket.recvfrom(1024)
            current_time = time.time()
            
            # 解析首部
            if len(data) < 8:
                log(f"收到无效数据包，长度不足：{len(data)}字节")
                continue
                
            header = struct.unpack("!BHHHB", data[:8])
            msg_type, student_id, seq_num, ack_num, data_len = header
            payload = data[8:8+data_len] if data_len > 0 else b""
            
            # 处理连接请求
            if msg_type == 0x01:
                log(f"收到来自 {addr} 的连接请求，StudentID: {student_id:04X}")
                if validate_student_id(student_id):
                    is_connected = True
                    client_addr = addr
                    expected_seq = 0
                    received_bytes = 0
                    
                    # 发送连接响应
                    response_header = struct.pack("!BHHHB", 0x02, student_id, 0, 0, 0)
                    server_socket.sendto(response_header, addr)
                    log(f"连接建立成功，客户端地址：{addr}")
                else:
                    log(f"连接被拒绝：无效的StudentID {student_id:04X}")
                    response_header = struct.pack("!BHHHB", 0x02, 0, 0, 0, 0)
                    server_socket.sendto(response_header, addr)
            
            # 处理数据报文（仅在已连接状态）
            elif msg_type == 0x03 and is_connected and addr == client_addr:
                log(f"收到数据报文，序列号：{seq_num}，长度：{data_len}字节")
                
                # 模拟随机丢包
                if random.random() < PACKET_LOSS_RATE:
                    log(f"模拟丢包：序列号 {seq_num} 的数据包被丢弃")
                    continue
                
                # 累积确认：只确认按序到达的数据包
                if seq_num == expected_seq:
                    expected_seq += data_len
                    received_bytes += data_len
                    log(f"数据按序接收，更新预期序列号为：{expected_seq}")
                
                # 发送确认报文
                ack_header = struct.pack("!BHHHB", 0x04, 0, 0, expected_seq, 0)
                server_socket.sendto(ack_header, addr)
                log(f"发送确认报文，确认号：{expected_seq}")
            
            # 处理断开连接请求
            elif msg_type == 0x05 and is_connected and addr == client_addr:
                log(f"收到断开连接请求")
                response_header = struct.pack("!BHHHB", 0x05, 0, 0, 0, 0)
                server_socket.sendto(response_header, addr)
                log(f"连接已断开，共接收 {received_bytes} 字节数据")
                is_connected = False
                client_addr = None
    
    except KeyboardInterrupt:
        print("\n服务器正在关闭...")
        log("服务器手动关闭")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()


    #  python udpserver.py