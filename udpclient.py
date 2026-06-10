import socket
import sys
import time
import struct
import random
import pandas as pd
from datetime import datetime

# 配置参数
STUDENT_ID_LAST4 = 2211 
STUDENT_ID_XOR = 0x5A3C
WINDOW_SIZE = 400  # 发送窗口大小(字节)
MIN_PACKET_SIZE = 40
MAX_PACKET_SIZE = 80
TOTAL_BYTES_TO_SEND = 2400  # 总共发送30个80字节数据包
TIMEOUT = 0.3  # 初始超时时间(秒)
LOG_FILE = "run_log.txt"

# 计算StudentID字段
STUDENT_ID_FIELD = STUDENT_ID_LAST4 ^ STUDENT_ID_XOR

def log(message):
    """写入运行日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [CLIENT] {message}\n")

def create_packet(msg_type, seq_num=0, ack_num=0, data=b""):
    """创建应用层报文"""
    data_len = len(data)
    header = struct.pack("!BHHHB", msg_type, STUDENT_ID_FIELD, seq_num, ack_num, data_len)
    return header + data

def main():
    if len(sys.argv) != 3:
        print("用法: python udpclient.py <服务器IP> <服务器端口>")
        print("示例: python udpclient.py 192.168.1.100 12345")
        sys.exit(1)
    
    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    server_addr = (server_ip, server_port)
    
    # 创建UDP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(TIMEOUT)
    
    # 清空日志文件
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("=== UDP可靠传输运行日志 ===\n")
    
    print("客户端启动，正在连接服务器...")
    log("客户端启动")
    
    # 1. 连接建立阶段
    connect_packet = create_packet(0x01)
    client_socket.sendto(connect_packet, server_addr)
    log("发送连接请求")
    
    try:
        response, _ = client_socket.recvfrom(1024)
        header = struct.unpack("!BHHHB", response[:8])
        if header[0] == 0x02 and header[1] == STUDENT_ID_FIELD:
            print("连接建立成功！")
            log("连接建立成功")
        else:
            print("连接被服务器拒绝：无效的StudentID")
            log("连接被拒绝")
            client_socket.close()
            sys.exit(1)
    except socket.timeout:
        print("连接超时")
        log("连接超时")
        client_socket.close()
        sys.exit(1)
    
    # 2. 数据传输阶段
    print("\n开始数据传输...")
    log("开始数据传输")
    
    # 生成测试数据
    test_data = b"A" * TOTAL_BYTES_TO_SEND
    
    # GBN协议变量
    base = 0  # 窗口起始位置
    next_seq = 0  # 下一个要发送的字节编号
    packet_count = 0  # 已发送的数据包编号
    total_packets_sent = 0  # 实际发送的总数据包数
    rtt_list = []  # 存储所有RTT值
    send_times = {}  # 记录每个数据包的发送时间
    
    while base < TOTAL_BYTES_TO_SEND:
        # 发送窗口内的数据包
        while next_seq < base + WINDOW_SIZE and next_seq < TOTAL_BYTES_TO_SEND:
            # 随机选择数据包大小
            packet_size = random.randint(MIN_PACKET_SIZE, MAX_PACKET_SIZE)
            if next_seq + packet_size > TOTAL_BYTES_TO_SEND:
                packet_size = TOTAL_BYTES_TO_SEND - next_seq
            
            # 创建并发送数据包
            data = test_data[next_seq:next_seq+packet_size]
            data_packet = create_packet(0x03, seq_num=next_seq, data=data)
            client_socket.sendto(data_packet, server_addr)
            
            # 记录发送时间和统计信息
            send_time = time.time()
            send_times[next_seq] = send_time
            packet_count += 1
            total_packets_sent += 1
            
            # 打印发送信息
            end_byte = next_seq + packet_size - 1
            print(f"第{packet_count}个(第{next_seq}~{end_byte}字节) client端已经发送")
            log(f"发送第{packet_count}个数据包，字节范围：{next_seq}~{end_byte}")
            
            next_seq += packet_size
        
        # 等待确认
        try:
            response, _ = client_socket.recvfrom(1024)
            recv_time = time.time()
            header = struct.unpack("!BHHHB", response[:8])
            
            if header[0] == 0x04:  # 确认报文
                ack_num = header[3]
                log(f"收到确认报文，确认号：{ack_num}")
                
                # 计算已确认的数据包的RTT
                confirmed_seqs = [seq for seq in send_times if seq < ack_num]
                for seq in confirmed_seqs:
                    rtt = (recv_time - send_times[seq]) * 1000  # 转换为毫秒
                    rtt_list.append(rtt)
                    del send_times[seq]
                
                # 更新窗口起始位置
                if ack_num > base:
                    base = ack_num
                    
                    # 打印确认信息
                    print(f"已收到确认，当前已确认到第{base-1}字节")
                    log(f"窗口滑动，新的窗口起始位置：{base}")
        
        except socket.timeout:
            # 超时重传
            print(f"超时，重传窗口内的所有数据包")
            log("超时事件发生，开始重传")
            
            # 重传窗口内所有未确认的数据包
            retransmit_seq = base
            retransmit_count = 0
            while retransmit_seq < next_seq:
                # 找到该序列号对应的数据包大小
                packet_size = min(MAX_PACKET_SIZE, TOTAL_BYTES_TO_SEND - retransmit_seq)
                data = test_data[retransmit_seq:retransmit_seq+packet_size]
                data_packet = create_packet(0x03, seq_num=retransmit_seq, data=data)
                client_socket.sendto(data_packet, server_addr)
                
                # 更新发送时间
                send_times[retransmit_seq] = time.time()
                total_packets_sent += 1
                retransmit_count += 1
                
                # 打印重传信息
                end_byte = retransmit_seq + packet_size - 1
                print(f"重传第{packet_count-retransmit_count+1}个(第{retransmit_seq}~{end_byte}字节)数据包")
                log(f"重传数据包，字节范围：{retransmit_seq}~{end_byte}")
                
                retransmit_seq += packet_size
    
    # 3. 断开连接
    print("\n数据传输完成，正在断开连接...")
    log("数据传输完成，发送断开连接请求")
    
    disconnect_packet = create_packet(0x05)
    client_socket.sendto(disconnect_packet, server_addr)
    
    try:
        response, _ = client_socket.recvfrom(1024)
        header = struct.unpack("!BHHHB", response[:8])
        if header[0] == 0x05:
            print("连接已断开")
            log("连接已断开")
    except socket.timeout:
        log("断开连接确认超时")
    
    client_socket.close()
    
    # 4. 统计信息
    print("\n" + "="*50)
    print("传输统计信息")
    print("="*50)
    
    # 计算丢包率
    theoretical_packets = packet_count
    packet_loss_rate = (1 - theoretical_packets / total_packets_sent) * 100
    print(f"丢包率: {packet_loss_rate:.2f}%")
    
    # 使用pandas计算RTT统计量
    if rtt_list:
        df = pd.DataFrame(rtt_list, columns=["RTT"])
        max_rtt = df["RTT"].max()
        min_rtt = df["RTT"].min()
        avg_rtt = df["RTT"].mean()
        std_rtt = df["RTT"].std()
        
        print(f"最大RTT: {max_rtt:.2f} ms")
        print(f"最小RTT: {min_rtt:.2f} ms")
        print(f"平均RTT: {avg_rtt:.2f} ms")
        print(f"RTT标准差: {std_rtt:.2f} ms")
        
        # 写入日志
        log(f"传输统计：丢包率={packet_loss_rate:.2f}%, 最大RTT={max_rtt:.2f}ms, 最小RTT={min_rtt:.2f}ms, 平均RTT={avg_rtt:.2f}ms, RTT标准差={std_rtt:.2f}ms")
    else:
        print("未收集到RTT数据")
        log("未收集到RTT数据")

if __name__ == "__main__":
    main()


    #  python udpclient.py 127.0.0.1 10000