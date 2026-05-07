import subprocess
import os
import signal
import time
import sys,platform,re
from datetime import datetime

class PerfProfiler:
    """在指定代码段收集硬件性能计数器"""
    
    def __init__(self, events=None, output_file=None,name='perf'):
        self.arch = platform.machine().lower()
        self.perf_proc = None
        self.output_file = output_file
        self._perf_enabled = os.environ.get("DYNAMO_PERF_COLLECT", "0") == "1"
        print("DYNAMO_PERF_COLLECT:",self._perf_enabled)
        self.name = name

    def start(self):
        if not self._perf_enabled:
            print("perf采集未启用")
            return
        """开始采集"""
        pid = os.getpid()
        data_time = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

        if self.output_file is None:
            self.output_file = f"/home/w30063991/{self.name}_{self.arch}_{data_time}.data"
        
        # 构造命令：只附加到 PID，不要加 -- sleep
        cmd = [
            "perf", "record",
            '-e', 'instructions',
            '-F', '99',
            '-m', '64M',
            '-g', '--call-graph=dwarf',
            "-p", str(pid),
            "-o", self.output_file,
            # "-v" # 如果需要调试 perf 本身，可以打开 verbose
        ]
        
        print(f"[PerfProfiler] Starting perf on PID {pid}...")
        
        # 启动 perf
        # 使用 stderr=subprocess.PIPE 以便捕获启动失败的信息（如权限不足）
        self.perf_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL, # perf stat 的标准输出通常没用，结果在 stderr 或 -o 文件
            stderr=subprocess.PIPE,    # 捕获错误日志
            text=True                  # 让 stderr 输出为字符串而不是字节
        )
        
        # 【关键】等待 perf 初始化
        # perf 启动需要时间加载 PMU 计数器，如果不等，可能代码跑完了 perf 还没开始记
        time.sleep(0.2) 
        
        # 检查 perf 是否意外退出（例如因为权限问题）
        if self.perf_proc.poll() is not None:
            stderr_out = self.perf_proc.stderr.read()
            print(f"[PerfProfiler] Error: perf failed to start:\n{stderr_out}")
            self.perf_proc = None
            raise RuntimeError("perf failed to start. Check permissions or event names.")
    
    def stop(self):
        if not self._perf_enabled:
            return
        """停止采集并解析结果"""
        if self.perf_proc:
            print("[PerfProfiler] Stopping perf...")
            # 发送 SIGINT (相当于 Ctrl+C)，perf 收到后会写入结果并退出
            self.perf_proc.send_signal(signal.SIGINT)
            
            try:
                # 设置超时，防止死锁
                stdout, stderr = self.perf_proc.communicate(timeout=20)
                if stderr:
                    print(f"[PerfProfiler] Perf stderr: {stderr}")
            except subprocess.TimeoutExpired:
                print("[PerfProfiler] Warning: perf didn't exit gracefully, killing it.")
                self.perf_proc.kill()
            
            # 读取结果
            if os.path.exists(self.output_file):
                print(f"数据已采集到{self.output_file}" + "\n")
            else:
                print(f"[PerfProfiler] Warning: Output file {self.output_file} not found.")

    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()