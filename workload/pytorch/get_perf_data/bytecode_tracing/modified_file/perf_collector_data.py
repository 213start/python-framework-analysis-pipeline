import contextlib
import os
import subprocess
import threading
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

@dataclass
class PerfConfig:
    """Perf 采集配置"""
    enabled: bool = True
    output_dir: str = "/home/w30063991"
    frequency: int = 99  # 采样频率
    event: str = "instructions"  # 采集事件
    buffer_size: str = "64M"  # 缓冲区大小
    callgraph_mode: str = "dwarf"  # 调用图模式
    record_callchain: bool = True


class PerfCollector:
    """
    Perf 采集管理器，支持嵌套和局部采集
    使用 perf record -e instructions -F 99 -m 64M -g --call-graph=dwarf
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self.config = PerfConfig()
        self._active_collectors: Dict[int, subprocess.Popen] = {}
        self._collector_stack: list = []
        self._counter = 0
        
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
    
    def start_collection(
        self, 
        name: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        启动 perf 采集
        
        Args:
            name: 采集名称
            metadata: 元数据（会保存到对应的文件中）
        
        Returns:
            collector_id: 采集器 ID
        """
        if not self.config.enabled:
            return -1
        
        self._counter += 1
        collector_id = self._counter

        data_time = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
        
        output_file = os.path.join(
            self.config.output_dir,
            f"{name}_{collector_id}_{data_time}.data"
        )
        
        if metadata:
            metadata_file = os.path.join(
                self.config.output_dir,
                f"{name}_{collector_id}_metadata.txt"
            )
            with open(metadata_file, 'w') as f:
                for key, value in metadata.items():
                    f.write(f"{key}: {value}\n")
        
        cmd = [
            "perf", "record",
            "-e", self.config.event,
            "-F", str(self.config.frequency),
            "-m", self.config.buffer_size,
            "-g",
            "--call-graph", self.config.callgraph_mode,
            "-o", output_file,
            "-p", str(os.getpid()),
            # "--", "sleep", "999999",
        ]
        
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            self._active_collectors[collector_id] = proc
            self._collector_stack.append(collector_id)
            
            print(f"[Perf] Started collection {collector_id}: {name}")
            print(f"[Perf] Output file: {output_file}")
            print(f"[Perf] Command: {' '.join(cmd)}")
            return collector_id
            
        except Exception as e:
            print(f"[Perf] Failed to start collection: {e}")
            return -1
    
    def stop_collection(self, collector_id: int) -> bool:
        """
        停止 perf 采集
        
        Args:
            collector_id: 采集器 ID
        
        Returns:
            是否成功停止
        """
        if collector_id == -1 or collector_id not in self._active_collectors:
            return False
        
        try:
            proc = self._active_collectors[collector_id]
            proc.terminate()
            proc.wait(timeout=5)
            
            del self._active_collectors[collector_id]
            if collector_id in self._collector_stack:
                self._collector_stack.remove(collector_id)
            
            print(f"[Perf] Stopped collection {collector_id}")
            return True
            
        except Exception as e:
            print(f"[Perf] Failed to stop collection {collector_id}: {e}")
            return False
    
    @contextlib.contextmanager
    def collect(self, name: str, metadata: Optional[Dict[str, Any]] = None):
        """
        上下文管理器，用于局部采集
        
        Usage:
            with perf_collector.collect("my_function", {"arg": value}):
                pass
        """
        collector_id = self.start_collection(name, metadata)
        try:
            yield collector_id
        finally:
            self.stop_collection(collector_id)
    
    def get_active_collectors(self) -> list:
        """获取当前活跃的采集器列表"""
        return list(self._active_collectors.keys())


perf_collector = PerfCollector()
