"""
标注采集数据时插入形式及存入文件目录
compile_fx_inductor_
compile_fx 修改文件名称
inductor 文件所在目录
完整相对目录 torch\_inductor\compile_fx.py

采用with的形式插入
采集阶段为图编译阶段
"""
from .wkf_your_perf_module import PerfProfiler
with PerfProfiler() as prof:
    pass