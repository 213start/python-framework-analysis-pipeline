"""
标注采集数据时插入形式及存入文件目录
graph_compile_fun_aot_.py
graph_compile 修改文件名称
fun/aot 文件所在目录
完整相对目录 torch\_functorch\_aot_autograd\graph_compile.py

采用with的形式插入
采集阶段为fx图生成阶段
"""
from .wkf_your_perf_module import PerfProfiler
with PerfProfiler() as prof:
    pass