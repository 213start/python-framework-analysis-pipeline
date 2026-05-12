# import os
# os.environ["TORCHINDUCTOR_FORCE_DISABLE_CACHES"] = "1"

import torch  
import torch.nn as nn  
import torch.nn.functional as F  
import time  
from torch._dynamo.utils import compile_times  
from torch._inductor import standalone_compile  
  
print("PyTorch:", torch.__version__)
# import sys  
# t_v = torch.__version__  
# p_v = sys.version.split(" ")[0]  
# with open("inductor_only_com.txt", "a") as f:  
#     f.write(f"{p_v} + {t_v}\n")  
import platform

# 获取当前系统的架构
architecture = platform.machine().lower()



# 判断架构并赋值
if "x86" in architecture or "amd64" in architecture or "intel" in architecture:
    name = 9654
elif "arm" in architecture or "aarch64" in architecture:
    name = 950
class DynamoUltraHeavyModel(nn.Module):  
    def __init__(self, dim, depth):  
        super().__init__()  
        self.weights = nn.ParameterList(  
            [nn.Parameter(torch.randn(dim, dim)) for _ in range(depth)]  
        )  
  
    def forward(self, x):  
        for i, w in enumerate(self.weights):  
            for j in range(5):  # 内层 loop，增加 guard 数  
                if x.shape[0] == x.shape[1]:  
                    x = torch.matmul(x, w)  
                else:  
                    x = torch.matmul(w, x)  
  
                if x.numel() > 0:  
                    x = x + 1.0  
  
                if x.dtype == torch.float32 and x.dim() == 2:  
                    x = F.relu(x)  
                else:  
                    x = torch.abs(x)  
  
                # Python-level list/dict 操作增加前端复杂度  
                tmp_list = [x, x + j, x * j]  
                tmp_dict = {k: v for k, v in enumerate(tmp_list)}  
                x = tmp_dict[0]  
  
        return x  
  
# -------------------- 参数 --------------------  
DIM = 512
DEPTH = 1000   # 外层 loop，控制前端复杂度  
x = torch.randn(DIM, DIM)  
  
# -------------------- 初始化模型 --------------------  
model = DynamoUltraHeavyModel(DIM, DEPTH)  
  
# 清理 Dynamo 缓存，保证第一次 compile  
torch._dynamo.reset()  
  
# -------------------- 只编译，不执行 --------------------  

pre = compile_times()  
  
# 获取 GraphModule 和示例输入  
start = time.time()  

with torch.no_grad():  
    # 使用 export 获取 GraphModule  
    ep = torch.export.export(model, (x,))  
    gm = ep.module()  
    example_inputs = (x,)  
export_time = time.time() - start  
print(f"export_time:{export_time:.4f} s")
# 执行 standalone 编译  
start = time.time()  
compiled_artifact = standalone_compile(gm, example_inputs)  
fir = compile_times()  
elapsed_compile = time.time() - start  
from datetime import datetime
data_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(f"{name}_inductor_only_com.txt","a") as f:  
    f.write(f"[{data_time}]\n") 
    f.write(f"[compile inductor]:\n{fir}\n\n")  
print(f"[Compile only] {elapsed_compile:.4f} s")  