import torch
import torch.nn as nn
import torch.nn.functional as F
import os
import time
from torch._dynamo.utils import compile_times
print("PyTorch:", torch.__version__)

import platform

# 获取当前系统的架构
architecture = platform.machine().lower()

# 判断架构并赋值
if "x86" in architecture or "amd64" in architecture or "intel" in architecture:
    name = 9654
elif "arm" in architecture or "aarch64" in architecture:
    name = 950

class DynamoUltraHeavyModel(nn.Module):

    # def __init__(self, dim: int, depth: int):
    #     super().__init__()
    #     self.weights = nn.ParameterList(
    #         [nn.Parameter(torch.randn(dim, dim)) for _ in range(depth)]
    #     )
    #     # self.inner_iters = inner_iters
    
    # def forward(self, x: torch.Tensor) -> torch.Tensor:
    #     for w in self.weights:
    #         for _ in range(5):
    #             x = torch.matmul(x, w)
    #             x = x + 1.0
    #             x = F.relu(x)
    #     return x
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
DIM = int(os.environ.get("PYTORCH_DEMO_DIM", "512"))
DEPTH = int(os.environ.get("PYTORCH_DEMO_DEPTH", "1000"))   # 外层 loop，控制前端复杂度
print(f"Demo config: DIM={DIM}, DEPTH={DEPTH}")
x = torch.randn(DIM, DIM)
x2 = torch.randn(DIM, DIM)


# -------------------- 初始化模型 --------------------
model = DynamoUltraHeavyModel(DIM, DEPTH)

# 使用 eager backend，只测前端
model = torch.compile(model, backend="inductor")
pre = compile_times()
# 清理 Dynamo 缓存，保证第一次 compile
torch._dynamo.reset()

# -------------------- 第一次编译 + 执行 --------------------
start = time.time()
y = model(x)
fir = compile_times()
from datetime import datetime
data_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(f"{name}_compile.txt","a") as f:
    f.write(f"[{data_time}]\n") 
    f.write(f"[compile inductor]:\n{fir}\n\n")
elapsed_first = time.time() - start
print(f"[First compile + run] {elapsed_first:.4f} s")

# -------------------- 后续执行 --------------------
# start = time.time()
# y = model(x)
# sec = compile_times()
# with open("compile_aot.txt", "a") as f:
#     f.write(f"[second]: {sec} 秒\n\n")
# elapsed_second = time.time() - start
# print(f"[Subsequent run] {elapsed_second:.4f} s")
