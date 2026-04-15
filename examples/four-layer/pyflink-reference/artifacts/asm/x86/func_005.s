mov eax, dword ptr [rbx]
test eax, eax
jne wait_retry
