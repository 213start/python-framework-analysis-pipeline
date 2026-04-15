push %rbp
mov %rsp, %rbp
mov 0x18(%rdi), %rax
call *%rax
pop %rbp
ret
