stp x29, x30, [sp, #-16]!
mov x29, sp
ldr x8, [x0, #24]
blr x8
ldp x29, x30, [sp], #16
ret
