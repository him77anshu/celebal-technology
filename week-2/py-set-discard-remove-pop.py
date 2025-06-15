n = int(input())
s = list(map(int, input().split()))  
s = list(dict.fromkeys(s))  
q = int(input())
for _ in range(q):
    c = input().split()
    if c[0] == 'pop':
        if s:
            s.pop(0)  
    elif c[0] == 'remove':
        val = int(c[1])
        if val in s:
            s.remove(val)
    elif c[0] == 'discard':
        val = int(c[1])
        if val in s:
            s.remove(val)

print(sum(s))