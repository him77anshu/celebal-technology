from collections import Counter

shoe = int(input())
sizes = list(map(int, input().split()))
inv = Counter(sizes)
cus = int(input())
e = 0
for _ in range(cus):
    sizes, price = map(int, input().split())  
    if inv[sizes] > 0:
        e += price
        inv[sizes] -= 1         
print(e)