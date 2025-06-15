from itertools import combinations
n = int(input())
letters = input().split()
k = int(input())
combi= list(combinations(range(n), k))
count = 0
for i in combi:
    if 'a' in [letters[j] for j in i]:
        count += 1
prob = count / len(combi)
print(f"{prob:.4f}")