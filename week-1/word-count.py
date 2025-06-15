from collections import OrderedDict
n = int(input())
word_count = OrderedDict()
for _ in range(n):
    word = input().strip()
    if word in word_count:
        word_count[word] += 1
    else:
        word_count[word] = 1

print(len(word_count))
print(" ".join(str(count) for count in word_count.values()))