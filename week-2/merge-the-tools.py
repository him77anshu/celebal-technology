def merge_the_tools(string, k):
    for i in range(0, len(string), k):
        part = string[i:i+k]
        seen = ""
        for ch in part:
            if ch not in seen:
                seen += ch
        print(seen)

if __name__ == '__main__':
    string, k = input(), int(input())
    merge_the_tools(string, k)