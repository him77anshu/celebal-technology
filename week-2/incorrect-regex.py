import re
for _ in range(int(input())):
    pattern = input()
    try:
        re.compile(pattern)
        print("True")
    except re.error:
        print("False")