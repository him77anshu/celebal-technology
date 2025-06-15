import string
def print_rangoli(size):
    alp = string.ascii_lowercase
    lines = []

    for i in range(size):
        left = alp[size-1:i:-1]  
        center = alp[i]           
        right = alp[i+1:size]     
        row = '-'.join(left + center + right)
        lines.append(row.center(4 * size - 3, '-'))

    full_rangoli = lines[::-1] + lines[1:]
    for line in full_rangoli:
        print(line)
if __name__ == '__main__':
    n = int(input())
    print_rangoli(n)